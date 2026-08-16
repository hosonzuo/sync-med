# coding: utf-8
"""Re-chunk oversized chunks with paragraph anchoring (plan 2.7, ruling Q2, pilot batch).

Scope: for TEXT_IDS (comma list), split every chunk >10,000 chars into ~<=2,500-char
sub-chunks at natural boundaries, then swap the affected FTS v2 rows in place.

Measured facts driving the design (2026-08-16):
  - 158 books have >10k-char chunks (683 chunks, 11.16M chars = 7% of corpus).
  - Source text has ZERO page markers (4 patterns probed on the biggest offender)
    -> page anchoring is downgraded to paragraph anchoring + char offsets. Honest.
  - Source DOES have structural markers: <目录>..., <篇名>..., and \\x...\\x entry
    names -> used as preferred split boundaries and as the anchor string.

Tables: books_text_chunks stays FROZEN (ruling). New rows go to books_text_chunks_v2
+ chunk_lineage (migration 060). FTS swap: delete rid(parent,part..) rows, insert
rid(sub,0) rows into books_fts_v2_src + books_fts_v2.

NOTE on duplicated helpers: fts_v2_load.py has no import guard (module-level runs the
full load), so d1/bigrams/rid are copied here VERBATIM and pinned by the same golden
fixtures asserted at import -- any tokenizer drift crashes instead of diverging.

Env: GY_CF_ACCOUNT, GY_D1_TOKEN, D1_DB, GY_R2_ENDPOINT, GY_R2_AK, GY_R2_SK,
     GY_R2_BUCKET, TEXT_IDS
"""
import os, sys, io, json, re, time, hashlib
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
import requests, boto3
from botocore.config import Config
from opencc import OpenCC

ACC = os.environ["GY_CF_ACCOUNT"]
TOK = os.environ["GY_D1_TOKEN"]
DB = os.environ["D1_DB"]
U = f"https://api.cloudflare.com/client/v4/accounts/{ACC}/d1/database/{DB}/query"
H = {"Authorization": f"Bearer {TOK}", "Content-Type": "application/json"}
t2s = OpenCC("t2s").convert
CJK_RUN = re.compile("[一-鿿]+")

BIG = 10000          # split threshold (the 158-book criterion)
TARGET = 2500        # preferred sub-chunk size
HARD_MAX = 4000      # never exceed without a boundary


def d1(sql, params=None, retries=4):
    for a in range(retries):
        try:
            r = requests.post(U, headers=H, json={"sql": sql, "params": params or []}, timeout=180)
            j = r.json()
            if j.get("success"):
                res = j["result"][0]
                return res.get("results") or [], res.get("meta") or {}
            msg = json.dumps(j.get("errors"))[:200]
            if "rate" in msg.lower() or r.status_code in (429, 500, 503):
                time.sleep(2 * (a + 1)); continue
            raise RuntimeError(msg)
        except requests.RequestException:
            time.sleep(2 * (a + 1))
    raise RuntimeError("d1 retries exhausted: " + sql[:80])


def bigrams(text):
    toks = []
    for run in CJK_RUN.findall(t2s(text)):
        if len(run) == 1:
            toks.append(run)
        else:
            toks.extend(run[i:i + 2] for i in range(len(run) - 1))
    return " ".join(toks)


_FIX = [("桂枝湯", "桂枝 枝汤"), ("甘草", "甘草"), ("人參。大棗", "人参 大枣"), ("X草Y", "草")]
for _s, _w in _FIX:
    assert bigrams(_s) == _w, f"tokenizer fixture broken: {_s!r} -> {bigrams(_s)!r}"


def rid(chunk_id, part_no):
    h = hashlib.sha1(f"{chunk_id}#{part_no}".encode()).digest()
    return int.from_bytes(h[:6], "big")


s3 = boto3.client("s3", endpoint_url=os.environ["GY_R2_ENDPOINT"],
                  aws_access_key_id=os.environ["GY_R2_AK"],
                  aws_secret_access_key=os.environ["GY_R2_SK"],
                  config=Config(signature_version="s3v4", retries={"max_attempts": 4}),
                  region_name="auto")
BUCKET = os.environ["GY_R2_BUCKET"]

MARK = re.compile(r"<(?:目录|篇名)>[^\n]*|\\x[^\\]{1,40}\\x")


def split_big(text):
    """Yield (start, end, anchor) sub-spans <= HARD_MAX, preferring marker/sentence cuts."""
    marks = [(m.start(), m.group(0)) for m in MARK.finditer(text)]
    cuts = sorted({m[0] for m in marks} | {0, len(text)})
    # greedy assembly of marker-bounded segments into TARGET-sized windows
    spans = []
    seg_bounds = [(cuts[i], cuts[i + 1]) for i in range(len(cuts) - 1)]
    cur_s = None
    cur_e = None
    for s0, e0 in seg_bounds:
        if cur_s is None:
            cur_s, cur_e = s0, e0
        elif (e0 - cur_s) <= TARGET:
            cur_e = e0
        else:
            spans.append((cur_s, cur_e)); cur_s, cur_e = s0, e0
    if cur_s is not None:
        spans.append((cur_s, cur_e))
    # second pass: any span still over HARD_MAX gets sentence-cut
    final = []
    for s0, e0 in spans:
        while e0 - s0 > HARD_MAX:
            win = text[s0:s0 + HARD_MAX]
            cut = max(win.rfind("。"), win.rfind("\n"))
            cut = s0 + (cut + 1 if cut > 200 else HARD_MAX)
            final.append((s0, cut)); s0 = cut
        final.append((s0, e0))
    out = []
    for s0, e0 in final:
        if e0 <= s0:
            continue
        anchor = ""
        for pos, g in reversed(marks):
            if pos <= s0:
                anchor = g.replace("\\x", "").strip()[:60]; break
        out.append((s0, e0, anchor))
    return out


def esc(x):
    return (x or "").replace("'", "''")


def main():
    text_ids = [t.strip() for t in os.environ.get("TEXT_IDS", "").split(",") if t.strip()]
    if not text_ids:
        print("TEXT_IDS empty; nothing to do"); return
    batch = os.environ.get("SPLIT_BATCH", "pilot-3")
    tot_split = tot_sub = 0
    for tid in text_ids:
        rows, _ = d1("SELECT chunk_id, vol_no, paragraph_no, char_count, r2_key FROM books_text_chunks "
                     "WHERE text_id=? ORDER BY vol_no, paragraph_no", [tid])
        vols = {}
        for r in rows:
            vols.setdefault((r["vol_no"], r["r2_key"]), []).append(r)
        print(f"== {tid}: {len(rows)} chunks / {len(vols)} volumes")
        for (vol_no, key), lst in vols.items():
            bigs = [r for r in lst if (r["char_count"] or 0) > BIG]
            if not bigs:
                continue
            data = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            arr = data if isinstance(data, list) else data.get("chunks") or []
            by_id = {c.get("chunk_id"): c for c in arr if isinstance(c, dict)}
            v2_vals, lin_vals, src_vals, fts_vals, del_rids = [], [], [], [], []
            for r in bigs:
                c = by_id.get(r["chunk_id"])
                if not c:
                    print(f"  !! {r['chunk_id']} not in {key}, skip"); continue
                txt = str(c.get("text") or "")
                if len(txt) <= BIG:
                    print(f"  !! {r['chunk_id']} source shorter than D1 char_count, skip"); continue
                parts_old = (len(txt) // 50000) + 1
                del_rids.extend(rid(r["chunk_id"], p) for p in range(parts_old))
                subs = split_big(txt)
                for i, (s0, e0, anchor) in enumerate(subs):
                    sid = f"{r['chunk_id']}_s{i}"
                    piece = txt[s0:e0]
                    v2_vals.append(f"('{esc(sid)}','{esc(r['chunk_id'])}','{esc(tid)}',{vol_no},"
                                   f"{r['paragraph_no'] or 0},{i},{s0},{e0},{len(piece)},'{esc(anchor)}','{esc(key)}')")
                    lin_vals.append(f"('{esc(sid)}','{esc(r['chunk_id'])}',{s0},{e0},'{esc(batch)}')")
                    src_vals.append((rid(sid, 0), sid, 0, tid, vol_no, piece))
                tot_split += 1; tot_sub += len(subs)
                print(f"  {r['chunk_id']} {len(txt)} chars -> {len(subs)} subs")
            B = 40
            for i in range(0, len(v2_vals), B):
                d1("INSERT OR REPLACE INTO books_text_chunks_v2 (chunk_id,parent_chunk_id,text_id,vol_no,"
                   "paragraph_no,sub_no,char_start,char_end,char_count,anchor,r2_key) VALUES " + ",".join(v2_vals[i:i+B]))
            for i in range(0, len(lin_vals), B):
                d1("INSERT OR REPLACE INTO chunk_lineage (new_chunk_id,old_chunk_id,char_start,char_end,split_batch) "
                   "VALUES " + ",".join(lin_vals[i:i+B]))
            for i in range(0, len(del_rids), 80):
                ids = ",".join(str(x) for x in del_rids[i:i+80])
                d1(f"DELETE FROM books_fts_v2 WHERE rowid IN ({ids})")
                d1(f"DELETE FROM books_fts_v2_src WHERE rowid IN ({ids})")
            # SQLITE_TOOBIG lesson (first pilot run): bigram text ~5x source bytes, so
            # 8 rows/statement blew D1's statement size cap. src: 4 rows; fts: 1 row.
            for i in range(0, len(src_vals), 4):
                chunk = src_vals[i:i+4]
                d1("INSERT OR REPLACE INTO books_fts_v2_src (rowid,chunk_id,part_no,text_id,vol_no,body_raw) VALUES "
                   + ",".join(f"({rw},'{esc(cid)}',{pn},'{esc(t)}',{vn},'{esc(body)}')" for rw, cid, pn, t, vn, body in chunk))
            for rw, cid, pn, t, vn, body in src_vals:
                d1(f"INSERT OR REPLACE INTO books_fts_v2 (rowid, body_bi) VALUES ({rw},'{esc(bigrams(body))}')")
            print(f"  vol {vol_no}: swapped {len(del_rids)} old FTS rows -> {len(src_vals)} sub rows")
    print(f"DONE split_chunks={tot_split} sub_chunks={tot_sub}")


if __name__ == "__main__":
    main()
