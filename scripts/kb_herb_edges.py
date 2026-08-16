# coding: utf-8
"""KB aggregation pass 2b: formula->herb weighted edges (OB-structure migration, node 2).

Reads sue_formulas (is_formula=1), tokenizes herb mentions, maps them onto the
controlled registry kb_herbs (372 entries) via OpenCC t2s + approved aliases
(herb_aliases, human-gated only), then writes:
  - kb_formula_herbs (name_s, herb, occur_count)  weighted composition edges
  - kb_herbs.formula_count                        reverse hub counts (distinct formulas)
  - kb_formulas.display_name                      majority original name per name_s

Rules honored:
  - Zero LLM, deterministic string work only.
  - No auto-merging of unknown herb strings: unmatched mentions are simply not
    edges (candidate review queue is maintained separately, human-approved only).
  - Idempotent: full DELETE + reinsert of kb_formula_herbs; UPDATEs converge.

Env: GY_CF_ACCOUNT, GY_D1_TOKEN, D1_DB
"""
import os, sys, io, json, re, time, collections
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
import requests
from opencc import OpenCC

ACC = os.environ["GY_CF_ACCOUNT"]
TOK = os.environ["GY_D1_TOKEN"]
DB  = os.environ["D1_DB"]
API = f"https://api.cloudflare.com/client/v4/accounts/{ACC}/d1/database/{DB}/query"
S = requests.Session()
S.headers.update({"Authorization": f"Bearer {TOK}", "Content-Type": "application/json"})
t2s = OpenCC("t2s").convert

def d1(sql, tries=5):
    for i in range(tries):
        try:
            r = S.post(API, json={"sql": sql}, timeout=180)
            if r.status_code == 200:
                d = r.json()
                if d.get("success"):
                    return d["result"][0]["results"]
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(3 * (i + 1))

def esc(s):
    return (s or "").replace("'", "''")

# ---- registry + approved aliases ----
reg = [r["name"] for r in d1("SELECT name FROM kb_herbs")]
regset = set(reg)
alias = {r["variant"]: r["canonical"] for r in d1("SELECT variant, canonical FROM herb_aliases")}
print(f"registry={len(reg)} aliases={len(alias)}")

# longest-first scan list for composition fallback (avoid short-name shadowing)
scan_names = sorted(regset | set(alias.keys()), key=len, reverse=True)

# split on CJK/ASCII list punctuation (escapes only: public-repo opsec, no CJK literals)
SPLIT = re.compile("[\\u3001\\uFF0C,;\\uFF1B/\\s]+")

def mentions_from(ai_herbs, composition):
    out = []
    if ai_herbs and ai_herbs.strip() and ai_herbs != "[]":
        for tk in SPLIT.split(ai_herbs):
            tk = t2s(tk.strip())
            if tk and len(tk) <= 8:
                out.append(tk)
        return out
    # fallback: masked longest-first substring scan over normalized composition
    text = t2s(composition or "")
    if not text:
        return out
    for nm in scan_names:
        if nm in text:
            c = text.count(nm)
            out.extend([nm] * c)
            text = text.replace(nm, " " * len(nm))
    return out

# ---- pull instances (keyset pagination) ----
edge = collections.Counter()          # (name_s, canon_herb) -> occurrences
namevote = collections.defaultdict(collections.Counter)  # name_s -> original name votes
herb_formulas = collections.defaultdict(set)             # canon_herb -> {name_s}
last = -1
rows_seen = 0
matched = total = 0
while True:
    rows = d1("SELECT formula_id, name_s, name, ai_herbs, composition FROM sue_formulas "
              f"WHERE is_formula=1 AND formula_id>{last} ORDER BY formula_id LIMIT 3000")
    if not rows:
        break
    for r in rows:
        last = r["formula_id"]
        ns = (r["name_s"] or "").strip()
        if not ns:
            continue
        rows_seen += 1
        namevote[ns][r["name"] or ns] += 1
        for tk in mentions_from(r["ai_herbs"], r["composition"]):
            total += 1
            canon = alias.get(tk, tk)
            if canon in regset:
                matched += 1
                edge[(ns, canon)] += 1
                herb_formulas[canon].add(ns)
    print(f"  scanned {rows_seen} (last id {last})")

print(f"instances={rows_seen} mentions={total} matched={matched} "
      f"({matched*100.0/max(total,1):.1f}%) edges={len(edge)}")

# ---- write kb_formula_herbs ----
d1("DELETE FROM kb_formula_herbs")
items = [(ns, hb, c) for (ns, hb), c in edge.items()]
B = 300
for i in range(0, len(items), B):
    vals = ",".join(f"('{esc(ns)}','{esc(hb)}',{c})" for ns, hb, c in items[i:i+B])
    d1(f"INSERT OR REPLACE INTO kb_formula_herbs (name_s,herb,occur_count) VALUES {vals}")
    if (i // B) % 20 == 0:
        print(f"  edges written {i+B}/{len(items)}")
print(f"kb_formula_herbs rows={len(items)}")

# ---- backfill hub counts on kb_herbs ----
for i in range(0, len(reg), 60):
    chunk = reg[i:i+60]
    case = " ".join(f"WHEN '{esc(h)}' THEN {len(herb_formulas.get(h, ()))}" for h in chunk)
    inl = ",".join(f"'{esc(h)}'" for h in chunk)
    d1(f"UPDATE kb_herbs SET formula_count = CASE name {case} END WHERE name IN ({inl})")
print("kb_herbs.formula_count backfilled")

# ---- majority display_name (D1 API is single-statement; send individually) ----
ups = []
for ns, votes in namevote.items():
    best = votes.most_common(1)[0][0]
    if best and best != ns:
        ups.append((ns, best))
UB = 50
for i in range(0, len(ups), UB):
    chunk = ups[i:i+UB]
    case = " ".join(f"WHEN '{esc(ns)}' THEN '{esc(dn)}'" for ns, dn in chunk)
    inl = ",".join(f"'{esc(ns)}'" for ns, _ in chunk)
    d1(f"UPDATE kb_formulas SET display_name = CASE name_s {case} END WHERE name_s IN ({inl})")
    if (i // UB) % 40 == 0:
        print(f"  display_name updates {min(i+UB, len(ups))}/{len(ups)}")
print(f"display_name updated={len(ups)}")

# ---- verification (no CJK literals: read top rows back from D1) ----
top = d1("SELECT name_s, instance_count FROM kb_formulas ORDER BY instance_count DESC LIMIT 3")
for t in top:
    hs = d1(f"SELECT herb, occur_count FROM kb_formula_herbs WHERE name_s='{esc(t['name_s'])}' "
            "ORDER BY occur_count DESC LIMIT 5")
    print("TOP formula", t["name_s"], t["instance_count"], "->", [(h["herb"], h["occur_count"]) for h in hs])
hubs = d1("SELECT name, formula_count FROM kb_herbs ORDER BY formula_count DESC LIMIT 5")
print("TOP hubs:", [(h["name"], h["formula_count"]) for h in hubs])
print("DONE")
