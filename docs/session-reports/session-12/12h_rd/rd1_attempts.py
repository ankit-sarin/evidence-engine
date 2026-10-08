"""12h RD-1 — Pass-1 attempt-1 vs attempt-2 failure codes, per paper and field. Read-only
(files opened for reading). usage, from the repository root:
    rd1_attempts.py --codebook <extraction_codebook.yaml> [--log <label>=<run.log>]
                    <label>=<extraction_calls.jsonl> [...]
(stdout: JSON; also writes rd1_attempts_cells.csv beside the script: one row per
source x paper x field x attempt). The accepted attempt's raw Pass-1 content is parsed with
the engine's own contracts.parse_container; the losing attempt's raw content is not on disk.

Sources with the ELICIT-DESIGN-02 inner loop carry extra.attempts = [attempt 1, attempt 2];
each attempt's fields[name] holds class, indices, escape, violations. Sources from the
ELICIT-DESIGN-01 policy (identical re-issue) carry one telemetry row per outer attempt with
extra.fields; they are reported separately and never pooled with the feedback-retry sources.
A field "failed" an attempt when it is in that attempt's failed_fields (the engine's own list).
"""
import csv, json, os, sys
from collections import Counter, defaultdict

import re
import yaml
sys.path.insert(0, os.getcwd())
from engine.elicitation.contracts import parse_container, KEY_FIELD, KEY_INDICES, KEY_VALUE
assert not any(m == "ollama" or m.startswith("ollama.") for m in sys.modules), "ollama imported"

HERE = os.path.dirname(os.path.abspath(__file__))
args = sys.argv[1:]
CODEBOOK = yaml.safe_load(open(args.pop(args.index("--codebook") + 1))); args.remove("--codebook")
LOGS = {}
while "--log" in args:
    i = args.index("--log"); k, v = args[i + 1].split("=", 1); LOGS[k] = v; del args[i:i + 2]
sources = [a.split("=", 1) for a in args]
SENTINELS = set(CODEBOOK["absence_sentinels"])
ESCAPE = CODEBOOK["escape_token"]
accepted_failing = []   # the accepted attempt's failing entries, as the model wrote them
SENT_RE = re.compile(r"\b(?:" + "|".join(re.escape(x) for x in sorted(SENTINELS, key=len, reverse=True)) + r")\b")
FIELD_FLAGS = {}
for f in CODEBOOK["fields"]:
    prose = " ".join(str(f.get(k) or "") for k in ("definition", "instruction", "decision_criteria"))
    m = SENT_RE.search(prose)
    FIELD_FLAGS[f["name"]] = {
        "field_class": f.get("field_class"),
        "prose_names_a_sentinel": bool(m),
        "prose_sentence": (re.search(r"[^.\n]*" + re.escape(m.group(0)) + r"[^.\n]*", prose).group(0).strip()
                           if m else None),
        "valid_values_include_a_sentinel": any(str(v.get("value")) in SENTINELS
                                               for v in f.get("valid_values") or []),
    }
cells = []          # source, policy, paper, field, class, attempt, failed, codes, n_indices, escape
papers = []
for label, path in sources:
    rows = [json.loads(l) for l in open(path)]
    by_paper = defaultdict(list)
    for r in rows:
        by_paper[r["paper_id"]].append(r)
    for pid, rs in by_paper.items():
        ex = [r for r in rs if (r.get("extra") or {}).get("attempts")]
        if ex:                                   # feedback-retry policy (design 02)
            e = ex[-1]["extra"]
            papers.append({"source": label, "policy": "feedback_retry", "paper_id": pid,
                           "n_attempts": len(e["attempts"]),
                           "n_failed": [a["n_failed"] for a in e["attempts"]],
                           "accepted_attempt": e["accepted_attempt"],
                           "feedback_chars": [a.get("feedback_chars") for a in e["attempts"]]})
            entries, _path = parse_container(e.get("pass1_raw_content"))
            acc = e["attempts"][e["accepted_attempt"] - 1]
            for ent in entries:
                name = ent.get(KEY_FIELD)
                if name in acc["failed_fields"]:
                    val = ent.get(KEY_VALUE)
                    kind = ("escape token" if val == ESCAPE else "absence sentinel"
                            if val in SENTINELS else "no value" if val in (None, "")
                            else "other value")
                    accepted_failing.append({
                        "source": label, "paper_id": pid, "field_name": name,
                        "field_class": acc["fields"][name]["class"],
                        "accepted_attempt": e["accepted_attempt"],
                        "codes": acc["fields"][name]["violations"], "value_kind": kind,
                        "value": (str(val)[:80] if val is not None else None),
                        "indices": ent.get(KEY_INDICES),
                        "n_steps": len(ent.get("reasoning_steps") or []) if isinstance(ent.get("reasoning_steps"), list) else None,
                        "steps_citing_a_unit": sum(1 for st in (ent.get("reasoning_steps") or [])
                                                   if isinstance(st, dict) and st.get(KEY_INDICES)),
                        "steps_marked_criteria": sum(1 for st in (ent.get("reasoning_steps") or [])
                                                     if isinstance(st, dict) and st.get("criteria_application")),
                        "keys": sorted(k for k in ent if k != KEY_FIELD)})
            for a in e["attempts"]:
                failed = set(a["failed_fields"])
                for name, f in a["fields"].items():
                    cells.append((label, "feedback_retry", pid, name, f["class"], a["attempt"],
                                  name in failed, tuple(f["violations"]), len(f["indices"]),
                                  f["escape"]))
        else:
            ex = [r for r in rs if (r.get("extra") or {}).get("fields")]
            if not ex:
                papers.append({"source": label, "policy": "no_elicitation_record",
                               "paper_id": pid, "outcomes": [r["outcome"] for r in rs]})
                continue
            papers.append({"source": label, "policy": "identical_reissue", "paper_id": pid,
                           "n_attempts": len(ex),
                           "n_failed": [len(r["extra"].get("failed_fields", [])) for r in ex],
                           "parse_path": [r["extra"].get("parse_path") for r in ex]})
            for r in ex:
                failed = set(r["extra"].get("failed_fields", []))
                for name, f in r["extra"]["fields"].items():
                    cells.append((label, "identical_reissue", pid, name, f["class"], r["attempt"],
                                  name in failed, tuple(f["violations"]), len(f["indices"]),
                                  f["escape"]))

with open(os.path.join(HERE, "rd1_attempts_cells.csv"), "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["source", "policy", "paper_id", "field_name", "field_class", "attempt",
                "failed", "violation_codes", "n_indices", "escape"])
    for c in cells:
        w.writerow([*c[:6], int(c[6]), "|".join(c[7]), c[8], int(c[9])])


def summarise(sel):
    """sel: cells of one source."""
    out = {}
    att = defaultdict(dict)
    for c in sel:
        att[(c[2], c[3])][c[5]] = c
    n_fields = Counter(); fail = Counter(); codes = defaultdict(Counter)
    by_class = defaultdict(lambda: defaultdict(Counter)); by_field = defaultdict(lambda: defaultdict(Counter))
    for c in sel:
        n_fields[c[5]] += 1
        fail[c[5]] += c[6]
        by_class[c[4]][c[5]]["fields"] += 1
        by_class[c[4]][c[5]]["failed"] += c[6]
        by_field[c[3]][c[5]]["fields"] += 1
        by_field[c[3]][c[5]]["failed"] += c[6]
        if c[6]:
            for code in (c[7] or ("<none recorded>",)):
                codes[c[5]][code] += 1
                by_class[c[4]][c[5]][code] += 1
    trans = Counter(); trans_by_class = defaultdict(Counter); trans_code = defaultdict(Counter)
    for (pid, name), a in att.items():
        if 1 in a and 2 in a:
            k = ("failed" if a[1][6] else "ok") + " -> " + ("failed" if a[2][6] else "ok")
            trans[k] += 1
            trans_by_class[a[1][4]][k] += 1
            if a[1][6]:
                for code in a[1][7]:
                    trans_code[code]["fixed" if not a[2][6] else "still failing"] += 1
            if not a[1][6] and a[2][6]:
                for code in a[2][7]:
                    trans_code[code]["new on attempt 2"] += 1
    # an attempt that returned no entry at all for a whole class
    whole = []
    per_pa = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    for c in sel:
        x = per_pa[(c[2], c[5])][c[4]]; x[0] += 1; x[1] += ("FIELD_MISSING" in c[7])
    for (pid, a), d in sorted(per_pa.items()):
        gone = sorted(cl for cl, (n, m) in d.items() if n and n == m)
        if gone:
            whole.append({"paper_id": pid, "attempt": a, "classes_with_every_field_missing": gone})
    out["attempts_missing_a_whole_class"] = whole
    code_class = defaultdict(lambda: defaultdict(Counter))
    for c in sel:
        if c[6]:
            for code in c[7]:
                code_class[str(c[5])][c[4]][code] += 1
    out["codes_by_attempt_and_class"] = {a: {cl: dict(v) for cl, v in sorted(d.items())}
                                         for a, d in sorted(code_class.items())}
    out["field_attempts"] = {str(k): v for k, v in sorted(n_fields.items())}
    out["failed_by_attempt"] = {str(k): v for k, v in sorted(fail.items())}
    out["codes_by_attempt"] = {str(k): dict(v.most_common()) for k, v in sorted(codes.items())}
    out["by_class"] = {cl: {str(a): dict(v) for a, v in sorted(d.items())} for cl, d in sorted(by_class.items())}
    out["by_field"] = {f: {str(a): dict(v) for a, v in sorted(d.items())} for f, d in sorted(by_field.items())}
    out["transitions_attempt1_to_2"] = dict(trans)
    out["transitions_by_class"] = {k: dict(v) for k, v in sorted(trans_by_class.items())}
    out["attempt1_code_outcome_on_attempt2"] = {k: dict(v) for k, v in sorted(trans_code.items())}
    return out


calls = {}
for label, path in LOGS.items():
    seq = defaultdict(list)
    for line in open(path):
        m = re.search(r"input_fit paper_id=(\d+) (\{.*\})", line)
        if m:
            j = json.loads(m.group(2))
            seq[int(m.group(1))].append({"at": line[:8], "prompt_tokens": j["count"],
                                         "done_reason": j["done_reason"], "model": j["model"]})
    calls[label] = {str(k): v for k, v in sorted(seq.items())}
kinds = defaultdict(Counter)
for x in accepted_failing:
    for code in x["codes"]:
        kinds[x["source"] + " | " + code][x["value_kind"]] += 1
# attempt-1 failures against what each field's own codebook prose says about a sentinel
flag_tab = {}
for label, _ in sources:
    t = defaultdict(lambda: [0, 0])
    for c in cells:
        if c[0] == label and c[5] == 1 and c[1] == "feedback_retry":
            fl = FIELD_FLAGS.get(c[3], {})
            k = f'{c[4]} | prose names a sentinel: {fl.get("prose_names_a_sentinel")}'
            t[k][0] += 1; t[k][1] += c[6]
    if t:
        flag_tab[label] = {k: {"field_attempts": n, "failed": m} for k, (n, m) in sorted(t.items())}
# what accepting attempt 2 gave up: fields that met the contract on attempt 1 and not on 2
accepted2 = []
idx = {(c[0], c[2], c[3], c[5]): c for c in cells}
for pp in papers:
    if pp.get("policy") == "feedback_retry" and pp.get("accepted_attempt") == 2:
        names = sorted({c[3] for c in cells if c[0] == pp["source"] and c[2] == pp["paper_id"]})
        lost = [n for n in names if not idx[(pp["source"], pp["paper_id"], n, 1)][6]
                and idx[(pp["source"], pp["paper_id"], n, 2)][6]]
        gained = [n for n in names if idx[(pp["source"], pp["paper_id"], n, 1)][6]
                  and not idx[(pp["source"], pp["paper_id"], n, 2)][6]]
        accepted2.append({"source": pp["source"], "paper_id": pp["paper_id"],
                          "fixed_by_attempt_2": gained, "met_on_1_and_failed_on_accepted_2": lost})
res = {"papers": papers, "sources": {}, "field_flags_from_codebook": FIELD_FLAGS,
       "papers_where_attempt_2_was_accepted": accepted2,
       "attempt1_failures_by_class_and_sentinel_prose": flag_tab,
       "absence_sentinels": sorted(SENTINELS), "escape_token": ESCAPE,
       "accepted_attempt_failing_entries": accepted_failing,
       "accepted_attempt_failing_value_kinds": {k: dict(v) for k, v in sorted(kinds.items())},
       "calls_from_run_log": calls}
for label, _ in sources:
    sel = [c for c in cells if c[0] == label]
    if sel:
        res["sources"][label] = {"policy": sel[0][1], **summarise(sel)}
print(json.dumps(res, indent=1, sort_keys=True))
