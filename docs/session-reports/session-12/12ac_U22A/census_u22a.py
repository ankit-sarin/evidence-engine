"""12ac-U22A — AST census of U22's sites (INT-g12-6: eval_count recorded) and of the tests coupled to them, in
12ab_U21A/census_u21a.py's form. Read-only: AST only, no scanned module is imported, no database is opened.
usage, from the repository root: census_u22a.py   (stdout: sections, then counts)
Sites: engine/, with scripts/ and analysis/ reported apart — every read of a response field (eval_count,
prompt_eval_count, done_reason, eval_duration, prompt_eval_duration, total_duration, load_duration) by getattr or
attribute, and every write site of the structures that carry them (the telemetry record's record_call, the
_LAST_PASS*_TELEMETRY dicts, the run.log input_fit line, run_manifest's record_call, the vision log line).
Tests: the five classes (R722(a), R723(c), R724(a), R725(b)) over the telemetry record's key set, the input_fit line's
text, the _LAST_PASS*_TELEMETRY keys, prompt_eval_count pins and the vision log line; plus the fake responses that
carry or lack eval_count (the seam a new read would use). Declared completeness: direct names, attribute access,
patch-target strings, setattr on an alias (oc, _oc, E, X, ollama_client, extractor, client_mod), literal strings and
keyword arguments. It does not see a name assembled at runtime, a patch whose target is not a literal, a key read
through a variable, or a test coupled only through a fixture's behaviour."""
import ast, collections, re
from pathlib import Path
FIELDS = {"eval_count", "prompt_eval_count", "done_reason", "eval_duration", "prompt_eval_duration", "total_duration", "load_duration"}
RECORD_KEYS = {"schema", "ts", "arm", "paper_id", "attempt", "outcome", "model", "finish_reason", "raw_content", "raw_content_chars",
               "raw_content_truncated", "spans_parsed", "fields_expected", "missing_fields", "salvage", "input_tokens", "output_tokens",
               "reasoning_tokens", "thinking_present", "thinking_chars", "parse_branch", "pass1_done_reason", "pass1_prompt_eval_count",
               "pass2_prompt_eval_count", "error", "extra"}  # extraction_telemetry.record_call's row, read at HEAD
RECORD_KW = RECORD_KEYS - {"schema", "ts", "raw_content_chars", "raw_content_truncated"}
LANE = {"record_call", "read_calls", "telemetry_path", "SCHEMA_VERSION", "_LAST_PASS1_TELEMETRY", "_LAST_PASS2_TELEMETRY",
        "_check_input_was_read", "extract_pass1_reasoning", "extract_pass2_structured", "run_pass1", "record_active_ollama_call",
        "record_run_event", "parse_with_vision"}
LAST_KEYS = {"finish_reason", "prompt_eval_count", "raw_content", "thinking_present", "thinking_chars", "parse_branch", "model", "elicitation",
             "n_contract_unmet", "accepted_pass1_attempt"}
INPUT_FIT_KEYS = {"model", "ceiling", "chars", "estimate_low", "count", "ratio", "done_reason", "loaded_context"}
STRINGS = ("input_fit", "extraction-telemetry", "Vision page", "eval_count=", "prompt_eval_count=", "done_reason=", "UNVERIFIED")
ALIASES = {"oc", "_oc", "E", "X", "ollama_client", "extractor", "client_mod", "T", "P", "pipeline"}
def scopes(tree):
    out = {}
    def walk(node, name):
        for ch in ast.iter_child_nodes(node):
            n = f"{name}.{ch.name}" if isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) else name
            out[id(ch)] = n; walk(ch, n)
    walk(tree, "<module>"); return out
def line(src, n): return src.splitlines()[n - 1].strip()[:150]
sites = collections.defaultdict(list)
for root in ("engine", "scripts", "analysis"):
    for p in sorted(Path(root).rglob("*.py")):
        src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t)
        for n in ast.walk(t):
            rec = (str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)) if hasattr(n, "lineno") else None
            if isinstance(n, ast.Call):
                f = ast.unparse(n.func); tail = f.rsplit(".", 1)[-1]
                if tail == "getattr" and len(n.args) >= 2 and isinstance(n.args[1], ast.Constant) and n.args[1].value in FIELDS:
                    sites[root, f"read: getattr {n.args[1].value}"].append(rec)
                if tail == "record_call":
                    kws = sorted(k.arg for k in n.keywords if k.arg)
                    sites[root, "write: record_call (telemetry or run_calls)"].append(rec + (f"kw={kws}",))
                if tail in ("update", "setdefault") and "_LAST_PASS" in f:
                    sites[root, "write: _LAST_PASS*_TELEMETRY"].append(rec + (f"kw={sorted(k.arg for k in n.keywords if k.arg)}",))
                if tail in ("info", "warning", "error") and n.args and isinstance(n.args[0], ast.Constant) and isinstance(n.args[0].value, str) \
                        and (n.args[0].value.startswith("input_fit") or n.args[0].value.startswith("Vision page")):
                    sites[root, "write: log line (input_fit / Vision page)"].append(rec)
            if isinstance(n, ast.Attribute) and n.attr in FIELDS and not (isinstance(getattr(n, "ctx", None), ast.Store)):
                sites[root, f"read: attribute .{n.attr}"].append(rec)
            if isinstance(n, ast.Subscript) and isinstance(n.slice, ast.Constant) and n.slice.value in ("_LAST_PASS1_TELEMETRY", "_LAST_PASS2_TELEMETRY"):
                sites[root, "write: _LAST_PASS* subscript"].append(rec)
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and n.value in FIELDS and root == "engine":
                sites[root, f"literal key '{n.value}'"].append(rec)
tests = collections.defaultdict(list)
for p in sorted(Path("tests").rglob("*.py")):
    src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t)
    for n in ast.walk(t):
        rec = (str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)) if hasattr(n, "lineno") else None
        if isinstance(n, ast.Call):
            f = ast.unparse(n.func)
            if f.endswith("setattr") and len(n.args) >= 2:
                a = ast.unparse(n.args[0]); b = ast.unparse(n.args[1]).strip("'\"")
                if b in LANE and a in ALIASES: tests["ii_module_alias_patch"].append(rec + (b,))
                elif b == "ollama_chat" or b == "_client": tests["v_whole_function_or_client"].append(rec + (b,))
            if f in ("patch", "patch.object", "mock.patch", "mock.patch.object") and n.args:
                s = " ".join(ast.unparse(x) for x in n.args[:2])
                if re.search(r"\b(record_call|_check_input_was_read|extract_pass1_reasoning|extract_pass2_structured|run_pass1|parse_with_vision)\b", s):
                    tests["i_patch_string_lane"].append(rec + (s[:80],))
                elif re.search(r"\bollama_chat\b|\b_client\b", s): tests["v_whole_function_or_client"].append(rec + (s[:80],))
            if f.rsplit(".", 1)[-1] == "record_call":
                tests["i_direct_call_record_call"].append(rec + (f"kw={sorted(k.arg for k in n.keywords if k.arg)}",))
            if f.rsplit(".", 1)[-1] in ("SimpleNamespace", "Mock", "MagicMock", "_resp", "_response", "_fake_response", "FakeResponse") or f.endswith("Namespace"):
                kws = {k.arg for k in n.keywords if k.arg}
                if "prompt_eval_count" in kws or "done_reason" in kws or "eval_count" in kws:
                    tests["fake_response_with_eval_count" if "eval_count" in kws else "fake_response_without_eval_count"].append(rec + (sorted(kws & FIELDS),))
            for kw in n.keywords:
                if kw.arg in RECORD_KW and f.rsplit(".", 1)[-1] == "record_call":
                    tests["iii_record_kw"].append(rec + (kw.arg,))
        if isinstance(n, ast.Assign) and any(isinstance(tg, ast.Attribute) and tg.attr in FIELDS for tg in n.targets):
            tests["fake_response_attr_assign"].append(rec + (sorted(tg.attr for tg in n.targets if isinstance(tg, ast.Attribute)),))
        if isinstance(n, ast.Name) and n.id in LANE: tests["i_direct_name"].append(rec + (n.id,))
        if isinstance(n, ast.Attribute) and n.attr in LANE: tests["i_direct_name"].append(rec + (n.attr,))
        if isinstance(n, ast.Subscript) and isinstance(n.slice, ast.Constant) and isinstance(n.slice.value, str):
            k = n.slice.value
            if k in ("pass1_prompt_eval_count", "pass2_prompt_eval_count", "pass1_done_reason", "output_tokens", "input_tokens", "finish_reason", "schema") :
                tests["iii_record_key_read"].append(rec + (k,))
            if k in ("prompt_eval_count", "finish_reason") and "_LAST_PASS" in ast.unparse(n.value): tests["iii_last_pass_key"].append(rec + (k,))
        if isinstance(n, ast.Compare) and any(isinstance(c, (ast.Dict, ast.Set)) for c in n.comparators) and ("input_fit" in ast.unparse(n) or "row" in ast.unparse(n) or "tel" in ast.unparse(n)):
            tests["iv_exact_set_or_dict_compare"].append(rec)
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            v = n.value
            for m in STRINGS:
                if m in v: tests["vi_literal_string"].append(rec + (v[:80].replace("\n", " "),)); break
            if re.fullmatch(r"[0-9a-f]{64}", v) and ("telemetry" in src or "record_call" in src): tests["iv_hard_coded_hash_in_telemetry_file"].append(rec)
print("# SITES")
for (root, k), v in sorted(sites.items()):
    print(f"## {root} / {k}: {len(v)}")
    for x in v: print("  ", *x)
print("\n# TESTS")
for k, v in sorted(tests.items()):
    print(f"## {k}: {len(v)} hits in {len({x[0] for x in v})} files")
    c = collections.Counter(x[0] for x in v)
    print("   by file:", ", ".join(f"{a} {b}" for a, b in c.most_common()))
    for x in v: print("  ", *x)
print("\n# COUNTS")
for (root, k), v in sorted(sites.items()): print(f"site {root} / {k}: {len(v)}")
for k, v in sorted(tests.items()): print(f"test {k}: {len(v)} / {len({x[0] for x in v})} files")
