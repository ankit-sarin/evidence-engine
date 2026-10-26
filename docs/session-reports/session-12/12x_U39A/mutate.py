"""12x-U39A Q4 driver: each mutation applied alone in the worktree, the U39 file run against
it (from ~/scratch/12x-u39a/wt-run/, never the worktree's tests/), both pins taken, the diff
saved, then `git checkout -- engine` and the clean status recorded. No model call."""
import json, os, re, subprocess, sys
H = os.path.expanduser("~"); S = f"{H}/scratch/12x-u39a"; WT = f"{S}/wt"; OUT = f"{S}/mut"
PY = f"{H}/projects/evidence-engine/.venv/bin/python"; TEST = f"{S}/wt-run/test_u39_runtime_equals_hash.py"
os.makedirs(OUT, exist_ok=True)
X1 = '[{"role": "user", "content": "M1"}]'; X2 = '[{"role": "user", "content": "M2"}]'
EC, SC, FT, EX, PL, AU, PP, OP = ("engine/core/effective_config.py", "engine/agents/screener.py",
    "engine/agents/ft_screener.py", "engine/agents/extractor.py", "engine/elicitation/pipeline.py",
    "engine/agents/auditor.py", "engine/parsers/pdf_parser.py", "engine/utils/ollama_preflight.py")
SCR = "response = ollama_chat(messages=build_messages(paper, spec, role=role), **cfg.kwargs())"
SCR_R = "        return build_messages(SENTINEL_PAPER, spec, role=role)"
FT_R = "        return build_ft_messages(SENTINEL_TEXT, spec, which)"
M = [
 ("abstract_screen_primary", "M1", SC, SCR, SCR.replace("role=role),", f"role=role) + ({X1} if role == \"primary\" else []),")),
 ("abstract_screen_primary", "M2", EC, SCR_R, SCR_R + f" + ({X2} if role == \"primary\" else [])"),
 ("abstract_screen_verifier", "M1", SC, SCR, SCR.replace("role=role),", f"role=role) + ({X1} if role == \"verifier\" else []),")),
 ("abstract_screen_verifier", "M2", EC, SCR_R, SCR_R + f" + ({X2} if role == \"verifier\" else [])"),
 ("ft_screen_primary", "M1", FT, 'messages=build_ft_messages(paper_text, spec, "primary"),', f'messages=build_ft_messages(paper_text, spec, "primary") + {X1},'),
 ("ft_screen_primary", "M2", EC, FT_R, FT_R + f' + ({X2} if which == "primary" else [])'),
 ("ft_screen_verifier", "M1", FT, 'messages=build_ft_messages(paper_text, spec, "verifier"),', f'messages=build_ft_messages(paper_text, spec, "verifier") + {X1},'),
 ("ft_screen_verifier", "M2", EC, FT_R, FT_R + f' + ({X2} if which == "verifier" else [])'),
 ("extract_pass1", "M1", EX, "result = ollama_chat(paper_id=paper_id, messages=pass1_messages(prompt),", f"result = ollama_chat(paper_id=paper_id, messages=pass1_messages(prompt) + {X1},"),
 ("extract_pass1", "M2", EC, "            return ex.pass1_messages(prompt)", f"            return ex.pass1_messages(prompt) + {X2}"),
 ("extract_pass2", "M1", EX, "messages=pass2_messages(prompt, reasoning_trace),", f"messages=pass2_messages(prompt, reasoning_trace) + {X1},"),
 ("extract_pass2 (non-elicited)", "M2", EC, '            return ex.pass2_messages(prompt, "R")', f'            return ex.pass2_messages(prompt, "R") + {X2}'),
 ("extract_pass2 (elicited)", "M2", EC, "return ex.pass2_messages(prompt, el.sentinel_priming_message(spec, codebook_path))", f"return ex.pass2_messages(prompt, el.sentinel_priming_message(spec, codebook_path)) + {X2}"),
 ("extract_retry_snippet", "M1", EX, "messages=retry_snippet_messages(field_name, value, paper_text),", f"messages=retry_snippet_messages(field_name, value, paper_text) + {X1},"),
 ("extract_retry_snippet", "M2", EC, '        return ex.retry_snippet_messages("f", "v", SENTINEL_TEXT)', f'        return ex.retry_snippet_messages("f", "v", SENTINEL_TEXT) + {X2}'),
 ("elicitation_pass1", "M1", PL, "response, req_hash = ollama_chat(paper_id=paper_id, messages=pass1_messages(prompt),", f"response, req_hash = ollama_chat(paper_id=paper_id, messages=pass1_messages(prompt) + {X1},"),
 ("elicitation_pass1", "M2", EC, "        messages = el.pass1_messages(el.sentinel_pass1_prompt(spec, codebook_path))", f"        messages = el.pass1_messages(el.sentinel_pass1_prompt(spec, codebook_path)) + {X2}"),
 ("elicitation_pass1", "M2v (variant count)", EC, "        if el.MAX_PASS1_ATTEMPTS > 1:", "        if False and el.MAX_PASS1_ATTEMPTS > 1:"),
 ("audit", "M1", AU, "messages=build_audit_messages(span, field_type=field_type), **cfg.kwargs())", f"messages=build_audit_messages(span, field_type=field_type) + {X1}, **cfg.kwargs())"),
 ("audit", "M2", EC, '                + build_audit_messages(span, field_type="categorical"))', f'                + build_audit_messages(span, field_type="categorical") + {X2})'),
 ("audit", "M2v (variant count)", EC, '                + build_audit_messages(span, field_type="categorical"))', '                + [])'),
 ("vision_parse", "M1", PP, "messages=vision_messages(img_b64),", f"messages=vision_messages(img_b64) + {X1},"),
 ("vision_parse", "M2", EC, "        return vision_messages(SENTINEL_IMAGE)", f"        return vision_messages(SENTINEL_IMAGE) + {X2}"),
 ("preflight", "M1", OP, "messages=[dict(m) for m in PREFLIGHT_MESSAGES],", f"messages=[dict(m) for m in PREFLIGHT_MESSAGES] + {X1},"),
 ("preflight", "M2", EC, "        return [dict(m) for m in PREFLIGHT_MESSAGES]", f"        return [dict(m) for m in PREFLIGHT_MESSAGES] + {X2}"),
]
def run(*a, **k): return subprocess.run(a, cwd=WT, capture_output=True, text=True, **k)
env = dict(os.environ, PYTHONPATH=".:tests")
def pins():
    r = run(PY, "docs/session-reports/session-12/12l_U04/pin_tuples.py", "review_specs/surgical_autonomy.yaml",
            "data/surgical_autonomy/extraction_codebook.yaml", env=env)
    if r.returncode: return {"error": r.stderr[-400:]}
    d = json.loads(r.stdout); return {"elicited": d["elicited"]["pin"], "non_elicited": d["non_elicited"]["pin"]}
def pytest(tag):
    r = run(PY, "-m", "pytest", "-p", "conftest", TEST, "-v", "-rfE", "-p", "no:cacheprovider", "--rootdir=.",
            "-c", "pyproject.toml", f"--basetemp={S}/bt/{tag}", env=env, timeout=600)
    open(f"{OUT}/{tag}_pytest.txt", "w").write(r.stdout + r.stderr)
    failed = sorted(set(re.findall(r"^FAILED \S*::(\S+)", r.stdout, re.M)))
    return r.returncode, failed
assert run("git", "status", "--porcelain").stdout == "", "worktree not clean at start"
summary = {"baseline": {}}
rc, failed = pytest("baseline"); summary["baseline"] = {"rc": rc, "failed": failed, "pins": pins()}
print("baseline", summary["baseline"], flush=True)
for i, (stage, kind, path, old, new) in enumerate(M, 1):
    tag = f"m{i:02d}"
    f = os.path.join(WT, path); src = open(f, encoding="utf-8").read()
    assert src.count(old) == 1, (tag, "anchor not unique")
    open(f, "w", encoding="utf-8").write(src.replace(old, new))
    diff = run("git", "diff").stdout; open(f"{OUT}/{tag}.diff", "w").write(diff)
    rc, failed = pytest(tag); p = pins()
    run("git", "checkout", "--", "engine")
    status = run("git", "status", "--porcelain").stdout
    summary[tag] = {"stage": stage, "kind": kind, "file": path, "rc": rc, "failed": failed, "pins": p,
                    "status_after_revert": status or "clean"}
    print(tag, stage, kind, "rc", rc, "failed", failed, "pins", p, "revert:", status or "clean", flush=True)
    assert status == "", "revert left the worktree dirty"
json.dump(summary, open(f"{OUT}/summary.json", "w"), indent=1)
