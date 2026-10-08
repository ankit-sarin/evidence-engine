"""12h RD-6 — git history of screen_paper's pass_number handling. Read-only (git show only).
usage, from the repository root: rd6_history.py   (stdout: JSON)

For every commit that touched engine/agents/screener.py (git log --follow), parse the
file at that commit and report, for `screen_paper`:
  - whether the parameter `pass_number` is read anywhere in the body (AST Name load);
  - every `temperature` / `seed` mention and every `options=` keyword in the body;
  - the model expression and the keyword arguments of the chat call;
and, for every function in the file that calls screen_paper more than once in one loop
body, the two calls as written (so a difference between pass 1 and pass 2 arguments shows).
Also lists every other file at HEAD and in history that defines or calls screen_paper.
"""
import ast, json, subprocess


def git(*a):
    return subprocess.run(["git", *a], capture_output=True, text=True, check=True).stdout


PATH = "engine/agents/screener.py"
commits = [l.split(" ", 2) for l in
           git("log", "--follow", "--format=%H %ad %s", "--date=iso-strict", "--", PATH).splitlines()]
out = []
for sha, date, subject in reversed(commits):
    try:
        src = git("show", f"{sha}:{PATH}")
    except subprocess.CalledProcessError:
        out.append({"commit": sha, "date": date, "subject": subject, "file": "absent"}); continue
    tree = ast.parse(src)
    row = {"commit": sha, "date": date, "subject": subject}
    for fn in ast.walk(tree):
        if isinstance(fn, ast.FunctionDef) and fn.name == "screen_paper":
            body = ast.Module(body=fn.body, type_ignores=[])
            seg = ast.get_source_segment(src, fn) or ""
            row["params"] = [a.arg for a in fn.args.args]
            row["pass_number_reads_in_body"] = sum(
                1 for n in ast.walk(body) if isinstance(n, ast.Name) and n.id == "pass_number")
            row["temperature_mentions"] = seg.count("temperature")
            row["seed_mentions"] = seg.count("seed")
            calls = []
            for n in ast.walk(body):
                if isinstance(n, ast.Call) and ast.unparse(n.func) in (
                        "ollama.chat", "ollama_chat", "client.chat"):
                    calls.append({"callee": ast.unparse(n.func),
                                  "keywords": {k.arg or "**": ast.unparse(k.value)[:90]
                                               for k in n.keywords
                                               if (k.arg or "**") != "messages"}})
            row["chat_calls"] = calls
    pairs = []
    for fn in ast.walk(tree):
        if isinstance(fn, ast.FunctionDef):
            cs = [ast.unparse(n) for n in ast.walk(fn)
                  if isinstance(n, ast.Call) and ast.unparse(n.func) == "screen_paper"]
            if cs:
                pairs.append({"function": fn.name, "screen_paper_calls": sorted(cs)})
    row["callers_in_file"] = pairs
    out.append(row)

# collapse runs of commits whose screen_paper facts are identical
def key(r):
    return json.dumps({k: r.get(k) for k in ("params", "pass_number_reads_in_body",
                       "temperature_mentions", "seed_mentions", "chat_calls",
                       "callers_in_file")}, sort_keys=True)

# every screen_paper( call line ever added, in any file, on any ref
added = {}
cur = None
for line in git("log", "--all", "--format=@@@%h %ad", "--date=short", "-G", r"screen_paper\(",
                "-p", "--", "*.py").splitlines():
    if line.startswith("@@@"):
        cur = line[3:]
    elif line.startswith("+") and not line.startswith("+++") and "screen_paper(" in line \
            and "def " not in line and "ft_screen_paper" not in line:
        added.setdefault(" ".join(line[1:].split()), []).append(cur)
call_lines = [{"line": k, "first_added": sorted(v, key=lambda x: x.split()[1])[0]}
              for k, v in sorted(added.items())]

head_callers = git("grep", "-n", "screen_paper(", "HEAD", "--", "*.py").splitlines()
defs_in_history = git("log", "--all", "--format=%h %ad %s", "--date=short",
                      "-S", "def screen_paper").splitlines()
print(json.dumps({
    "path": PATH, "commits_touching_file": len(out),
    "pass_number_ever_read_in_screen_paper": any(r.get("pass_number_reads_in_body") for r in out),
    "seed_ever_mentioned_in_screen_paper": any(r.get("seed_mentions") for r in out),
    "per_commit": out,
    "distinct_states": len({key(r) for r in out}),
    "every_screen_paper_call_line_ever_added": call_lines,
    "screen_paper_call_sites_at_HEAD": [l.split(":", 1)[1][:150] for l in head_callers],
    "commits_adding_or_removing_def_screen_paper": defs_in_history,
}, indent=1, sort_keys=True))
