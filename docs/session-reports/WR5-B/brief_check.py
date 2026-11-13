#!/usr/bin/env python3
"""WR5-B prototype brief checker (evidence-engine lane, workflow review item 5). Prototype only: not installed.

usage:
  brief_check.py check <brief.txt> [--repo <path> --commit <hash> [--also-repo <path>]...]
      Prints one line per hit: <check>\t<region>\t<line>\t<matched text, at most 200 chars>.
      Exit 1 when any check hits, 0 when none does. Text-only checks always run; the repository
      checks (K2, K11, K12, K13, K14) run only with --repo and --commit, reading the tree at that
      commit through git (show / cat-file / ls-tree), never a checkout.
  brief_check.py controls <dir> --repo <path> --commit <hash>
      Runs every <K>_hit.txt / <K>_miss.txt pair in <dir>: the named check must hit the first and must
      not hit the second. Prints PASS / FAIL per file.
  brief_check.py measure --out <hits.jsonl> --summary <summary.json> --repo <path> [--also-repo <path>]...
      Measures sets S1-S4 (the WR5-B brief and its resume ruling) and writes the hits of S1, S2 and S4.

Read-only on everything it inspects. Standard library only.

Region rule (WR5-B resume ruling, clause (b)): a text is split at the first line that, after leading
whitespace, begins "TASK ["; region PRE is the text before that line, region BRIEF that line onward.
Every check runs on each region separately. A text with no such line is one region, UNSPLIT.
Line numbers are lines of the whole text, 1-based.
"""
import argparse, json, os, re, subprocess, sys
from collections import defaultdict

# --------------------------------------------------------------------------------------------- regions

def split_regions(text):
    """[(region, first line number, lines)], and the number of lines that begin "TASK [" after whitespace."""
    lines = text.split("\n")
    idx = [i for i, l in enumerate(lines) if l.lstrip().startswith("TASK [")]
    if not idx:
        return [("UNSPLIT", 1, lines)], 0
    k = idx[0]
    return [("PRE", 1, lines[:k]), ("BRIEF", k + 1, lines[k:])], len(idx)


def logical(lines, start):
    """Lines with backslash continuations joined; (line number of the first physical line, text)."""
    out, buf, first = [], None, None
    for i, l in enumerate(lines):
        if buf is None:
            buf, first = l, start + i
        else:
            buf = buf[:-1] + " " + l.lstrip()
        if not buf.rstrip().endswith("\\"):
            out.append((first, buf)); buf = None
    if buf is not None:
        out.append((first, buf))
    return out


def clip(s):
    return s.strip()[:200]

# --------------------------------------------------------------------------------------------- text-only checks

# K1 — R730(c): "Rule from here: every full commit hash a brief or a ruling carries is 40 hexadecimal
#   characters, checked by the architect before issue; CC resolves every full hash a brief names
#   with git rev-parse --verify at the step that first uses it, and a hash that does not resolve
#   is a stop."
# Narrower reading (what counts as "full"): a lowercase hex token of 33-39 or 41-47 characters, holding
# at least one digit and one letter a-f, bounded by non-alphanumerics. Abbreviations (7-12) and sha256
# values (64) are not full commit hashes and are not flagged.
HEXTOK = re.compile(r"(?<![0-9A-Za-z_])[0-9a-f]+(?![0-9A-Za-z_])")

def k1(lines, start):
    out = []
    for i, l in enumerate(lines):
        for m in HEXTOK.finditer(l):
            t = m.group(0); n = len(t)
            if (33 <= n <= 39 or 41 <= n <= 47) and re.search(r"\d", t) and re.search(r"[a-f]", t):
                out.append((start + i, t))
    return out


# K3 — R720(c): "From here a check-and-commit chain contains no pipe: the check writes its own evidence
#   file (a redirect inside the check, or the script writes it), and the chain is <check> && <commit>,
#   one line, links joined by && (R716(e)). A tee in the chain is a defect of the chain whatever the
#   check printed."
# Narrower reading: a logical line holding "git commit" and "&&" (a chain), with a single "|" (not "||")
# or the word tee. Markdown table rows (lines starting "|") are not chains.
def k3(lines, start):
    out = []
    for n, l in logical(lines, start):
        if l.lstrip().startswith("|"):
            continue
        if re.search(r"\bgit\s+commit\b", l) and "&&" in l and (re.search(r"(?<!\|)\|(?!\|)", l) or re.search(r"\btee\b", l)):
            out.append((n, l))
    return out


# K4 — R656(d): "The interpreter is .venv/bin/python; ... Briefs from here name .venv/bin/python for
#   every repository script."
# Narrower reading: an interpreter word (python, python3, python3.N, or a path ending in one) followed,
# after options, by a repository script path (scripts/, engine/, analysis/, tests/,
# docs/session-reports/tools/ ... .py) or "-m engine|analysis|scripts"; a hit when the interpreter does
# not end in ".venv/bin/python". "-m pytest" is not a repository script and is not matched.
K4RE = re.compile(r"(?<![\w./-])((?:[\w.~$/-]*/)?python3?(?:\.\d+)?)\s+(?:-[A-Za-z]+\s+)*"
                  r"((?:\./)?(?:scripts|engine|analysis|tests|docs/session-reports/tools)/[\w./-]+\.py"
                  r"|-m\s+(?:engine|analysis|scripts)(?:\.[\w.]+)?)")

def k4(lines, start):
    out = []
    for n, l in logical(lines, start):
        for m in K4RE.finditer(l):
            if not m.group(1).endswith(".venv/bin/python"):
                out.append((n, m.group(0)))
    return out


# K5 — R658(c): "From here, no helper command composed for a brief deletes any path outside
#   ~/scratch/<session>/; a deletion elsewhere is its own named step with the PI's ruling."
# Narrower reading: rm / rmdir / unlink / shred (git rm included) or find ... -delete, with a path-like
# argument (holds "/" or starts "~" or ".") that does not start ~/scratch/, $HOME/scratch/ or
# /home/ankitsarin/scratch/. An argument starting "$" cannot be resolved from text and is not flagged.
DELRE = re.compile(r"(?<![\w./-])(rm|rmdir|unlink|shred)((?:\s+(?!&&|\|\||;|\||>)[^\s;&|>]+)+)")
FINDDEL = re.compile(r"(?<![\w./-])find\s+(\S+)[^\n]*?\s-delete\b")
SCRATCH = re.compile(r"(~|\$HOME|/home/ankitsarin)/scratch/")

def _bad_path(a):
    a = a.strip("`'\"()")
    if not a or a.startswith("-") or a.startswith("$"):
        return False
    if not ("/" in a or a.startswith("~") or a.startswith(".")):
        return False
    return not SCRATCH.match(a)

def k5(lines, start):
    out = []
    for n, l in logical(lines, start):
        for m in DELRE.finditer(l):
            if any(_bad_path(a) for a in m.group(2).split()):
                out.append((n, m.group(0)))
        for m in FINDDEL.finditer(l):
            if _bad_path(m.group(1)):
                out.append((n, m.group(0)))
    return out


# K6 — R697(b): "The pkill -f self-match is a reading: briefs name a pid, never a pattern, when a
#   process is to be stopped."
# Reading: pkill or killall followed by an argument, kill fed by $(pgrep ...), or pgrep piped to xargs kill.
K6RE = re.compile(r"(?<![\w-])(?:pkill|killall)\s+\S+|\bkill\b[^\n]*\$\(\s*pgrep|\bpgrep\b[^\n]*\|\s*xargs\s+(?:-\S+\s+)*kill\b")

def k6(lines, start):
    return [(n, m.group(0)) for n, l in logical(lines, start) for m in K6RE.finditer(l)]


# K7 — R676(d)(iii): "from here an ad-hoc pytest run in a brief passes --basetemp under the session's
#   scratch directory, so the shared root holds gates' and the nightly's directories only."
# Narrower reading: a pytest command (-m pytest, or the word pytest followed by an option, tests/, a
# .py file or a quote) on a logical line with no --basetemp. Not ad hoc, so not flagged: a line naming
# launch_gate.sh / run_gate.sh, and the standard gate (the marker expression
# "not network and not ollama and not integration" with no file or test-path selection).
PYT = re.compile(r"(?:-m\s+pytest|(?<![\w.@/-])pytest)(?=\s+(?:-|tests/|\S+\.py|['\"]))")

def k7(lines, start):
    out = []
    for n, l in logical(lines, start):
        m = PYT.search(l)
        if not m or "--basetemp" in l or re.search(r"(launch|run)_gate\.sh", l):
            continue
        rest = l[m.end():]
        if "not network and not ollama and not integration" in l and not re.search(r"tests/\S|\S\.py\b", rest):
            continue
        out.append((n, l))
    return out


# K8 — R705(c): "From here a fix brief that edits engine/ lists docs/inventory/ among its touched paths."
#   R725(c): "from here every engine-commit brief lists docs/inventory/ in its Touched list by default."
# Narrower reading: a "Touched:" / "Touches:" label at a line's start; the list runs to a blank line, a
# column-0 label, or a line starting STOP / DO NOT TOUCH / Reads, and is cut at "Reads:", "STOP" or
# "DO NOT TOUCH" inside it. A hit when the list names engine/ (not as part of "evidence-engine/") and
# does not name docs/inventory/.
TOUCHRE = re.compile(r"\s*Touch(?:ed|es)\s*(?:\([^)]*\))?\s*:")
COL0LABEL = re.compile(r"[A-Z][A-Z0-9 /-]{2,}(?::|\s\(|\s\[|$)")

def k8(lines, start):
    out = []
    for i, l in enumerate(lines):
        m = TOUCHRE.match(l)
        if not m:
            continue
        parts = [l[m.end():]]
        for j in range(i + 1, len(lines)):
            nl = lines[j]
            if not nl.strip() or COL0LABEL.match(nl) or re.match(r"\s*(STOP\b|DO NOT TOUCH|Reads?:)", nl):
                break
            parts.append(nl.strip())
        block = " ".join(parts)
        cut = re.search(r"\b(Reads?:|STOP\b|DO NOT TOUCH)", block)
        if cut:
            block = block[:cut.start()]
        if re.search(r"(?<![\w-])engine/", block) and "docs/inventory/" not in block:
            out.append((start + i, block))
    return out


# Assumption-ledger parser, shared by K9 and K11. The ledger starts at a line beginning (after whitespace)
# "ASSUMPTION LEDGER" or "ASSUMPTIONS IN THIS RULING" and ends at the next column-0 label. Inside it a
# section starts at a line beginning MEASURED, READ or INFERRED; its header is that line up to its first
# colon, its body the rest of that line and the following lines.
def ledger_sections(lines):
    secs, cur, inl = [], None, False
    for i, l in enumerate(lines):
        if re.match(r"\s*(ASSUMPTION LEDGER|ASSUMPTIONS IN THIS RULING)\b", l):
            inl, cur = True, None
            continue
        if inl and COL0LABEL.match(l):
            inl, cur = False, None
            continue
        if not inl:
            continue
        m = re.match(r"\s*(MEASURED|READ|INFERRED)\b", l)
        if m:
            rest = l[m.end():]
            c = rest.find(":")
            header, body = (rest[:c], rest[c + 1:]) if c >= 0 else (rest, "")
            cur = {"kind": m.group(1), "line": i, "header": header, "body": [(i, body)]}
            secs.append(cur)
        elif cur is not None:
            cur["body"].append((i, l))
    return secs

ITEMLABEL = re.compile(r"(?<![\w(])([A-Z]{1,2}\d+[a-z]?)\.(?=\s)")


# K9 — R667(e): "from here an INFERRED item names only the fact that must hold, and a brief that wants a
#   value checked names it as a separate reading."
# Narrower reading: a free-standing numeral (digits, with "." or "," groups) in an INFERRED section's body,
# after item labels (I1., B2., X3.) are removed. Not flagged: digits inside an identifier (R722(a), 12y,
# U18B, a hash, a path, a date or time joined by "-" or ":"), and a number directly after a reference
# word (step, item, section, block, rule, phase, part, row, §).
NUMRE = re.compile(r"(?<![\w./:#@$~§(-])\d+(?:[.,]\d+)*(?![\w/(:-])")
REFWORD = re.compile(r"(?:\b(?:steps?|items?|sections?|blocks?|rules?|phases?|parts?|rows?)\s*|§\s*)$", re.I)

def k9(lines, start):
    out = []
    for s in ledger_sections(lines):
        if s["kind"] != "INFERRED":
            continue
        for i, t in s["body"]:
            t2 = ITEMLABEL.sub(lambda m: " " * len(m.group(0)), t)
            for m in NUMRE.finditer(t2):
                if REFWORD.search(t2[:m.start()]):
                    continue
                out.append((start + i, t.strip()))
                break
    return out


# K10 — R723(a): "the brief predicted a count ("exactly 3") ... From here a brief states a "−" count as
#   "one per noted row, N rows named" and lets CC compute N from the rows";
#   R750(c): "a brief that names a tool states no "−" count the tool's own run does not supply";
#   R772(c): "it matches a count only when a record supplies that count (a tool's own report, a prior
#   run's figure), never a prediction of the diff's shape made before the diff exists."
# Reading: "exactly N" (N a numeral or a number word to twelve); a "−" count stated with a number
# ('"−" count is 2', '"−" lines: 3', 'one "−" line', '−5' in the numstat form).
NUMW = r"(?:zero|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|\d+)"
K10RE = re.compile(rf"\bexactly\s+{NUMW}\b"
                   rf"|[\"“][−-][\"”]\s+(?:counts?|lines?)\s*(?:is|of|=|:)?\s*{NUMW}\b"
                   rf"|\b{NUMW}\s+[\"“]?−[\"”]?\s+lines?\b"
                   rf"|(?<![\w−])−\d+\b", re.I)

def k10(lines, start):
    return [(start + i, m.group(0)) for i, l in enumerate(lines) for m in K10RE.finditer(l)]

# --------------------------------------------------------------------------------------------- repository checks

class Repo:
    """Read-only views of one commit of a repository, through git."""
    def __init__(self, path, commit, also=()):
        self.path, self.commit, self.also = path, commit, list(also)
        self._tree = self._blobs = None
        self._cache = {}

    def git(self, *a, repo=None):
        return subprocess.run(["git", "-C", repo or self.path, *a], capture_output=True, text=True)

    def tree(self):
        if self._tree is None:
            r = self.git("ls-tree", "-r", "-t", "--name-only", self.commit)
            self._tree = set(r.stdout.split("\n")) - {""}
            self._blobs = [p for p in self._tree]
        return self._tree

    def exists(self, p):
        return p.rstrip("/") in self.tree()

    def show(self, p):
        if p not in self._cache:
            r = subprocess.run(["git", "-C", self.path, "show", f"{self.commit}:{p}"], capture_output=True)
            self._cache[p] = r.stdout.decode("utf-8", "replace") if r.returncode == 0 else None
        return self._cache[p]

    def commit_exists(self, h):
        return any(self.git("cat-file", "-e", f"{h}^{{commit}}", repo=r).returncode == 0 for r in [self.path, *self.also])


# K2 — R783(d): "a full hash is proved to exist by git cat-file -e <hash>^{commit} in the repository it
#   belongs to; git rev-parse --verify on a 40-character hash proves its form only and is not the
#   existence check."
# Narrower reading of "the repository it belongs to": the repository given and every --also-repo (the
# measurement passes ~/claude-config, the lane's second repository); a hit only when no one of them holds
# the commit.
HASH40 = re.compile(r"(?<![0-9A-Za-z_])[0-9a-f]{40}(?![0-9A-Za-z_])")

def k2(lines, start, repo):
    return [(start + i, m.group(0)) for i, l in enumerate(lines) for m in HASH40.finditer(l)
            if not repo.commit_exists(m.group(0))]


# K11 — R733(a): "From here a READ item quoting a report is quoted from that report's text, not from a
#   later report's recollection of it." R756(b): "a brief's wrapped quote is compared
#   whitespace-normalised, the words verbatim".
# Narrower reading: in each READ or MEASURED section, items split at item labels (R1., M2.; the whole body
# if none); the cited file is the first file-like token (…​.md/.py/.txt/.json/.jsonl/.yaml/.yml/.sh/.toml/
# .csv/.tsv) in the item, else in the section header, resolved at the commit to the one tree path equal
# to it or ending in "/"+token (none, or more than one: the item is not checked). Each double-quoted run
# of three or more words is split at "…" / "..." and every piece must occur in the file,
# whitespace-normalised. A hit names the quote.
FILETOK = re.compile(r"(?<![\w/.~-])((?:[\w.-]+/)*[\w.-]+\.(?:md|py|txt|json|jsonl|yaml|yml|sh|toml|csv|tsv))(?![\w/])")
QUOTE = re.compile(r'"([^"]+)"|“([^”]+)”')

def ws(s):
    return " ".join(s.split())

def _resolve(repo, tok):
    tok = re.sub(r"^.*?/projects/evidence-engine/", "", tok)
    tr = repo.tree()
    if tok in tr:
        return tok
    c = [p for p in tr if p.endswith("/" + tok)]
    return c[0] if len(c) == 1 else None

def k11(lines, start, repo, stats=None):
    out = []
    for s in ledger_sections(lines):
        if s["kind"] not in ("MEASURED", "READ"):
            continue
        body = " ".join(t for _, t in s["body"])
        line_of = start + s["line"]
        cuts = [m.start() for m in ITEMLABEL.finditer(body)] or [0]
        if cuts[0] != 0:
            cuts = [0] + cuts
        items = [body[a:b] for a, b in zip(cuts, cuts[1:] + [len(body)])]
        hdr_file = FILETOK.search(s["header"])
        for it in items:
            quotes = [q for m in QUOTE.finditer(it) for q in [m.group(1) or m.group(2)] if len(q.split()) >= 3]
            if not quotes:
                continue
            f = FILETOK.search(it) or hdr_file
            path = _resolve(repo, f.group(1)) if f else None
            text = repo.show(path) if path else None
            if text is None:
                if stats is not None:
                    stats["K11_unchecked_quotes"] = stats.get("K11_unchecked_quotes", 0) + len(quotes)
                continue
            nt = ws(text)
            if stats is not None:
                stats["K11_checked_quotes"] = stats.get("K11_checked_quotes", 0) + len(quotes)
            for q in quotes:
                pieces = [ws(p) for p in re.split(r"…|\.\.\.", q) if ws(p)]
                if any(p not in nt for p in pieces):
                    out.append((line_of, f"{path}: \"{q}\""))
    return out


# K12 — R731(b): "the brief named the 12y-U18C gate directory from unified plan v96's summary ("gate/")
#   instead of having CC list it; the record's "gate_round1/" governed (R18)."
# Narrower reading: a token starting engine/, scripts/, tests/, docs/, analysis/ or review_specs/ (or the
# same under ~/projects/evidence-engine/ or /home/ankitsarin/projects/evidence-engine/), cut at the first
# character outside [\w./@+-] and stripped of trailing ".", ",", ";"; skipped when the next character is
# "<", "{", "*" or "…" (a placeholder, glob or elision). A hit when it is absent from the tree at the
# commit, unless the token or one of its parent directories appears on a line holding the word "new".
PATHRE = re.compile(r"(?:(?:~|/home/ankitsarin)/projects/evidence-engine/|(?<![\w./~$-]))"
                    r"((?:engine|scripts|tests|docs|analysis|review_specs)/[\w./@+-]*)")

def k12(lines, start, repo, alltext):
    newlines = [l for l in alltext.split("\n") if re.search(r"\bnew\b", l, re.I)]
    out = []
    for i, l in enumerate(lines):
        for m in PATHRE.finditer(l):
            nxt = l[m.end():m.end() + 1]
            if nxt in ("<", "{", "*", "…"):
                continue
            p = m.group(1).rstrip(".,;")
            if not p or repo.exists(p):
                continue
            parts = p.rstrip("/").split("/")
            prefixes = ["/".join(parts[:k]) for k in range(1, len(parts) + 1)]
            if any(re.search(rf"(?<![\w./-]){re.escape(x)}(?:/|(?![\w.-]))", nl) for x in prefixes[1:] for nl in newlines):
                continue
            out.append((start + i, p))
    return out


# K13 — R784(g)(i): "A clause STANDING_RULES.md holds is cited by its id and not restated; a brief restates
#   a convention only when the file lacks it."
# Reading: words are whitespace-separated tokens, lowercased, with surrounding quotes, brackets and
# punctuation removed; a hit is a maximal run of twelve or more consecutive brief words equal to twelve
# consecutive words of one clause (the text inside a ```text fence) of STANDING_RULES.md at the commit.
STRIP = "\"'“”‘’()[]{}`,.;:*"

def words_with_lines(lines, start):
    w = []
    for i, l in enumerate(lines):
        for t in l.split():
            t2 = t.strip(STRIP).lower()
            if t2:
                w.append((t2, start + i, t))
    return w

def rule_ngrams(repo):
    key = ("ngrams",)
    if key not in repo._cache:
        txt = repo.show("docs/plan/STANDING_RULES.md") or ""
        grams, cid, inf, buf = {}, None, False, []
        for l in txt.split("\n"):
            if l.startswith("## R"):
                cid = l[3:].strip()
            elif l.strip() == "```text":
                inf, buf = True, []
            elif l.strip() == "```" and inf:
                inf = False
                ws_ = [t.strip(STRIP).lower() for t in " ".join(buf).split()]
                ws_ = [t for t in ws_ if t]
                for k in range(len(ws_) - 11):
                    grams.setdefault(tuple(ws_[k:k + 12]), cid)
            elif inf:
                buf.append(l)
        repo._cache[key] = grams
    return repo._cache[key]

def k13(lines, start, repo):
    grams = rule_ngrams(repo)
    w = words_with_lines(lines, start)
    out, k = [], 0
    while k + 12 <= len(w):
        g = tuple(x[0] for x in w[k:k + 12])
        if g in grams:
            e = k + 12
            while e < len(w) and tuple(x[0] for x in w[e - 11:e + 1]) in grams:
                e += 1
            out.append((w[k][1], f"{grams[g]}: " + " ".join(x[2] for x in w[k:e])))
            k = e
        else:
            k += 1
    return out


# K14 — the architect's own addition (no clause): a cited rule id absent from the plan.
# Narrower reading: an id R<n> or R<n>(<letter>) (a trailing roman sub-clause is dropped); present when
# the plan at the commit holds that id as a token anywhere.
RID = re.compile(r"(?<![\w])R(\d{1,4})(\([a-z]\))?")

def k14(lines, start, repo):
    key = ("plan",)
    if key not in repo._cache:
        plan = repo.show("docs/plan/ENGINE_REFACTOR_PLAN.md") or ""
        repo._cache[key] = set(m.group(0) for m in RID.finditer(plan))
    have = repo._cache[key]
    return [(start + i, m.group(0)) for i, l in enumerate(lines) for m in RID.finditer(l) if m.group(0) not in have]

# --------------------------------------------------------------------------------------------- driver

TEXT_CHECKS = [("K1", k1), ("K3", k3), ("K4", k4), ("K5", k5), ("K6", k6), ("K7", k7), ("K8", k8), ("K9", k9), ("K10", k10)]
REPO_CHECKS = ["K2", "K11", "K12", "K13", "K14"]
ALL_CHECKS = ["K1", "K2", "K3", "K4", "K5", "K6", "K7", "K8", "K9", "K10", "K11", "K12", "K13", "K14"]

def run_checks(text, repo=None, stats=None):
    """(hits, regions info). Each hit: {check, region, line, text}."""
    regions, ntitles = split_regions(text)
    hits = []
    for name, start, lines in regions:
        found = [(c, f(lines, start)) for c, f in TEXT_CHECKS]
        if repo is not None:
            found += [("K2", k2(lines, start, repo)), ("K11", k11(lines, start, repo, stats)),
                      ("K12", k12(lines, start, repo, text)), ("K13", k13(lines, start, repo)),
                      ("K14", k14(lines, start, repo))]
        for c, hs in found:
            for ln, t in hs:
                hits.append({"check": c, "region": name, "line": ln, "text": clip(t)})
    hits.sort(key=lambda h: (ALL_CHECKS.index(h["check"]), h["line"]))
    return hits, {"split": regions[0][0] != "UNSPLIT", "task_lines": ntitles}


def cmd_check(a):
    text = open(a.brief, encoding="utf-8").read()
    repo = Repo(a.repo, a.commit, a.also_repo) if a.repo and a.commit else None
    hits, info = run_checks(text, repo)
    if not info["split"]:
        print("REGION\tUNSPLIT")
    for h in hits:
        print(f"{h['check']}\t{h['region']}\t{h['line']}\t{h['text']}")
    return 1 if hits else 0


def cmd_controls(a):
    repo = Repo(a.repo, a.commit, a.also_repo)
    bad = 0
    for f in sorted(os.listdir(a.dir)):
        m = re.fullmatch(r"(K\d+|REGION)_(hit|miss)\.txt", f)
        if not m:
            continue
        text = open(os.path.join(a.dir, f), encoding="utf-8").read()
        if m.group(1) == "REGION":
            regions, n = split_regions(text)
            got = regions[0][0] != "UNSPLIT"
            want = m.group(2) == "hit"
            ok = got == want
            print(f"{'PASS' if ok else 'FAIL'}  {f}: {'split at line ' + str(regions[1][1]) if got else 'unsplit'}"
                  f" (expected {'split' if want else 'unsplit'})")
        else:
            hits, _ = run_checks(text, repo)
            mine = [h for h in hits if h["check"] == m.group(1)]
            ok = bool(mine) == (m.group(2) == "hit")
            shown = "; ".join(f"{h['region']}:{h['line']} {h['text'][:90]}" for h in mine) or "no hit"
            print(f"{'PASS' if ok else 'FAIL'}  {f}: {m.group(1)} {shown}")
        bad += not ok
    return 1 if bad else 0

# --------------------------------------------------------------------------------------------- measurement

ARCHIVE = os.path.expanduser("~/claude-session-archive")
LIVE = os.path.expanduser("~/.claude/projects/-home-ankitsarin-projects-evidence-engine")
S1_FILE = os.path.join(ARCHIVE, "dc9f61b7-39e2-4cc6-ae86-dd391b660667.jsonl")
S1_LINES = [766, 862, 909, 1039, 1517]
SCAN = re.compile(r"TASK \[([^\]]*)\]\s*[—-]")   # WR5-A's scan pattern, M3
PASTE = re.compile(r'<pasted_content id="[^"]*">\n?(.*?)\n?</pasted_content id="[^"]*">', re.S)

def turn_text(d):
    c = d.get("message", {}).get("content")
    if isinstance(c, list):
        c = "\n".join(x.get("text", "") for x in c if isinstance(x, dict) and x.get("type") == "text")
    return c if isinstance(c, str) else None

def brief_text(c):
    """The paste holding the TASK title, in the brief-file form (inner text + newline); else the whole turn."""
    for m in PASTE.finditer(c):
        if SCAN.search(m.group(1)):
            return m.group(1) + "\n", "paste"
    return c, "whole_turn"

def archived_turns():
    """Every user turn of an evidence-engine transcript, in file order: (file, line, ts, text)."""
    files = sorted([os.path.join(ARCHIVE, f) for f in os.listdir(ARCHIVE) if f.endswith(".jsonl")]
                   + [os.path.join(LIVE, f) for f in os.listdir(LIVE) if f.endswith(".jsonl")])
    for f in files:
        cwd, turns = None, []
        for n, line in enumerate(open(f, encoding="utf-8", errors="replace"), 1):
            try:
                d = json.loads(line)
            except Exception:
                continue
            if cwd is None and d.get("cwd"):
                cwd = d["cwd"]
            if d.get("type") != "user" or d.get("isMeta"):
                continue
            c = turn_text(d)
            if c is not None:
                turns.append((f, n, d.get("timestamp", ""), c))
        if cwd and cwd.rstrip("/").endswith("projects/evidence-engine"):
            yield f, turns


def ruling_titles(plan):
    """{ruling id: title text} from the plan's "RULING [evidence-engine <id>] — …" lines through the line before STATUS:."""
    out, lines = {}, plan.split("\n")
    for i, l in enumerate(lines):
        m = re.match(r"RULING \[evidence-engine ([^\]]+)\] — (.*)", l)
        if m and m.group(1) not in out:
            t = [m.group(2)]
            for l2 in lines[i + 1:i + 8]:
                if l2.startswith("STATUS:"):
                    break
                t.append(l2.strip())
            out[m.group(1)] = " ".join(t)
    return out


def tally(records, checks):
    """records: [(split?, hits)]. Per check: briefs hit PRE-only / BRIEF-only / both / any, hits PRE / BRIEF / total,
    unsplit briefs hit and hits."""
    t = {c: dict(pre_only=0, brief_only=0, both=0, any=0, hits_pre=0, hits_brief=0, hits=0, unsplit_briefs=0, unsplit_hits=0)
         for c in checks}
    for split, hits in records:
        for c in checks:
            mine = [h for h in hits if h["check"] == c]
            p = sum(h["region"] == "PRE" for h in mine); b = sum(h["region"] == "BRIEF" for h in mine)
            u = sum(h["region"] == "UNSPLIT" for h in mine)
            r = t[c]
            if split:
                r["hits_pre"] += p; r["hits_brief"] += b; r["hits"] += p + b
                if p and b: r["both"] += 1
                elif p: r["pre_only"] += 1
                elif b: r["brief_only"] += 1
                if p or b: r["any"] += 1
            else:
                r["unsplit_hits"] += u
                if u: r["unsplit_briefs"] += 1
    return t


def cmd_measure(a):
    out, summary = open(a.out, "w", encoding="utf-8"), {}
    textonly = [c for c, _ in TEXT_CHECKS]

    def emit(set_name, brief, hits):
        for h in hits:
            out.write(json.dumps({"set": set_name, "brief": brief, **h}, ensure_ascii=False) + "\n")

    # S1 — the five stop-brief turns, text-only
    lines = open(S1_FILE, encoding="utf-8").read().split("\n")
    recs, per = [], {}
    for n in S1_LINES:
        d = json.loads(lines[n - 1]); text, src = brief_text(turn_text(d))
        hits, info = run_checks(text)
        title = SCAN.search(text).group(1)
        name = f"{S1_FILE}:{n} [{title}]"
        emit("S1", name, hits); recs.append((info["split"], hits))
        per[name] = {"source": src, "split": info["split"], "task_lines": info["task_lines"],
                     "BRIEF": sorted({h["check"] for h in hits if h["region"] == "BRIEF"}, key=ALL_CHECKS.index),
                     "PRE": sorted({h["check"] for h in hits if h["region"] == "PRE"}, key=ALL_CHECKS.index)}
    summary["S1"] = {"total": len(recs), "split": sum(r[0] for r in recs), "multi_title": sum(v["task_lines"] > 1 for v in per.values()),
                     "table": tally(recs, textonly), "per_brief": per}

    # S2 / S4 — committed brief files, all checks at the commit that added each
    files = subprocess.run(["git", "-C", a.repo, "ls-files"], capture_output=True, text=True).stdout.split("\n")
    files = [f for f in files if re.search(r"(^|/)brief[^/]*\.(md|txt)$", f)]
    for set_name, sel in (("S2", lambda f: not os.path.basename(f).startswith("brief_WR5")),
                          ("S4", lambda f: os.path.basename(f).startswith("brief_WR5"))):
        recs, per, stats = [], {}, {}
        for f in sorted(filter(sel, files)):
            added = subprocess.run(["git", "-C", a.repo, "log", "--diff-filter=A", "--format=%H", "--", f],
                                   capture_output=True, text=True).stdout.split()[-1]
            repo = Repo(a.repo, added, a.also_repo)
            text = repo.show(f)
            hits, info = run_checks(text, repo, stats)
            emit(set_name, f"{f}@{added}", hits); recs.append((info["split"], hits))
            per[f] = {"added_in": added, "split": info["split"], "task_lines": info["task_lines"],
                      "hits": len(hits)}
        summary[set_name] = {"total": len(recs), "split": sum(r[0] for r in recs),
                             "multi_title": sum(v["task_lines"] > 1 for v in per.values()),
                             "table": tally(recs, ALL_CHECKS), "per_brief": per, "k11_stats": stats}

    # S3 — every archived brief turn (M3's predicate), WR5 titles excluded, text-only; counts only
    plan = subprocess.run(["git", "-C", a.repo, "show", "HEAD:docs/plan/ENGINE_REFACTOR_PLAN.md"],
                          capture_output=True, text=True).stdout
    titles = ruling_titles(plan)
    recs_all, by_month, by_word = [], defaultdict(list), defaultdict(list)
    counts = dict(turns=0, excluded_wr5=0, transcripts=0, whole_turn=0, multi_title=0, unmatched=0,
                  unmatched_no_next_ruling=0, unmatched_not_in_plan=0)
    for f, turns in archived_turns():
        counts["transcripts"] += 1
        for k, (_, n, ts, c) in enumerate(turns):
            m = SCAN.search(c)
            if not m:
                continue
            if any("WR5" in t for t in SCAN.findall(c)):
                counts["excluded_wr5"] += 1
                continue
            counts["turns"] += 1
            text, src = brief_text(c)
            counts["whole_turn"] += src == "whole_turn"
            hits, info = run_checks(text)
            counts["multi_title"] += info["task_lines"] > 1
            rec = (info["split"], hits)
            recs_all.append(rec); by_month[ts[:7] or "unknown"].append(rec)
            own = set(re.findall(r"RULING \[evidence-engine ([^\]]+)\]", c))
            nxt = None
            for _, _, _, c2 in turns[k + 1:]:
                ids = [i for i in re.findall(r"RULING \[evidence-engine ([^\]]+)\]", c2) if i not in own]
                if ids:
                    nxt = ids[0]; break
            if nxt is None:
                counts["unmatched"] += 1; counts["unmatched_no_next_ruling"] += 1; by_word["unmatched"].append(rec); continue
            if nxt not in titles:
                counts["unmatched"] += 1; counts["unmatched_not_in_plan"] += 1; by_word["unmatched"].append(rec); continue
            t = titles[nxt].lower()
            st, ac = bool(re.search(r"\bstop\b", t)), bool(re.search(r"\baccepted\b", t))
            by_word["stop+accepted" if st and ac else "stop" if st else "accepted" if ac else "neither"].append(rec)
    summary["S3"] = {"counts": counts, "split": sum(r[0] for r in recs_all), "table": tally(recs_all, textonly),
                     "by_month": {k: {"total": len(v), "split": sum(r[0] for r in v), "table": tally(v, textonly)} for k, v in sorted(by_month.items())},
                     "by_word": {k: {"total": len(v), "split": sum(r[0] for r in v), "table": tally(v, textonly)} for k, v in sorted(by_word.items())}}
    out.close()
    json.dump(summary, open(a.summary, "w", encoding="utf-8"), indent=1, ensure_ascii=False)
    print(f"wrote {a.out} and {a.summary}")
    return 0


def main():
    ap = argparse.ArgumentParser()
    sp = ap.add_subparsers(dest="cmd", required=True)
    c = sp.add_parser("check"); c.add_argument("brief"); c.add_argument("--repo"); c.add_argument("--commit")
    c.add_argument("--also-repo", action="append", default=[])
    k = sp.add_parser("controls"); k.add_argument("dir"); k.add_argument("--repo", required=True)
    k.add_argument("--commit", required=True); k.add_argument("--also-repo", action="append", default=[])
    m = sp.add_parser("measure"); m.add_argument("--out", required=True); m.add_argument("--summary", required=True)
    m.add_argument("--repo", required=True); m.add_argument("--also-repo", action="append", default=[])
    a = ap.parse_args()
    return {"check": cmd_check, "controls": cmd_controls, "measure": cmd_measure}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
