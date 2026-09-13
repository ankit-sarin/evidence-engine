"""SCREEN-AUTH-01 Phase 2f smoke — the shared core. Standard library only.

Three arms screen the same 86 papers at the abstract stage:

  A  pre-fold engine and spec (83defc5, run from a git worktree)
  B  HEAD engine, live spec
  C  HEAD engine, a spec variant in which the four exclusions absent from
     abstract_primary are added to it — nothing else differs

Why this module imports nothing from `engine` or `analysis`: the worker runs one
arm inside ONE engine tree (arm A's tree is a worktree of 83defc5), and imports
this file by path. Anything here that pulled in a package would pull it from the
wrong tree. The worker binds the engine; this module holds the rules.

The rules reproduced here are the pipeline's own (ruling R2), read from
`engine/agents/screener.py` `run_screening` / `run_verification`:

  * primary: two calls, pass_number 1 then 2, same model, same request.
    include+include -> IN, exclude+exclude -> OUT, anything else -> FLAGGED.
    A malformed response to either call -> FLAGGED; a malformed pass 1 means
    pass 2 is never called (both calls sit in one try block in the runner).
  * verifier: runs on primary IN only. include -> IN, otherwise FLAGGED.
    A malformed response -> FLAGGED.

Anything else a call raises (a timeout after the client's own retries) is
recorded as ERROR and never retried by the harness.
"""

from __future__ import annotations

import difflib
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

PRIMARY = "primary"
VERIFIER = "verifier"
IN, OUT, FLAGGED, ERROR = "IN", "OUT", "FLAGGED", "ERROR"

ARMS = ("A", "B", "C")

#: The criteria arm C adds to abstract_primary: the four exclusions the live
#: spec leaves off that stage (2f Part 1a, I7).
ARM_C_ADDED_CRITERIA = (
    "exc-abstract-only",
    "exc-teleoperation-only",
    "exc-analysis-only",
    "exc-no-autonomy",
)
ARM_C_STAGE = "abstract_primary"

#: Commit arm A runs from.
ARM_A_COMMIT = "83defc5"

#: Placeholder-paper request hashes: SHA-256 of {format, messages}, key-sorted,
#: ensure_ascii=False — the R1–R4 definition in tests/test_eligibility.py.
#:   A: that file's T7 comment, "83defc5 under R31", measured again in 2f Part 1a.
#:   B: FROZEN R1 / R2.
#: Arm C's verifier request must equal B's (abstract_verifier stage lists are
#: untouched); its primary request is checked by render-diff instead.
EXPECTED_PLACEHOLDER_HASHES = {
    "A": {
        PRIMARY: "33bdeea91007fb81f2401566f01bc05b6a3c0315db003f41c6dcd4c8808abee3",
        VERIFIER: "2c89ef61c690d0c544408f34ee6ea80702520f8cd5a9b1b61da93b6223c4d5f8",
    },
    "B": {
        PRIMARY: "e02ce2c9a77ab578415bb6ca32477a952bd2727de19558d80fd19fd5ddf84649",
        VERIFIER: "bc36e29191d4a61ce1049634d39adabcd58e52fdd177d1665453ac370f071ff3",
    },
}

#: The placeholder paper the R1–R4 pins are computed over.
PLACEHOLDER_PAPER = {"title": "T", "abstract": "A", "id": 1}

#: No call starts inside this UTC window (ruling R5); the lead keeps a paper's
#: calls from running into it.
QUIET_START = (6, 30)
QUIET_END = (9, 30)
DEFAULT_LEAD_MINUTES = 10

#: Test hook: a fixed ISO-8601 UTC clock, so window logic is testable at any hour.
NOW_ENV = "SCREEN2F_NOW"


# ── request identity ──────────────────────────────────────────────────


def request_hash(fmt, messages) -> str:
    """The R1–R4 request hash: {format, messages}, key-sorted, ensure_ascii=False."""
    blob = json.dumps({"format": fmt, "messages": messages}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def exclusion_block_span(user_prompt: str) -> tuple[int, int]:
    """Line indices [start, end) of the exclusion bullets in a primary user prompt."""
    lines = user_prompt.split("\n")
    start = lines.index("EXCLUSION CRITERIA:") + 1
    end = start
    while end < len(lines) and lines[end].startswith("  - "):
        end += 1
    return start, end


def check_identity(identities: dict[str, dict]) -> dict:
    """Gate 1 (ruling R4) over the per-arm placeholder captures.

    `identities[arm][role]` carries `hash`, `messages`, `format` and `call_kwargs`
    (every kwarg the engine passed besides messages and format).
    Returns {"ok": bool, "checks": [...]}; never raises on a failed check.
    """
    checks: list[dict] = []

    def check(name: str, ok: bool, detail=None):
        checks.append({"check": name, "ok": bool(ok), "detail": detail})

    for arm in ("A", "B"):
        for role in (PRIMARY, VERIFIER):
            got = identities[arm][role]["hash"]
            want = EXPECTED_PLACEHOLDER_HASHES[arm][role]
            check(f"{arm}.{role} placeholder hash", got == want, {"got": got, "want": want})

    check("C.verifier == B.verifier", identities["C"][VERIFIER]["hash"] == identities["B"][VERIFIER]["hash"],
          {"C": identities["C"][VERIFIER]["hash"], "B": identities["B"][VERIFIER]["hash"]})

    b, c = identities["B"][PRIMARY], identities["C"][PRIMARY]
    check("C.primary format == B.primary format", b["format"] == c["format"])
    check("C.primary system == B.primary system", b["messages"][0] == c["messages"][0])
    b_user, c_user = b["messages"][1]["content"], c["messages"][1]["content"]
    bs, be = exclusion_block_span(b_user)
    cs, ce = exclusion_block_span(c_user)
    bl, cl = b_user.split("\n"), c_user.split("\n")
    outside_equal = bl[:bs] == cl[:cs] and bl[be:] == cl[ce:]
    added = [ln for ln in difflib.ndiff(bl[bs:be], cl[cs:ce]) if ln.startswith(("+ ", "- "))]
    only_additions = all(ln.startswith("+ ") for ln in added)
    check("C.primary differs from B only inside the exclusion block", outside_equal,
          {"B_block_lines": be - bs, "C_block_lines": ce - cs})
    check("C.primary exclusion block only gains lines", only_additions and len(added) == len(ARM_C_ADDED_CRITERIA),
          {"diff": added})

    for role in (PRIMARY, VERIFIER):
        kw = {arm: identities[arm][role]["call_kwargs"] for arm in ARMS}
        check(f"{role} call options identical across arms", kw["A"] == kw["B"] == kw["C"], kw)

    return {"ok": all(c["ok"] for c in checks), "checks": checks}


# ── arm C spec variant ────────────────────────────────────────────────


def make_arm_c_spec_text(live_spec_text: str) -> str:
    """The live spec with ARM_C_STAGE added to each ARM_C_ADDED_CRITERIA stage list.

    A textual insertion, so every other byte of the file is untouched and the
    diff is exactly four lines. Refuses if a criterion is missing or already at
    the stage — the variant must mean the same thing every time it is built.
    """
    out = live_spec_text
    for cid in ARM_C_ADDED_CRITERIA:
        block = re.compile(
            r"  - id: %s\n(?P<body>(?:    .*\n|      .*\n)*?)    stages:\n(?P<stages>(?:    - .*\n)*)" % re.escape(cid)
        )
        m = block.search(out)
        if m is None:
            raise ValueError(f"criterion {cid!r} with a stages list not found in the live spec")
        if f"    - {ARM_C_STAGE}\n" in m.group("stages"):
            raise ValueError(f"criterion {cid!r} is already at {ARM_C_STAGE}; the variant would be a no-op")
        insert_at = m.start("stages")
        out = out[:insert_at] + f"    - {ARM_C_STAGE}\n" + out[insert_at:]
    return out


@dataclass(frozen=True)
class Arm:
    name: str
    root: Path
    spec: Path


def arms(repo_root: Path, arm_a_root: Path, spec_rel: Path, arm_c_spec: Path) -> dict[str, Arm]:
    """The three arms: which tree each imports from and which spec it loads.

    `spec_rel` is the review's spec path relative to a tree root, as the
    resolver returns it; arms A and B each root it in their own tree.
    """
    repo_root, arm_a_root = Path(repo_root).resolve(), Path(arm_a_root).resolve()
    return {
        "A": Arm("A", arm_a_root, arm_a_root / spec_rel),
        "B": Arm("B", repo_root, repo_root / spec_rel),
        "C": Arm("C", repo_root, Path(arm_c_spec).resolve()),
    }


# ── combination rules ─────────────────────────────────────────────────


def combine_primary(d1: str, d2: str) -> str:
    """The runner's agreement rule for the two primary passes."""
    if d1 == "include" and d2 == "include":
        return IN
    if d1 == "exclude" and d2 == "exclude":
        return OUT
    return FLAGGED


def verifier_outcome(decision: str) -> str:
    """The runner's verification rule: agreement keeps IN, anything else flags."""
    return IN if decision == "include" else FLAGGED


def final_outcome(primary: str, verifier: str | None) -> str | None:
    """Final outcome from the primary outcome and, for primary IN, the verifier's.

    None means incomplete: primary IN with no verifier result yet.
    """
    if primary != IN:
        return primary
    return verifier


def verifier_targets(papers: list[dict], primary_results: dict[int, dict], forced_ids=()) -> list[tuple[dict, bool]]:
    """Papers the verifier phase calls, in paper order, each with its `forced` flag.

    Primary IN papers are the pipeline's pattern. A forced id (pre-flight only)
    that is not primary IN is called too, flagged forced, and never feeds a final
    outcome. Refuses if any paper lacks a primary result.
    """
    forced = set(forced_ids)
    missing = [p["id"] for p in papers if p["id"] not in primary_results]
    if missing:
        raise ValueError(f"primary phase incomplete for paper ids {missing}")
    out = []
    for p in papers:
        is_in = primary_results[p["id"]]["outcome"] == IN
        if is_in or p["id"] in forced:
            out.append((p, not is_in))
    return out


# ── time ──────────────────────────────────────────────────────────────


def now_utc() -> datetime:
    fixed = os.environ.get(NOW_ENV)
    if fixed:
        return datetime.fromisoformat(fixed).astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def utc_iso(dt: datetime | None = None) -> str:
    return (dt or now_utc()).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def in_quiet_window(now: datetime, lead_minutes: int = DEFAULT_LEAD_MINUTES) -> bool:
    """True if a paper must not START now: [06:30 - lead, 09:30) UTC."""
    now = now.astimezone(timezone.utc)
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start = day + timedelta(hours=QUIET_START[0], minutes=QUIET_START[1] - lead_minutes)
    end = day + timedelta(hours=QUIET_END[0], minutes=QUIET_END[1])
    return start <= now < end


def quiet_window_end(now: datetime) -> datetime:
    now = now.astimezone(timezone.utc)
    return now.replace(hour=QUIET_END[0], minute=QUIET_END[1], second=0, microsecond=0)


# ── checkpoint ────────────────────────────────────────────────────────


class Checkpoint:
    """Append-only JSONL per arm per phase: a claim line, then a result line.

    A claim with no result is a paper a previous process started and did not
    finish; it is processed again and the result says so.
    """

    def __init__(self, path: Path):
        self.path = Path(path)

    def load(self) -> tuple[dict[int, dict], set[int]]:
        results: dict[int, dict] = {}
        claims: set[int] = set()
        if not self.path.exists():
            return results, claims
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                rec = json.loads(line)
                if rec["type"] == "claim":
                    claims.add(rec["paper_id"])
                elif rec["type"] == "result":
                    if rec["paper_id"] in results:
                        raise ValueError(f"{self.path}: paper {rec['paper_id']} has two results")
                    results[rec["paper_id"]] = rec
        return results, claims - set(results)

    def _append(self, rec: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False, sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def claim(self, paper_id: int) -> None:
        self._append({"type": "claim", "paper_id": paper_id, "at": utc_iso()})

    def result(self, rec: dict) -> None:
        self._append({"type": "result", **rec})


# ── phase loops ───────────────────────────────────────────────────────

#: screen(paper, pass_number, role) -> {"decision", "rationale", "confidence",
#: "parse_error", "error", "call"}. The worker's binding never raises.
ScreenFn = Callable[[dict, int, str], dict]


def _failed(o: dict) -> bool:
    return bool(o.get("parse_error") or o.get("error"))


def _fail_outcome(o: dict) -> str:
    return ERROR if o.get("error") else FLAGGED


def run_primary(papers: list[dict], ckpt: Checkpoint, screen: ScreenFn, *,
                blocked: Callable[[], bool], log: Callable[[str], None], meta: dict) -> str:
    """Primary phase over `papers`. Returns "done" or "paused"."""
    done, interrupted = ckpt.load()
    for p in papers:
        pid = p["id"]
        if pid in done:
            continue
        if blocked():
            log(f"PAUSE before paper {pid}: quiet window")
            return "paused"
        started = utc_iso()
        ckpt.claim(pid)
        o1 = screen(p, 1, PRIMARY)
        calls = [o1]
        if _failed(o1):
            outcome, d2 = _fail_outcome(o1), None
        else:
            o2 = screen(p, 2, PRIMARY)
            calls.append(o2)
            outcome = _fail_outcome(o2) if _failed(o2) else combine_primary(o1["decision"], o2["decision"])
            d2 = o2.get("decision")
        d1 = o1.get("decision")
        rec = {
            **meta, "phase": PRIMARY, "paper_id": pid, "outcome": outcome,
            "d1": d1, "d2": d2, "d1_ne_d2": d1 is not None and d2 is not None and d1 != d2,
            "parse_error": any(c.get("parse_error") for c in calls),
            "error": any(c.get("error") for c in calls),
            "calls": calls, "started_utc": started, "ended_utc": utc_iso(),
            "reprocessed_after_interrupt": pid in interrupted,
        }
        ckpt.result(rec)
        done[pid] = rec
        log(f"primary paper {pid}: d1={d1} d2={d2} -> {outcome}")
    return "done"


def run_verifier(targets: list[tuple[dict, bool]], ckpt: Checkpoint, screen: ScreenFn, *,
                 blocked: Callable[[], bool], log: Callable[[str], None], meta: dict) -> str:
    """Verifier phase over `targets` from `verifier_targets`. Returns "done" or "paused"."""
    done, interrupted = ckpt.load()
    for p, forced in targets:
        pid = p["id"]
        if pid in done:
            continue
        if blocked():
            log(f"PAUSE before paper {pid}: quiet window")
            return "paused"
        started = utc_iso()
        ckpt.claim(pid)
        o = screen(p, 1, VERIFIER)
        outcome = _fail_outcome(o) if _failed(o) else verifier_outcome(o["decision"])
        rec = {
            **meta, "phase": VERIFIER, "paper_id": pid, "outcome": outcome,
            "decision": o.get("decision"), "forced": forced,
            "parse_error": bool(o.get("parse_error")), "error": bool(o.get("error")),
            "calls": [o], "started_utc": started, "ended_utc": utc_iso(),
            "reprocessed_after_interrupt": pid in interrupted,
        }
        ckpt.result(rec)
        done[pid] = rec
        log(f"verifier paper {pid}{' (forced)' if forced else ''}: {o.get('decision')} -> {outcome}")
    return "done"


def load_papers(path: Path, ids: list[int] | None = None) -> list[dict]:
    """The exported papers, in file order, optionally restricted to `ids` (in that order)."""
    with open(path, encoding="utf-8") as f:
        papers = [json.loads(line) for line in f if line.strip()]
    if ids is None:
        return papers
    by_id = {p["id"]: p for p in papers}
    unknown = [i for i in ids if i not in by_id]
    if unknown:
        raise ValueError(f"paper ids not in {path}: {unknown}")
    return [by_id[i] for i in ids]


def sha256_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()
