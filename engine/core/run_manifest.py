"""Run open, arm pinning and call recording (S3a, S3b, S3g, R59, R66–R68).

MANIFEST-01 Phase 2a. A run writes its manifest — `run_manifests` plus one
`run_stage_configs` row per stage — **before its first model call**, in one
transaction with the pins of every arm it names. Nothing here makes a model
call: the only network use is `fetch_model_digest` (`GET /api/tags`, which loads
no model), injectable as `digest_fn`.

**Refusals, all before anything is written:**

* `DirtyTree` — the working tree has uncommitted changes. An untagged HEAD is
  allowed.
* `AmbiguousEngineState` — two or more engine-state tags point at HEAD (E-STATE).
* `PreManifestArm` — a named arm was registered pre-manifest (R59).
* `RetiredArm` — a named arm is retired (R21).
* `ArmPinMismatch` — a named arm is pinned and this run resolves it differently
  (R10); the differing keys are named.
* `CloudArmNotEnabled` — a cloud arm is requested that `cloud.enabled_arms`
  does not enable (S3g).

`engine_state` is the engine-state name tagged at HEAD, NULL when none is
(E-STATE). A state is tagged with its bare name as an annotated tag — e.g.
`freshman` — from `ENGINE_STATES`; any other tag at HEAD is ignored here (it
still reaches `git_tag` through `git describe`). The value is part of the
manifest body, so `manifest_sha256` covers it: the hash covers the recorded body.

**At extraction time** (R116/R117, WRITE-PATH-01 9b-2b), `extraction_digest`
refuses before anything is selected: `StageNotInRun` when the run did not declare
the extraction stage (or does not exist), and `ArmPinMismatch` — the same refusal
`open_run` raises — when a stage's digest disagrees with the arm's pin, naming
the stage and both digests.

An arm that is unregistered, or registered but unpinned, is **pinned at this
manifest**: its resolved configuration tuple goes into `arms.configuration_json`
with `configuration_marker = 'pinned'`, `pinned_run_id` and `pinned_sha256`.

**Call recording.** `active_run(...)` sets the run for the calls made inside it;
`ollama_chat` records one `run_calls` row per Ollama call (the hash of the
kwargs handed to `_client.chat`, and a digest of the response). A cloud
extractor records its own through `record_call`.
"""

from __future__ import annotations

import contextlib
import contextvars
import importlib.metadata
import json
import logging
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, NoReturn

from engine.core.effective import PRE_MANIFEST
from engine.core.effective_config import (
    CLOUD_STAGE_PREFIX, OLLAMA_STAGES, ResolvedStage, canonical_json,
    resolve_run, sha256_canonical, stage_config,
)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

#: Re-declared in migration 022 (R214, R35); a test asserts the two agree.
RUN_KINDS = ("extraction", "screening", "judge", "review_session", "import")
#: The named engine states, in order (R300): each is tagged at the commit that
#: reaches it with its bare name as an annotated tag (E-STATE).
ENGINE_STATES = ("freshman", "sophomore", "junior", "senior")
#: R215/10a-C3: 'aborted' wired to RunAborted's close in scripts/run_pipeline.py.
END_STATUSES = ("completed", "failed", "interrupted", "aborted")
#: C40: the closed set of `end_reason` values an 'interrupted' close carries.
#: One per `run_pipeline` gate that stops a run (C40), one per interrupt (C47).
#: Kept beside END_STATUSES so engine/ callers can import it too.
REASON_BLOCKED_ADJUDICATION = "blocked:adjudication"
REASON_BLOCKED_AUDIT_REVIEW = "blocked:audit_review"
REASON_INTERRUPT_SIGINT = "interrupt:SIGINT"
REASON_INTERRUPT_SIGTERM = "interrupt:SIGTERM"
REASON_INTERRUPT_SIGHUP = "interrupt:SIGHUP"
INTERRUPTED_REASONS = (REASON_BLOCKED_ADJUDICATION, REASON_BLOCKED_AUDIT_REVIEW,
                       REASON_INTERRUPT_SIGINT, REASON_INTERRUPT_SIGTERM,
                       REASON_INTERRUPT_SIGHUP)
#: The signal each interrupt reason names. SIGINT arrives as Python's own
#: KeyboardInterrupt; SIGTERM and SIGHUP as RunInterrupted (`interrupt_signals`).
_INTERRUPT_REASON_BY_SIGNAL = {
    signal.SIGINT: REASON_INTERRUPT_SIGINT,
    signal.SIGTERM: REASON_INTERRUPT_SIGTERM,
    signal.SIGHUP: REASON_INTERRUPT_SIGHUP,
}
ARM_PINNED = "pinned"

#: R75: the libraries whose versions every manifest records — the three model
#: clients, and `pysbd`, the sentence segmenter that cuts a paper into the units
#: an elicited run cites (INT-i12-4).
LIBRARIES = ("ollama", "openai", "anthropic", "pysbd")

#: The stage whose arm cites numbered units; its pin records the segmentation.
_UNIT_CITING_STAGE = "elicitation_pass1"

#: Spec arm kind -> registry arm kind (016's CHECK spells the human kind in full).
_REGISTRY_KIND = {"model": "model", "human": "human_extractor"}

#: The extraction stages whose claims land on `extraction_models.arm`.
_LOCAL_EXTRACTION_STAGES = ("extract_pass1", "extract_pass2",
                            "extract_retry_snippet", "elicitation_pass1")


class RunRefused(Exception):
    """A run that may not open. Raised before anything is written."""


class RunInterrupted(BaseException):
    """SIGTERM or SIGHUP, raised by `interrupt_signals`' handler (C47).

    A BaseException, like KeyboardInterrupt, and deliberately not an Exception:
    every `except Exception` on the run path (the per-paper loop, `ollama_chat`'s
    retry, an entry point's 'failed' close) must let it through."""

    def __init__(self, signum: int):
        super().__init__(signal.Signals(signum).name)
        self.signum = signum


class DirtyTree(RunRefused):
    pass


class AmbiguousEngineState(RunRefused):
    """Two or more `ENGINE_STATES` tags point at HEAD (E-STATE)."""


class PreManifestArm(RunRefused):
    pass


class RetiredArm(RunRefused):
    pass


class ArmPinMismatch(RunRefused):
    pass


class CloudArmNotEnabled(RunRefused):
    pass


class CodebookReviewMismatch(RunRefused):
    """INT-i12-9: the codebook handed to `open_run` declares a review other than
    the spec's, or none. A run's prompts are built from that codebook; one that
    belongs to another review does not fail, it extracts another review's
    fields under this review's manifest."""


class ModelLineageMissing(RunRefused):
    """R703(d): a stage the run declares resolves to a model the spec's
    `model_lineage` has no entry for. Spec load checks the same coverage; this is
    the re-check for a spec derived by `model_copy`, which skips validation."""


class SameLineageVerifier(RunRefused):
    """R703(e), INT-i12-45: the audit model shares a base-model lineage with the
    model of an extraction stage it verifies — the settled cross-family rule."""


class StageNotInRun(RunRefused):
    """The run named does not exist or did not declare the stage a caller needs."""


class OllamaHostNotLoopback(RunRefused):
    """R721(c), U18: the Ollama host the engine resolved at import is not a loopback
    address. Every local-arm prompt would leave the machine through it, unrecorded;
    a declared remote host is a later capability, not this one."""


class OllamaHostNotLocalService(RunRefused):
    """U19, R734(d)(i): the Ollama host the engine resolved at import is loopback but not
    the local service (the library's default port). The environment check reads the
    local service's unit, so it would be checking a server the calls do not go to; no
    run kind supports another local server (R721(c), R724(d) extended)."""


class ModelDigestChanged(RunRefused):
    """U18 commit 2, R721(b): the model a call names has a different digest now than
    the one this run's manifest recorded for the call's stage — a model replaced under
    its tag mid-run, which every later event would otherwise attribute to the recorded
    digest. Raised by `ollama_chat` before the input-fit read and the send; the runners
    close the run 'aborted' with this message, which starts with the class name.

    Constructed from a message, as every `RunRefused` is; `at` composes the message
    from the call's stage key, model and the two digests, and keeps them as fields."""

    def __init__(self, message: str, *, stage_key: str | None = None, model: str | None = None,
                 recorded: str | None = None, current: str | None = None):
        super().__init__(message)
        self.stage_key, self.model = stage_key, model
        self.recorded, self.current = recorded, current

    @classmethod
    def at(cls, stage_key: str, model: str, recorded: str | None, current: str | None,
           detail: str | None = None) -> "ModelDigestChanged":
        now = current if current is not None else f"no readable digest ({detail})"
        return cls(
            f"ModelDigestChanged: run refused at stage {stage_key!r}: model {model!r} now has "
            f"digest {now}, and this run's manifest recorded {recorded}. Nothing was sent for "
            "this call. Read `ollama list` for the model's current digest and the Ollama "
            "journal for a pull or create during the run; a new run records the current "
            "digest, and if the arm is pinned a changed model is a new arm (R10).",
            stage_key=stage_key, model=model, recorded=recorded, current=current)


class ServerEnvChanged(RunRefused):
    """U19, R734(c)(ii): after a performed proactive restart, the Ollama service's
    configured environment fails the correctness tier — a drop-in edited during the run
    and applied by the run's own restart, under which every later paper would be
    extracted. Raised by the extract loop before the next paper; the runner closes the
    run 'aborted' with this message, which starts with the class name.

    Constructed from a message, as every `RunRefused` is; `at` composes the message from
    the drifted keys and keeps them as `drift` ({key: (expected, actual or None)})."""

    def __init__(self, message: str, *, drift: Mapping[str, tuple[str, str | None]] | None = None,
                 papers_done: int | None = None):
        super().__init__(message)
        self.drift = dict(drift or {})
        self.papers_done = papers_done

    @classmethod
    def at(cls, drift: Mapping[str, tuple[str, str | None]], *,
           papers_done: int | None = None) -> "ServerEnvChanged":
        keys = "; ".join(f"{k} expected {want!r}, actual "
                         f"{'<not set>' if got is None else repr(got)}"
                         for k, (want, got) in sorted(drift.items()))
        done = f" ({papers_done} papers done)" if papers_done is not None else ""
        return cls(
            f"ServerEnvChanged: run refused after a proactive Ollama restart{done}: the "
            f"restarted service's configured environment fails the correctness tier: {keys}. "
            "No paper was extracted under it. Read `systemctl show ollama "
            "--property=Environment` and the drop-in under "
            "/etc/systemd/system/ollama.service.d/ for the edit made during the run; restore "
            "the value, restart the service, and start a new run.",
            drift=drift, papers_done=papers_done)


class LoadedContextMismatch(RunRefused):
    """U20 commit 2, R744(c): after a completed call under a run, `/api/ps` reports a
    context for the call's model and digest that is not the ceiling the call was checked
    against, or lists no such entry, or cannot be read. Raised by `ollama_chat` before the
    post-call input-fit checks; the runners close the run 'aborted' with this message,
    which starts with the class name.

    Constructed from a message, as every `RunRefused` is; `at` composes the message from
    the model, the ceiling and what was read (the value, "absent" or the error), and keeps
    them as fields."""

    def __init__(self, message: str, *, model: str | None = None, ceiling: int | None = None,
                 read: int | str | None = None):
        super().__init__(message)
        self.model, self.ceiling, self.read = model, ceiling, read

    @classmethod
    def at(cls, model: str, ceiling: int, read: int | str | None) -> "LoadedContextMismatch":
        return cls(
            f"LoadedContextMismatch: run refused after a completed call: model {model!r} was "
            f"checked against a ceiling of {ceiling}, and /api/ps read {read} for its name and "
            "digest. Read `ollama ps`, the Ollama journal's last load line, `systemctl show "
            "ollama --property=Environment` and the model's Modelfile parameters; bring the "
            "server and the ceiling's terms into agreement and start a new run.",
            model=model, ceiling=ceiling, read=read)


#: R745(b): the refusals of a server that changed under an open run. Both runners catch this
#: tuple and close the run 'aborted' with the refusal's message.
MID_RUN_SERVER_REFUSALS = (ModelDigestChanged, ServerEnvChanged, LoadedContextMismatch)


class UndeclaredCall(Exception):
    """C54: a model call the active run's manifest did not declare — its stage
    key has no `run_stage_configs` row, or the row names a different model.
    Raised by `ollama_chat` before anything is sent; a run fault, never a paper
    outcome. Not a `RunRefused`: that is a run that may not open, and this run
    is open. `declared_model` is None when the stage key is undeclared."""

    def __init__(self, stage_key: str, declared_model: str | None, requested_model: str):
        self.stage_key = stage_key
        self.declared_model = declared_model
        self.requested_model = requested_model
        if declared_model is None:
            detail = f"this run declared no {stage_key!r} stage"
        else:
            detail = f"this run declared {stage_key!r} with model {declared_model!r}"
        super().__init__(
            f"call refused before sending: {detail}, and the call would send model "
            f"{requested_model!r}. The manifest is the contract — a run sends only the "
            "stages and models it declared at open.")


# ── Inputs a manifest records ────────────────────────────────────────
@dataclass(frozen=True)
class GitState:
    commit: str
    dirty: bool
    tag: str | None
    #: The `ENGINE_STATES` tags pointing at HEAD, in R300 order (E-STATE).
    state_tags: tuple[str, ...] = ()


def git_state(repo_root: Path = REPO_ROOT) -> GitState:
    def _git(*args) -> subprocess.CompletedProcess:
        return subprocess.run(["git", "-C", str(repo_root), *args],
                              capture_output=True, text=True)
    commit = _git("rev-parse", "HEAD").stdout.strip()
    dirty = bool(_git("status", "--porcelain").stdout.strip())
    tag = _git("describe", "--exact-match", "--tags", "HEAD")
    at_head = set(_git("tag", "--points-at", "HEAD").stdout.split())
    return GitState(commit=commit, dirty=dirty,
                    tag=tag.stdout.strip() if tag.returncode == 0 else None,
                    state_tags=tuple(s for s in ENGINE_STATES if s in at_head))


def library_versions() -> dict[str, str]:
    """Installed versions, never the requirement line (C18)."""
    out = {}
    for lib in LIBRARIES:
        try:
            out[lib] = importlib.metadata.version(lib)
        except importlib.metadata.PackageNotFoundError:
            out[lib] = "not installed"
    return out


def spec_hash(spec) -> str:
    """The WHOLE spec's hash (R60/C17) — not `screening_hash()`, which covers
    eligibility only and cannot see a PICO or title edit."""
    return sha256_canonical(spec.model_dump(mode="json"))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Arms ─────────────────────────────────────────────────────────────
def _stage_arm(spec, stage: str) -> str | None:
    if stage.startswith(CLOUD_STAGE_PREFIX):
        return stage[len(CLOUD_STAGE_PREFIX):]
    if stage in _LOCAL_EXTRACTION_STAGES:
        return spec.extraction_models.arm
    return None


def _declared_models(spec, stage_list: list[str],
                     preflight_models: Iterable[str]) -> dict[str, str]:
    """Stage key -> the model it resolves to, for every stage a run declares."""
    out: dict[str, str] = {}
    for s in stage_list:
        if s == "preflight":
            out.update({f"preflight:{m}": m for m in preflight_models})
        else:
            out[s] = stage_config(s, spec).model
    return out


def _lineage_check(conn, spec, models: Mapping[str, str]) -> dict | None:
    """R703(d)/(e), before any row: every declared model has a lineage, and an
    audit stage shares none with the extraction stages it verifies. Returns the
    manifest's `lineage_check` record when the run declares `audit`, else None.

    In a run that declares audit and no extraction stage the extractor is the
    arm's pin when the arm is pinned, else the spec's extraction stages; the
    lineages are always the current spec's."""
    lineage = spec.model_lineage
    missing = {s: m for s, m in models.items() if m not in lineage}
    if missing:
        raise ModelLineageMissing(
            f"run refused: no model_lineage entry for {missing} (stage: model). Every "
            "model a run calls declares its base-model lineage (R703(d)); add the entry "
            "to the spec's model_lineage, or declare a model that has one. Nothing was "
            "written.")
    if "audit" not in models:
        return None
    auditor = models["audit"]
    extractors = {s: m for s, m in models.items() if s in _LOCAL_EXTRACTION_STAGES}
    source = "spec"
    if not extractors and spec.extraction_models.arm:
        row = conn.execute(
            "SELECT configuration_json, configuration_marker FROM arms WHERE arm_name = ?",
            (spec.extraction_models.arm,)).fetchone()
        if row is not None and row[1] == ARM_PINNED:
            source = "pin"
            extractors = {s: v["model"] for s, v in json.loads(row[0])["stages"].items()
                          if s in _LOCAL_EXTRACTION_STAGES}
        else:
            from engine.agents.extractor import extraction_stages
            extractors = {s: stage_config(s, spec).model for s in extraction_stages(spec)}
        unmapped = {s: m for s, m in extractors.items() if m not in lineage}
        if unmapped:
            raise ModelLineageMissing(
                f"run refused: the audited arm {spec.extraction_models.arm!r}'s extractor "
                f"({source}) names {unmapped}, which model_lineage has no entry for "
                "(R703(d)); add the entry. Nothing was written.")
    same = {s: m for s, m in extractors.items() if lineage[m] == lineage[auditor]}
    if same:
        pairs = ", ".join(f"{s} → audit" for s in sorted(same))
        raise SameLineageVerifier(
            f"run refused: {pairs}: the auditor {auditor!r} (lineage "
            f"{lineage[auditor]!r}) shares a base-model lineage with the extractor "
            f"{sorted(set(same.values()))} (lineage {lineage[auditor]!r}; extractor "
            f"model from the {source}). Verification is cross-lineage (R703(e), "
            "INT-i12-45): declare an auditor of another lineage. Nothing was written.")
    return {"audit_model": auditor, "audit_lineage": lineage[auditor],
            "extractor_models": dict(sorted(extractors.items())),
            "extractor_lineages": {s: lineage[m] for s, m in sorted(extractors.items())},
            "extractor_source": source}


def pin_tuple(spec, arm_name: str, resolved: Mapping[str, ResolvedStage],
              codebook_hash: str, libraries: Mapping[str, str]) -> dict:
    """The configuration an arm is pinned to (P9c): everything that, if it
    changed, would make the arm's claims incomparable with its earlier ones."""
    arm = spec.arm(arm_name)
    stages = {
        key: {"model": r.config.model, "model_digest": r.model_digest,
              "options_hash": r.options_hash,
              "format_schema_hash": r.format_schema_hash,
              "prompt_hash": r.prompt_hash}
        for key, r in sorted(resolved.items()) if r.arm_name == arm_name
    }
    lib = {"ollama": "ollama"}.get(arm.provider, arm.provider)
    tup = {
        "arm_kind": _REGISTRY_KIND[arm.kind],
        "provider": arm.provider,
        "model": arm.model,
        "stages": stages,
        "codebook_hash": codebook_hash,
        "client_library": {lib: libraries.get(lib)} if lib else {},
    }
    if _UNIT_CITING_STAGE in stages:
        # INT-i12-4: the index space the arm's citations live in. The prompt
        # hash renders a two-line sentinel text and cannot see a change to how
        # a paper is cut into units; these three keys can.
        from engine.elicitation import units
        tup["segmentation"] = {"pysbd": libraries.get("pysbd"),
                               "min_unit_tokens": units.MIN_UNIT_TOKENS,
                               "units_version": units.UNITS_VERSION}
    return tup


def _diff_keys(old: Any, new: Any, prefix: str = "") -> list[str]:
    if isinstance(old, dict) and isinstance(new, dict):
        out = []
        for k in sorted(set(old) | set(new)):
            out += _diff_keys(old.get(k), new.get(k), f"{prefix}{k}.")
        return out
    return [] if old == new else [prefix.rstrip(".")]


# ── Run open ─────────────────────────────────────────────────────────
@dataclass(frozen=True)
class RunHandle:
    run_id: int
    run_uid: str
    stages: Mapping[str, ResolvedStage]


def open_run(conn, spec, *, kind: str, stages: Iterable[str], codebook,
             cloud_arms: Iterable[str] = (), preflight_models: Iterable[str] = (),
             arms: Iterable[str] = (),
             digest_fn: Callable[[str], str] | None = None,
             git: GitState | None = None, host: str | None = None,
             payload_description: str | None = None,
             selection_bound: Mapping[str, Any] | None = None,
             inputs: Mapping[str, str] | None = None,
             ollama_environment: str | None = None) -> RunHandle:
    """Write the manifest and pin its arms, or refuse. Before the first call.

    The local extraction arm is the exception (R569(b)): it is registered and,
    if already pinned, compared here, but pinned only by the run that reaches
    the extract stage — `bind_extraction_arm`, with the tuple resolved here.

    `stages` are resolver stage names; each cloud arm in `cloud_arms` adds
    `cloud:<arm>`. `arms` names extra arms to pin (a human arm, say). `codebook`
    is the loaded `Codebook` the run's prompts are built from.

    `selection_bound` (10b-C2), when given, is recorded verbatim under the
    manifest's `selection_bound` key — declared before the first call and, like
    the rest of the body, never edited. Absent, the manifest carries no such key.

    `inputs` (11b-ADJ D1, amending R203's wording) maps each input file a run
    reads — an import's decision file — to the sha256 of its bytes, recorded
    under the manifest's `inputs` key. `pins` stays the arm pins. Absent, the
    manifest carries no such key.

    `ollama_environment` (U19, R734(c)(i)) is the Ollama unit's `Environment=` value
    as `systemctl show` printed it, read by the caller before this open, recorded
    verbatim under the manifest's `ollama_environment` key. Absent, no such key.
    """
    if kind not in RUN_KINDS:
        raise ValueError(f"run kind {kind!r} is not one of {RUN_KINDS}")
    # U18, R721(c): the one Ollama host, resolved at import, must be loopback. First
    # among the refusals (R722(e)): it reads nothing and writes nothing.
    from engine.utils.ollama_client import is_loopback_host, ollama_host
    if not is_loopback_host(ollama_host()):
        raise OllamaHostNotLoopback(
            f"run refused: the Ollama host is {ollama_host()}, which is not a loopback "
            "address, so every local-model prompt — paper text included — would leave "
            "this machine. Nothing was read or written. Read OLLAMA_HOST in the "
            "environment that launched this process (`echo $OLLAMA_HOST`; empty means "
            "the library's default, which is loopback); unset it, or point it at this "
            "machine, and launch again.")
    # U19, R734(d)(i): and it must be the local service, whose unit the environment
    # check reads. Second among the refusals; it too reads nothing and writes nothing.
    from engine.utils.ollama_client import is_local_service_host
    if not is_local_service_host(ollama_host()):
        raise OllamaHostNotLocalService(
            f"OllamaHostNotLocalService: run refused: the Ollama host is {ollama_host()}, a "
            "loopback address on a port other than the local service's, whose configured "
            "environment the run checks and records; the calls would go to a server that "
            "check cannot see. Nothing was read or written. Read OLLAMA_HOST in the "
            "environment that launched this process (`echo $OLLAMA_HOST`; empty means the "
            "local service); unset it and launch again.")
    # INT-i12-9: every loader on the run path takes the codebook by path and
    # checks no identity (`load_codebook`, `load_codebook_beside`); this is the
    # one place every run passes. Before anything is resolved or written.
    declared = str(getattr(codebook, "review", "") or "").strip()
    if declared != spec.review_id:
        where = getattr(codebook, "path", None)
        said = f"declares review {declared!r}" if declared else "declares no review"
        raise CodebookReviewMismatch(
            f"run refused: the codebook at {where} {said}, and this run is for review "
            f"{spec.review_id!r}. Nothing was written. Read the codebook's `review:` line: "
            f"if the file is this review's (a copy made under R235 whose `review:` was "
            f"not changed), set it to {spec.review_id!r}; if it is another review's "
            "codebook, put this review's codebook beside the database.")
    g = git or git_state()
    if g.dirty:
        raise DirtyTree(
            "run refused: the working tree has uncommitted changes, so the commit "
            f"{g.commit[:12]} is not the code that would run. Commit or stash, then retry.")
    if len(g.state_tags) > 1:
        raise AmbiguousEngineState(
            f"run refused: HEAD {g.commit[:12]} carries more than one engine-state tag "
            f"{list(g.state_tags)}; a commit reaches one state. Remove the wrong tag, then retry.")
    engine_state = g.state_tags[0] if g.state_tags else None

    cloud_arms = list(cloud_arms)
    enabled = set(spec.cloud.enabled_arms)
    not_enabled = [a for a in cloud_arms if a not in enabled]
    if not_enabled:
        raise CloudArmNotEnabled(
            f"run refused: cloud arm(s) {not_enabled} are not in the spec's "
            f"cloud.enabled_arms {sorted(enabled)} — nothing leaves the machine "
            "that the spec does not enable (S3g, R6).")

    stage_list = list(stages) + [f"{CLOUD_STAGE_PREFIX}{a}" for a in cloud_arms]
    arms_by_stage = {s: a for s in stage_list if (a := _stage_arm(spec, s))}
    named_arms = sorted(set(arms_by_stage.values()) | set(arms))
    # R569(b), Design A: the local extraction arm is not pinned here. It is
    # pinned by the run that reaches the extract stage (`bind_extraction_arm`).
    # A cloud arm's open is its extract start, and an `arms=` arm is named to
    # be pinned, so both still pin at open.
    deferred = ({a for s, a in arms_by_stage.items() if s in _LOCAL_EXTRACTION_STAGES}
                - set(arms))

    # R59 / R21 before any resolution or write.
    existing = {}
    for name in named_arms:
        spec.arm(name)  # a named arm must be declared in the spec
        row = conn.execute(
            "SELECT configuration_json, configuration_marker, retired_at, pinned_sha256 "
            "FROM arms WHERE arm_name = ?", (name,)).fetchone()
        if row is None:
            continue
        cfg_json, marker, retired, pinned_sha = row
        if marker == PRE_MANIFEST:
            raise PreManifestArm(
                f"run refused: arm {name!r} was registered pre-manifest. It never pins "
                "and accepts no new claims (R59); declare a new arm name for a new "
                "configuration (R10).")
        if retired is not None:
            raise RetiredArm(f"run refused: arm {name!r} was retired at {retired} (R21).")
        existing[name] = (cfg_json, marker, pinned_sha)

    # R703(d)/(e): lineage coverage and the extraction→audit pair, before the
    # server is asked for a digest and before any row.
    preflight_models = list(preflight_models)
    lineage_check = _lineage_check(
        conn, spec, _declared_models(spec, stage_list, preflight_models))

    resolved = resolve_run(spec, [s for s in stage_list],
                           digest_fn=digest_fn, preflight_models=preflight_models,
                           codebook_path=codebook.path, arms_by_stage=arms_by_stage)
    libs = library_versions()

    pins = {}
    carried: dict[str, tuple[dict, str]] = {}
    for name in named_arms:
        tup = pin_tuple(spec, name, resolved, codebook.semantic_hash, libs)
        sha = sha256_canonical(tup)
        if name in deferred:
            carried[name] = (tup, sha)
        if name in existing and existing[name][1] == ARM_PINNED:
            if existing[name][2] != sha:
                diff = _diff_keys(json.loads(existing[name][0]), tup)
                raise ArmPinMismatch(
                    f"run refused: arm {name!r} is pinned to a different configuration; "
                    f"this run differs at {diff}. A changed configuration is a new arm "
                    "(R10); declare one in the spec.")
            continue
        if name not in deferred:
            pins[name] = (tup, sha)

    cloud_list = sorted(cloud_arms)
    run_uid = str(uuid.uuid4())
    started = _now()
    manifest = {
        "run_uid": run_uid, "review_id": spec.review_id, "run_kind": kind,
        "git": {"commit": g.commit, "dirty": g.dirty, "tag": g.tag},
        "engine_state": engine_state,
        "spec_hash": spec_hash(spec),
        "codebook": {"semantic_hash": codebook.semantic_hash, "sha256": codebook.sha256},
        "libraries": libs, "host": host or socket.gethostname(),
        "ollama_host": ollama_host(),
        "started_at": started, "cloud_arms": cloud_list,
        "payload_description": payload_description,
        "stages": {k: {**_stage_row(k, r), "lineage": spec.model_lineage[r.config.model]}
                   for k, r in sorted(resolved.items())},
        "pins": {k: v[1] for k, v in sorted(pins.items())},
    }
    if lineage_check is not None:
        manifest["lineage_check"] = lineage_check
    if selection_bound is not None:
        manifest["selection_bound"] = dict(selection_bound)
    if inputs is not None:
        manifest["inputs"] = dict(inputs)
    # A run that sends nothing to the Ollama server records no environment (R735(b)).
    if ollama_environment is not None:
        manifest["ollama_environment"] = ollama_environment
    if cloud_list and not payload_description:
        raise ValueError("a run with cloud arms records a payload description (S3g)")

    # A savepoint, not BEGIN: it nests inside a caller's open transaction instead
    # of failing on it, and RELEASE commits when it is the outermost.
    conn.execute("SAVEPOINT open_run")
    try:
        cur = conn.execute(
            "INSERT INTO run_manifests (run_uid, review_id, run_kind, git_commit, git_dirty, "
            "git_tag, engine_state, spec_hash, codebook_hash, codebook_sha256, "
            "library_versions_json, host, started_at, cloud_arms_json, payload_description, "
            "manifest_json, manifest_sha256) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_uid, spec.review_id, kind, g.commit, int(g.dirty), g.tag, engine_state,
             manifest["spec_hash"], codebook.semantic_hash, codebook.sha256,
             canonical_json(libs), manifest["host"], started, canonical_json(cloud_list),
             payload_description, canonical_json(manifest), sha256_canonical(manifest)))
        run_id = cur.lastrowid
        # Arms first: a stage row's arm_name is an FK to the registry.
        for name in sorted(deferred):
            if name not in existing:
                # Registered, not pinned: the row the stage rows' FK needs.
                conn.execute(
                    "INSERT INTO arms (arm_name, arm_kind, configuration_json, "
                    "configuration_marker, registered_at, retired_at) "
                    "VALUES (?, ?, '{}', NULL, ?, NULL)",
                    (name, _REGISTRY_KIND[spec.arm(name).kind], started))
        for name, (tup, sha) in pins.items():
            arm = spec.arm(name)
            if name in existing:
                conn.execute(
                    "UPDATE arms SET configuration_json = ?, configuration_marker = ?, "
                    "pinned_run_id = ?, pinned_sha256 = ? WHERE arm_name = ?",
                    (canonical_json(tup), ARM_PINNED, run_id, sha, name))
            else:
                conn.execute(
                    "INSERT INTO arms (arm_name, arm_kind, configuration_json, "
                    "configuration_marker, registered_at, retired_at, pinned_run_id, "
                    "pinned_sha256) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)",
                    (name, _REGISTRY_KIND[arm.kind], canonical_json(tup), ARM_PINNED,
                     started, run_id, sha))
        for key, r in sorted(resolved.items()):
            row = _stage_row(key, r)
            conn.execute(
                "INSERT INTO run_stage_configs (run_id, stage, stage_kind, arm_name, "
                "provider, model_name, model_digest, options_json, options_hash, "
                "sent_keys_json, sources_json, keep_alive, format_schema_hash, prompt_hash) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (run_id, key, row["stage_kind"], r.arm_name, r.config.provider,
                 r.config.model, r.model_digest, canonical_json(row["options"]),
                 r.options_hash, canonical_json(row["sent_keys"]),
                 canonical_json(row["sources"]), row["keep_alive"],
                 r.format_schema_hash, r.prompt_hash))
        conn.execute("RELEASE open_run")
        if conn.in_transaction:
            conn.commit()
    except BaseException:
        conn.execute("ROLLBACK TO open_run")
        conn.execute("RELEASE open_run")
        raise
    # R225a: registered once, on success only — a rolled-back open never ran.
    _RUN_FIELD_NAMES[run_id] = frozenset(codebook.field_names)
    if carried:
        _RUN_ARM_TUPLES[(_database_file(conn), run_id)] = carried
    _RUN_DIGEST_FNS[(_database_file(conn), run_id)] = digest_fn
    return RunHandle(run_id=run_id, run_uid=run_uid, stages=resolved)


#: (database file, run_id) -> {arm: (pin tuple, its sha256)} for each local
#: extraction arm the run resolved at open. The run's own tuple, carried to its
#: extract stage in this process so nothing is resolved twice (R569(b)).
#: Registered on a successful open only, like `_RUN_FIELD_NAMES`.
_RUN_ARM_TUPLES: dict[tuple[str, int], dict[str, tuple[dict, str]]] = {}

#: (database file, run_id) -> the `digest_fn` `open_run` was given, for the per-call
#: digest check (U18 commit 2, R721(b)); `None` means `fetch_model_digest`, looked up
#: at call time. Registered on a successful open only, like `_RUN_ARM_TUPLES`, and
#: like it never cleared. A run with no entry (opened without `open_run`, M3's test
#: affordance) has no check.
_RUN_DIGEST_FNS: dict[tuple[str, int], Callable[[str], str] | None] = {}


def _database_file(conn) -> str:
    return conn.execute("PRAGMA database_list").fetchone()[2]


def bind_extraction_arm(conn, run_id: int, *, arm: str) -> None:
    """R569(b), Design A: at the extract stage, before anything is selected or
    sent, bind `arm` to the tuple run `run_id` resolved at open.

    * The arm is unpinned -> it is pinned with that tuple, `pinned_run_id` this
      run. One write, committed here.
    * The arm is pinned -> the whole tuple is compared, and a difference
      refuses the run naming every differing key. Nothing is written, so a
      second call in the same run is a comparison that agrees.

    The tuple is the one `open_run` computed, carried in this process; it is
    not resolved again. For a run this process did not open there is no tuple
    to bind or compare, and this returns: `extraction_digest`, which every
    caller runs next, then refuses an unpinned arm and compares each stage's
    digest against a pinned one, from the run's own rows.
    """
    held = _RUN_ARM_TUPLES.get((_database_file(conn), run_id), {}).get(arm)
    if held is None:
        return
    tup, sha = held
    row = conn.execute(
        "SELECT configuration_json, configuration_marker, pinned_sha256 FROM arms "
        "WHERE arm_name = ?", (arm,)).fetchone()
    if row is not None and row[1] == ARM_PINNED:
        if row[2] != sha:
            diff = _diff_keys(json.loads(row[0]), tup)
            raise ArmPinMismatch(
                f"run refused at the extract stage: arm {arm!r} is pinned to a different "
                f"configuration; run {run_id} differs at {diff}. Another run reached "
                "extract first and pinned the arm. A changed configuration is a new arm "
                "(R10); declare one in the spec.")
        return
    conn.execute("SAVEPOINT bind_extraction_arm")
    try:
        conn.execute(
            "UPDATE arms SET configuration_json = ?, configuration_marker = ?, "
            "pinned_run_id = ?, pinned_sha256 = ? WHERE arm_name = ? "
            "AND configuration_marker IS NULL",
            (canonical_json(tup), ARM_PINNED, run_id, sha, arm))
        conn.execute("RELEASE bind_extraction_arm")
        if conn.in_transaction:
            conn.commit()
    except sqlite3.IntegrityError as exc:
        conn.execute("ROLLBACK TO bind_extraction_arm")
        conn.execute("RELEASE bind_extraction_arm")
        # The freeze trigger: an unpinned arm that already holds a claim.
        raise ArmPinMismatch(
            f"run refused at the extract stage: arm {arm!r} is not pinned and cannot be "
            f"pinned by run {run_id} — {exc}. An arm that holds claims made before any "
            "run pinned it has no configuration of record; declare a new arm name in "
            "the spec (R10).") from exc
    except BaseException:
        conn.execute("ROLLBACK TO bind_extraction_arm")
        conn.execute("RELEASE bind_extraction_arm")
        raise


def _stage_row(key: str, r: ResolvedStage) -> dict:
    cfg = r.config
    return {
        "stage_kind": "cloud" if key.startswith(CLOUD_STAGE_PREFIX) else cfg.stage,
        "arm": r.arm_name, "provider": cfg.provider, "model": cfg.model,
        "model_digest": r.model_digest,
        "options": {**dict(cfg.options), "__recorded__": dict(cfg.recorded),
                    **({"__think__": cfg.think} if "think" in cfg.sent_keys else {})},
        "options_hash": r.options_hash,
        "sent_keys": sorted(cfg.sent_keys), "sources": dict(cfg.sources),
        "keep_alive": str(cfg.keep_alive) if "keep_alive" in cfg.sent_keys else "unset",
        "format_schema_hash": r.format_schema_hash, "prompt_hash": r.prompt_hash,
    }


def stage_digest(conn, run_id: int, stage: str) -> str | None:
    """The digest this run resolved for `stage`, or None if it did not declare it."""
    row = conn.execute("SELECT model_digest FROM run_stage_configs "
                       "WHERE run_id = ? AND stage = ?", (run_id, stage)).fetchone()
    return None if row is None else row[0]


def active_stage_row(stage: str) -> tuple[str, str] | None:
    """The active run's declared `(options_hash, options_json)` for `stage`
    (R223), or `None` outside a run or when this run never declared the stage —
    both cases leave `EffectiveConfig.with_options` at today's behaviour.

    Reads `_ACTIVE` directly rather than taking `(conn, run_id)`: it is called
    from `with_options`, which has neither."""
    active = _ACTIVE.get()
    if active is None:
        return None
    conn, run_id = active
    row = conn.execute("SELECT options_hash, options_json FROM run_stage_configs "
                       "WHERE run_id = ? AND stage = ?", (run_id, stage)).fetchone()
    return None if row is None else (row[0], row[1])


def extraction_digest(conn, run_id: int, *, arm: str, stages: Iterable[str]) -> str:
    """The run's one extraction digest (R117), checked against `arm`'s pin.

    `stages` are the run's local extraction stages, the pass-1 stage first. The
    pass-1 stage must be declared; every declared one must carry the digest the
    arm was pinned to. Reads only — a refusal here writes nothing.
    """
    stages = list(stages)
    first = stage_digest(conn, run_id, stages[0])
    if first is None:
        raise StageNotInRun(
            f"run refused: run {run_id} has no {stages[0]!r} stage — either no such "
            "run was opened or it did not declare extraction. Open the run with the "
            "extraction stages before extracting (R116).")
    row = conn.execute("SELECT configuration_json, configuration_marker FROM arms "
                       "WHERE arm_name = ?", (arm,)).fetchone()
    pinned = (json.loads(row[0]).get("stages", {})
              if row and row[1] == ARM_PINNED and row[0] else None)
    if pinned is None:
        raise ArmPinMismatch(
            f"run refused: arm {arm!r} is not pinned, so run {run_id}'s digest {first} "
            "has no pin to agree with (R10, R117).")
    for stage in stages:
        digest = stage_digest(conn, run_id, stage)
        if digest is None:
            continue
        pin = (pinned.get(stage) or {}).get("model_digest")
        if digest != pin:
            raise ArmPinMismatch(
                f"run refused: stage {stage!r} of run {run_id} resolved digest {digest}, "
                f"but arm {arm!r} is pinned to {pin}. A changed model is a new arm "
                "(R10); declare one in the spec.")
    return first


def open_review_session(conn, spec, *, codebook, git: GitState | None = None,
                        host: str | None = None, arms: Iterable[str] = ()) -> RunHandle:
    """R68: a human review session is a run of kind `review_session` with zero
    stage rows. Its reviewer events carry its run_id."""
    return open_run(conn, spec, kind="review_session", stages=(), codebook=codebook,
                    arms=arms, git=git, host=host, digest_fn=lambda m: "")


#: The longest `failure_reason` a manifest stores.
FAILURE_REASON_MAX = 500


def failure_reason(exc: BaseException, context: str | None = None) -> str:
    """The `end_reason` of a 'failed' close: "<ExceptionClass>: <message>", or
    "<ExceptionClass>: <context>: <message>" when the site has a label for where
    it failed (an import's entry label). R625(a)(8), INT-l12-14.

    The class name is unqualified; the message is the first line of `str(exc)`
    with its whitespace collapsed; an empty message leaves "<ExceptionClass>" or
    "<ExceptionClass>: <context>". The whole is cut to `FAILURE_REASON_MAX`
    characters, ending in "…" when cut. This is the only place the string is
    assembled: a site passes the exception and, where it has one, its label.
    """
    lines = str(exc).strip().splitlines()
    message = " ".join(lines[0].split()) if lines else ""
    label = " ".join(context.split()) if context else ""
    reason = ": ".join(part for part in (type(exc).__name__, label, message) if part)
    if len(reason) > FAILURE_REASON_MAX:
        reason = reason[:FAILURE_REASON_MAX - 1] + "…"
    return reason


def close_run(conn, run_id: int, status: str = "completed",
              reason: str | None = None) -> None:
    """`reason` is 022's `end_reason` column (R215/C26); 022's CHECK requires
    one for 'aborted', refuses one for 'completed', and permits either for
    'failed' and 'interrupted' (R234). `run_pipeline` passes the RunAborted
    message for 'aborted' and an `INTERRUPTED_REASONS` value at its gate stops
    (C40); `run_pipeline` and `run_ft_invocation` pass `interrupt_reason(exc)`
    — "interrupt:SIGINT" / "interrupt:SIGTERM" / "interrupt:SIGHUP" — on an
    interrupt (C47); the three import paths pass the refused entry for 'aborted'.
    Every 'failed' close passes `failure_reason(exc)` — the exception's class and
    message, with the entry label as context where an import has one
    (R625(a)(8)). Free text apart from `INTERRUPTED_REASONS`; this function does
    not check it. A second close is refused by 022's `run_manifests_end_once`
    trigger (an IntegrityError) — callers that may race one check `is_closed`."""
    if status not in END_STATUSES:
        raise ValueError(f"end status {status!r} is not one of {END_STATUSES}")
    conn.execute(
        "UPDATE run_manifests SET ended_at = ?, end_status = ?, end_reason = ? "
        "WHERE run_id = ?",
        (_now(), status, reason, run_id))
    conn.commit()


def is_closed(conn, run_id: int) -> bool:
    """True once the run's end is recorded (`ended_at` set). An interrupt
    handler checks this first, so a run already closed keeps its first close
    rather than meeting the end-once trigger (C47)."""
    row = conn.execute("SELECT ended_at FROM run_manifests WHERE run_id = ?",
                       (run_id,)).fetchone()
    return row is not None and row[0] is not None


def interrupt_signum(exc: BaseException) -> int:
    """The signal an interrupt stands for: SIGINT for KeyboardInterrupt, the
    carried number for RunInterrupted. Anything else is not an interrupt."""
    if isinstance(exc, KeyboardInterrupt):
        return signal.SIGINT
    if isinstance(exc, RunInterrupted):
        return exc.signum
    raise TypeError(f"{type(exc).__name__} is not an interrupt")


def interrupt_reason(exc: BaseException) -> str:
    """The `INTERRUPTED_REASONS` value an interrupted close records (C47)."""
    return _INTERRUPT_REASON_BY_SIGNAL[interrupt_signum(exc)]


#: The exit codes a command-line main returns after an interrupt: 128 + signum
#: for SIGINT / SIGTERM / SIGHUP — 130 / 143 / 129 (C47).
INTERRUPT_EXIT_CODES = frozenset(128 + signum for signum in _INTERRUPT_REASON_BY_SIGNAL)


def _flush_logging_handlers() -> None:
    loggers = [logging.getLogger()] + [
        lg for lg in logging.Logger.manager.loggerDict.values()
        if isinstance(lg, logging.Logger)]
    for handler in {h for lg in loggers for h in lg.handlers}:
        try:
            handler.flush()
        except Exception:  # a broken stream must not keep the process alive
            pass


def exit_process(code: int) -> NoReturn:
    """End a command-line process with `main()`'s exit code (E-EXEC).

    An interrupt exit (`INTERRUPT_EXIT_CODES`) flushes every logging handler,
    then stdout and stderr, then `os._exit(code)`. Normal interpreter shutdown
    would join `ollama_chat`'s executor worker — concurrent.futures' exit hook
    joins every started worker — and hold the process, and Ollama's GPU, for the
    rest of the abandoned generation (E-EXEC-A: 77 s on gemma3:27b; up to the
    900 s read timeout). `os._exit` closes the socket, and Ollama cancels a
    request whose client disconnects (E-EXEC-A V4). Nothing is lost: when
    `main()` returns an interrupt code every cleanup has already run — the
    experiment lock's release, the rollback and the 'interrupted' close, the
    entry point's `finally` with `db.close()`, the log line (C47) — and no atexit
    hook the CLIs register persists run data (E-EXEC Step 1).

    Every other code goes through `sys.exit(code)` and normal shutdown,
    unchanged. Called only from a module's `if __name__ == "__main__":` line,
    never inside `main()`, so tests that call `main()` keep their process."""
    if code in INTERRUPT_EXIT_CODES:
        _flush_logging_handlers()
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.flush()
            except Exception:
                pass
        os._exit(code)
    sys.exit(code)


@contextlib.contextmanager
def interrupt_signals():
    """Install SIGTERM and SIGHUP handlers that raise `RunInterrupted(signum)`,
    and restore the prior handlers on exit, whatever ends the block (C47).

    Only a command-line main uses this — never `run_pipeline()` or
    `run_ft_invocation()` themselves, so a library caller or a test keeps its
    own handlers. SIGINT keeps Python's default KeyboardInterrupt."""
    def _raise(signum, _frame):
        raise RunInterrupted(signum)

    prior = {}
    try:
        for signum in (signal.SIGTERM, signal.SIGHUP):
            prior[signum] = signal.signal(signum, _raise)
        yield
    finally:
        for signum, handler in prior.items():
            signal.signal(signum, handler)


# ── Call recording ───────────────────────────────────────────────────
def request_hash(request: Mapping[str, Any]) -> str:
    """The hash of a request as handed to the client — every kwarg (C16)."""
    return sha256_canonical(dict(request))


#: `record_call`'s `request_hash=` keyword shadows this name inside that
#: function's body (R224a(3)); this alias is what it falls back to.
_compute_request_hash = request_hash


def response_digest(content: str | None, thinking: str | None = None) -> str:
    return sha256_canonical({"content": content or "", "thinking": thinking or ""})


def record_call(conn, run_id: int, stage: str, paper_id: int | None,
                request: Mapping[str, Any], digest: str | None,
                started_at: str, ended_at: str, *,
                outcome: str,
                outcome_detail: str | None = None,
                request_hash: str | None = None) -> int:
    """`outcome` (R216/C24, 022's `run_calls.outcome` column) is required,
    keyword-only, no default (10a-C3): every caller now names it explicitly,
    so a caller this task missed fails loudly (TypeError) rather than
    silently writing 'completed'. `ollama_chat`'s recorder (10a-C3) is the
    only caller reached from a refusal path; the cloud path's `send()` still
    passes 'completed' explicitly (F15 — its own outcome semantics are out of
    scope here).

    `request_hash` (R224a(3)): the caller's own hash of `request`, when it
    already computed one — `ollama_chat` always does, once, and hands it
    here rather than paying for a second identical hash. `None` (every other
    caller, including the cloud path's `send()`) computes it here exactly as
    before this parameter existed — behaviour-preserving by construction: the
    module-level `request_hash()` function is the same one either way."""
    rh = request_hash if request_hash is not None else _compute_request_hash(request)
    cur = conn.execute(
        "INSERT INTO run_calls (run_id, stage, paper_id, request_hash, response_digest, "
        "started_at, ended_at, outcome, outcome_detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (run_id, stage, paper_id, rh, digest, started_at, ended_at,
         outcome, outcome_detail))
    conn.commit()
    return cur.lastrowid


_ACTIVE: contextvars.ContextVar = contextvars.ContextVar("active_run", default=None)

#: R225a (10a-C10, amending R225/B15): the codebook field set is a property of
#: the RUN, not of the activation — a process-local registry keyed by run_id,
#: written once by open_run and never changed. This is what makes nested
#: activation of the same run_id safe: activate()/active_run() carry no
#: field_names of their own to overwrite, so extractor.py's inner
#: `with rm.active_run(db._conn, run_id):` (the same run_id run_pipeline
#: already activated) cannot reset anything — there is nothing per-activation
#: to reset. A run opened without open_run (a test monkeypatching
#: _open_run_manifest, M3) has no registry entry and so no check — a test
#: affordance, not a production gap.
_RUN_FIELD_NAMES: dict[int, frozenset[str]] = {}


def activate(conn, run_id: int):
    """Record every Ollama call from here on against `run_id`. Returns a token
    for `deactivate`."""
    return _ACTIVE.set((conn, run_id))


def deactivate(token) -> None:
    _ACTIVE.reset(token)


@contextlib.contextmanager
def active_run(conn, run_id: int):
    """Record every Ollama call made inside the block against `run_id`."""
    token = activate(conn, run_id)
    try:
        yield
    finally:
        deactivate(token)


def active_field_names() -> frozenset[str] | None:
    """The active run's registered codebook field names (R225a), or `None`
    outside a run or when the active run_id was never registered by
    `open_run` (M3's test affordance)."""
    active = _ACTIVE.get()
    if active is None:
        return None
    _, run_id = active
    return _RUN_FIELD_NAMES.get(run_id)


def call_stage_key(stage: str, model: str | None) -> str:
    """The `run_stage_configs` key a call is declared and recorded under: the
    stage, except `preflight`, which is declared once per probed model (R63)."""
    return f"preflight:{model}" if stage == "preflight" else stage


def check_declared_call(stage: str | None, model: str) -> None:
    """C54: under an active run, refuse a call the manifest did not declare.

    Called by `ollama_chat` before it sends anything. Raises `UndeclaredCall`
    when the run has no `run_stage_configs` row for the call's stage key, or
    that row's `model_name` is not `model`. A no-op outside a run, and for a
    call that names no stage — the same two cases in which
    `record_active_ollama_call` records nothing. Reads only."""
    active = _ACTIVE.get()
    if active is None or stage is None:
        return
    conn, run_id = active
    key = call_stage_key(stage, model)
    row = conn.execute("SELECT model_name FROM run_stage_configs "
                       "WHERE run_id = ? AND stage = ?", (run_id, key)).fetchone()
    declared = None if row is None else row[0]
    if declared != model:
        raise UndeclaredCall(key, declared, model)


def check_call_digest(stage: str | None, model: str) -> str | None:
    """U18 commit 2, R721(b): under an active run, read `model`'s current digest
    through the run's own `digest_fn` and refuse the call if it is not the digest the
    manifest recorded for the call's stage key. Called by `ollama_chat` after
    `check_declared_call` and before the input-fit read and the send. Returns the
    digest read, or `None` when no check applies — outside a run, for a call that
    names no stage, or for a run `open_run` did not register.

    A digest that cannot be read is refused the same way, except a refused or
    timed-out connection, which is re-raised as itself: it stays the run fault it
    was before this check (R532)."""
    active = _ACTIVE.get()
    if active is None or stage is None:
        return None
    conn, run_id = active
    entry = (_database_file(conn), run_id)
    if entry not in _RUN_DIGEST_FNS:
        return None
    from engine.utils import ollama_client as oc
    read = _RUN_DIGEST_FNS[entry] or oc.fetch_model_digest
    key = call_stage_key(stage, model)
    recorded = stage_digest(conn, run_id, key)
    try:
        current = read(model)
    except oc.ModelDigestError as exc:
        import httpx
        if isinstance(exc.__cause__, (ConnectionError, httpx.ConnectError, httpx.ConnectTimeout)):
            raise exc.__cause__ from exc
        raise ModelDigestChanged.at(key, model, recorded, None, detail=str(exc)) from exc
    if current != recorded:
        raise ModelDigestChanged.at(key, model, recorded, current)
    return current


def record_active_ollama_call(stage: str | None, request: Mapping[str, Any],
                              paper_id: int | None, response, started_at: str, *,
                              outcome: str,
                              outcome_detail: str | None = None,
                              request_hash: str | None = None) -> None:
    """Called by `ollama_chat` on every outcome (10a-C3, R216) — success and
    every refusal alike. A no-op outside a run. `response` is None on a
    pre-call refusal (nothing was sent, so there is nothing to digest);
    `digest` stays NULL for it rather than the digest of an empty message.

    `request_hash` (R224a(3)): `ollama_chat` always passes its own, computed
    once; this is a pure pass-through to `record_call`, which falls back to
    computing it when `None` (every other caller of `record_call` directly,
    i.e. the cloud path's `send()`, which this function is never reached
    from)."""
    active = _ACTIVE.get()
    if active is None or stage is None:
        return
    conn, run_id = active
    key = call_stage_key(stage, request.get("model"))
    digest = None
    if response is not None:
        msg = getattr(response, "message", None)
        digest = response_digest(getattr(msg, "content", None), getattr(msg, "thinking", None))
    record_call(conn, run_id, key, paper_id, request, digest, started_at, _now(),
               outcome=outcome, outcome_detail=outcome_detail, request_hash=request_hash)


def stage_names_agree() -> tuple[str, ...]:
    """The resolver's stage vocabulary, for the 020 agreement test."""
    return OLLAMA_STAGES
