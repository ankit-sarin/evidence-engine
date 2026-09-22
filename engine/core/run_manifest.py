"""Run open, arm pinning and call recording (S3a, S3b, S3g, R59, R66–R68).

MANIFEST-01 Phase 2a. A run writes its manifest — `run_manifests` plus one
`run_stage_configs` row per stage — **before its first model call**, in one
transaction with the pins of every arm it names. Nothing here makes a model
call: the only network use is `fetch_model_digest` (`GET /api/tags`, which loads
no model), injectable as `digest_fn`.

**Refusals, all before anything is written:**

* `DirtyTree` — the working tree has uncommitted changes. An untagged HEAD is
  allowed; `engine_state` stays NULL until the freshman tag (session 10).
* `PreManifestArm` — a named arm was registered pre-manifest (R59).
* `RetiredArm` — a named arm is retired (R21).
* `ArmPinMismatch` — a named arm is pinned and this run resolves it differently
  (R10); the differing keys are named.
* `CloudArmNotEnabled` — a cloud arm is requested that `cloud.enabled_arms`
  does not enable (S3g).

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
import socket
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from engine.core.effective import PRE_MANIFEST
from engine.core.effective_config import (
    CLOUD_STAGE_PREFIX, OLLAMA_STAGES, ResolvedStage, canonical_json,
    resolve_run, sha256_canonical,
)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

#: Re-declared in migration 020 (R35); a test asserts the two agree.
RUN_KINDS = ("extraction", "screening", "judge", "review_session")
END_STATUSES = ("completed", "failed", "interrupted")
ARM_PINNED = "pinned"

#: R75: the client libraries whose versions every manifest records.
LIBRARIES = ("ollama", "openai", "anthropic")

#: Spec arm kind -> registry arm kind (016's CHECK spells the human kind in full).
_REGISTRY_KIND = {"model": "model", "human": "human_extractor"}

#: The extraction stages whose claims land on `extraction_models.arm`.
_LOCAL_EXTRACTION_STAGES = ("extract_pass1", "extract_pass2",
                            "extract_retry_snippet", "elicitation_pass1")


class RunRefused(Exception):
    """A run that may not open. Raised before anything is written."""


class DirtyTree(RunRefused):
    pass


class PreManifestArm(RunRefused):
    pass


class RetiredArm(RunRefused):
    pass


class ArmPinMismatch(RunRefused):
    pass


class CloudArmNotEnabled(RunRefused):
    pass


# ── Inputs a manifest records ────────────────────────────────────────
@dataclass(frozen=True)
class GitState:
    commit: str
    dirty: bool
    tag: str | None


def git_state(repo_root: Path = REPO_ROOT) -> GitState:
    def _git(*args) -> subprocess.CompletedProcess:
        return subprocess.run(["git", "-C", str(repo_root), *args],
                              capture_output=True, text=True)
    commit = _git("rev-parse", "HEAD").stdout.strip()
    dirty = bool(_git("status", "--porcelain").stdout.strip())
    tag = _git("describe", "--exact-match", "--tags", "HEAD")
    return GitState(commit=commit, dirty=dirty,
                    tag=tag.stdout.strip() if tag.returncode == 0 else None)


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
    return {
        "arm_kind": _REGISTRY_KIND[arm.kind],
        "provider": arm.provider,
        "model": arm.model,
        "stages": stages,
        "codebook_hash": codebook_hash,
        "client_library": {lib: libraries.get(lib)} if lib else {},
    }


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
             payload_description: str | None = None) -> RunHandle:
    """Write the manifest and pin its arms, or refuse. Before the first call.

    `stages` are resolver stage names; each cloud arm in `cloud_arms` adds
    `cloud:<arm>`. `arms` names extra arms to pin (a human arm, say). `codebook`
    is the loaded `Codebook` the run's prompts are built from.
    """
    if kind not in RUN_KINDS:
        raise ValueError(f"run kind {kind!r} is not one of {RUN_KINDS}")
    g = git or git_state()
    if g.dirty:
        raise DirtyTree(
            "run refused: the working tree has uncommitted changes, so the commit "
            f"{g.commit[:12]} is not the code that would run. Commit or stash, then retry.")

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

    resolved = resolve_run(spec, [s for s in stage_list],
                           digest_fn=digest_fn, preflight_models=preflight_models,
                           codebook_path=codebook.path, arms_by_stage=arms_by_stage)
    libs = library_versions()

    pins = {}
    for name in named_arms:
        tup = pin_tuple(spec, name, resolved, codebook.semantic_hash, libs)
        sha = sha256_canonical(tup)
        if name in existing and existing[name][1] == ARM_PINNED:
            if existing[name][2] != sha:
                diff = _diff_keys(json.loads(existing[name][0]), tup)
                raise ArmPinMismatch(
                    f"run refused: arm {name!r} is pinned to a different configuration; "
                    f"this run differs at {diff}. A changed configuration is a new arm "
                    "(R10); declare one in the spec.")
            continue
        pins[name] = (tup, sha)

    cloud_list = sorted(cloud_arms)
    run_uid = str(uuid.uuid4())
    started = _now()
    manifest = {
        "run_uid": run_uid, "review_id": spec.review_id, "run_kind": kind,
        "git": {"commit": g.commit, "dirty": g.dirty, "tag": g.tag},
        "engine_state": None,
        "spec_hash": spec_hash(spec),
        "codebook": {"semantic_hash": codebook.semantic_hash, "sha256": codebook.sha256},
        "libraries": libs, "host": host or socket.gethostname(),
        "started_at": started, "cloud_arms": cloud_list,
        "payload_description": payload_description,
        "stages": {k: _stage_row(k, r) for k, r in sorted(resolved.items())},
        "pins": {k: v[1] for k, v in sorted(pins.items())},
    }
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
            (run_uid, spec.review_id, kind, g.commit, int(g.dirty), g.tag, None,
             manifest["spec_hash"], codebook.semantic_hash, codebook.sha256,
             canonical_json(libs), manifest["host"], started, canonical_json(cloud_list),
             payload_description, canonical_json(manifest), sha256_canonical(manifest)))
        run_id = cur.lastrowid
        # Arms first: a stage row's arm_name is an FK to the registry.
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
    return RunHandle(run_id=run_id, run_uid=run_uid, stages=resolved)


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


def open_review_session(conn, spec, *, codebook, git: GitState | None = None,
                        host: str | None = None, arms: Iterable[str] = ()) -> RunHandle:
    """R68: a human review session is a run of kind `review_session` with zero
    stage rows. Its reviewer events carry its run_id."""
    return open_run(conn, spec, kind="review_session", stages=(), codebook=codebook,
                    arms=arms, git=git, host=host, digest_fn=lambda m: "")


def close_run(conn, run_id: int, status: str = "completed") -> None:
    if status not in END_STATUSES:
        raise ValueError(f"end status {status!r} is not one of {END_STATUSES}")
    conn.execute("UPDATE run_manifests SET ended_at = ?, end_status = ? WHERE run_id = ?",
                 (_now(), status, run_id))
    conn.commit()


# ── Call recording ───────────────────────────────────────────────────
def request_hash(request: Mapping[str, Any]) -> str:
    """The hash of a request as handed to the client — every kwarg (C16)."""
    return sha256_canonical(dict(request))


def response_digest(content: str | None, thinking: str | None = None) -> str:
    return sha256_canonical({"content": content or "", "thinking": thinking or ""})


def record_call(conn, run_id: int, stage: str, paper_id: int | None,
                request: Mapping[str, Any], digest: str | None,
                started_at: str, ended_at: str) -> int:
    cur = conn.execute(
        "INSERT INTO run_calls (run_id, stage, paper_id, request_hash, response_digest, "
        "started_at, ended_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (run_id, stage, paper_id, request_hash(request), digest, started_at, ended_at))
    conn.commit()
    return cur.lastrowid


_ACTIVE: contextvars.ContextVar = contextvars.ContextVar("active_run", default=None)


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


def record_active_ollama_call(stage: str | None, request: Mapping[str, Any],
                              paper_id: int | None, response, started_at: str) -> None:
    """Called by `ollama_chat` after a call returns. A no-op outside a run."""
    active = _ACTIVE.get()
    if active is None or stage is None:
        return
    conn, run_id = active
    key = f"preflight:{request.get('model')}" if stage == "preflight" else stage
    msg = getattr(response, "message", None)
    digest = response_digest(getattr(msg, "content", None), getattr(msg, "thinking", None))
    record_call(conn, run_id, key, paper_id, request, digest, started_at, _now())


def stage_names_agree() -> tuple[str, ...]:
    """The resolver's stage vocabulary, for the 020 agreement test."""
    return OLLAMA_STAGES
