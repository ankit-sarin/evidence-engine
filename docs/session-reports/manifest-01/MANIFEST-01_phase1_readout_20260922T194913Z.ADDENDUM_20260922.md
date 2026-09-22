# Addendum 2026-09-22 to MANIFEST-01 Phase 1 read-out

The read-out (`MANIFEST-01_phase1_readout_20260922T194913Z.md`) is not edited. This addendum
records where Phase 2a (`1cd80a498b3894378847d1a7c54702b501f10285`) departed from P9, with the
ruling or measurement behind each departure.

1. **P9(a)/(d), the run-link CHECK.** P9(d) proposed
   `CHECK (run_id IS NOT NULL OR run_marker = 'pre-manifest')`. **MEASURED in Phase 2a:** that
   text admits a row with `run_id` and `run_marker` both NULL, because SQLite passes a CHECK that
   evaluates to NULL. Superseded by **R77**, which enumerates the two permitted states with `IS`;
   **R78** makes every 020 CHECK NULL-safe; **R79** is Step 4 rule 11. The same audit found a
   second hole in P9(a)'s run-stage digest CHECK (`provider <> 'ollama' OR length(model_digest) = 64`
   admits a NULL digest), now closed.
2. **P9(a), `run_manifests.run_kind`.** P9 proposed six kinds. The Phase 2a brief fixed four:
   `extraction`, `screening`, `judge`, `review_session`.
3. **P9(b), the stage list.** P9(b)'s single abstract stage was two in the proposal, and the
   Phase 2a brief named one (`abstract_screen`). It is implemented as `abstract_screen_primary` /
   `abstract_screen_verifier`, because a stage carries one model. See the Phase 2a report §7.
4. **P9(b), which options become explicit.** Decided by **R66/R69**: nothing newly sent;
   `seed` and `num_ctx` recorded with source `modelfile_or_server` where not sent.
5. **P9(d), `run_calls`.** As proposed, with a composite FK `(run_id, stage)` →
   `run_stage_configs`, so a call can name only a declared stage.
6. **P9(g), table count.** P9 did not state the post-020 table count. It is **34** (31 + three new
   tables), measured on a throwaway copy of live.
