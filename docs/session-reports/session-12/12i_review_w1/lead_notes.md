# lead notes (W1-c) — verdict per Class 1/2 candidate after re-derivation at HEAD b711683
All 29 reader reproducers rerun by lead: stdout identical to recorded after masking timestamps/temp names (stderr was merged in 5 recorded outs).
L01-C1 CONFIRMED re-derived (counts_toward_abort `return not outcome.fields`; plan `if not rec.fields`; selection skip by reuse key; extractor resets counter). = L02-C1 (merge).
L01-C3 CONFIRMED re-derived (locator.py SequenceMatcher(None, norm_snippet, window) no autojunk=False; classifier/font_audit pass False). rerun shows 0.732 vs 0.974.
L01-C4 CONFIRMED mechanics (citation_guard escape_u exemption not mode-gated; legacy_record has no DECLINED branch). conditional.
L02-C1 CONFIRMED (contracts unparseable -> FIELD_MISSING all; terminal `if not rec.ok`; pipeline `if n_evidenced … else Pass 2 skipped`) — MERGE with L01-C1 (trigger side).
L02-C2 CONFIRMED (value_divergence/elicitation_run_id/accepted_pass1_attempt set on _LAST_PASS2_TELEMETRY at pipeline tail; extractor reads only model/finish_reason/raw_content/prompt_eval_count; no other reader by grep).
L02-C3 CONFIRMED code (spans.append with span.value unchecked; guard: empty value_u w/ evidence -> no offender). MERGE with W1-F1-C3 (legacy site).
L02-C4 CONFIRMED (contracts `_parse_steps`: non-dict skipped; criteria_application=bool(...); text unchecked).
L02-C5 CONFIRMED (a) unit_map dir = _default_run_id() process timestamp, recorded only in dead telemetry [extends INT-g12-1/R568]; (b) LIBRARIES=("ollama","openai","anthropic") no pysbd; sentinel prompt hash.
L02-C6 CONFIRMED regex `<!--.*?-->` DOTALL; comment claims unterminated tails matched. conditional on corpus. extends A-10.
L02-C7 CONFIRMED (except DuplicateFieldError -> context_chain=(ctx_hash,) single).
L06-C1 = L01-C2 CONFIRMED (effective.py `others <= e.against_decisions` governs; action via against_claims only; events.py refuses only when both empty). Rerun case A: CORRECT→ row 11 machine value. No reviewer-event writer at HEAD (grep: only effective, events, migrations, adjudication/schema). MERGE.
L06-C2 CONFIRMED (events.py `len(live) > 1` counts claim ids; effective row 6 text "ACCEPT is refused at write"; rerun B1/B2). conditional: needs duplicate asserted values under one claim id.
L06-C3 CONFIRMED Class 2 (effects set over all reviewer_events incl. stale; rerun C lands row 2).
L03-C1 CONFIRMED mechanism (audit_events: processing read only to fill from_state; to_state="audited_ai" unconditional; selection by claims). rerun parse_failed→audited_ai, analysis_ready True. production trigger not driven (needs live asserted claims on a failure-state paper).
L03-C2 CONFIRMED (monitor `if total_non_null == 0: status OK`; `distinct_count <= 1 and total_non_null >= collapsed_min_papers`). rerun 40×NR → OK. Pinned by test_all_nr_excluded → needs ruling (intended?).
L03-C3 CONFIRMED (append(ev.value) raw; Counter(values)). rerun.
L03-C4 CONFIRMED (low.append only after `if not todo: continue`; no reader in exporters/run_pipeline/adjudication by grep; CLAUDE.md "PRISMA-reported"). Also threshold = getattr(spec,"low_yield_threshold",4) literal default.
L03-C5 CONFIRMED (no read of spec.distribution_monitor anywhere; literals `< 10`).
L04-C1 CONFIRMED lines (cloud/base.py `build_extraction_prompt(parsed_text, self.spec)` no path; effective_config hashes with codebook_path; run_cloud_extraction `--db` + codebook beside db). conditional (--db outside data/<review>/; R71). kin to L05-C2.
L04-C2 CONFIRMED (git_state: no returncode read for rev-parse/status). rerun: corrupt index → dirty=False.
L04-C3 CONFIRMED (digests[model]=digest_fn(model) once; check_declared_call compares model name). extends A4-b. conditional.
L04-C4 CONFIRMED Class 2 (RELEASE then `if conn.in_transaction: conn.commit()`). rerun. no current caller holds txn.
L04-C5 CONFIRMED Class 2 lines (pin_tuple stages filtered by arm; named_arms includes `arms`). conditional, no caller passes arms= (C45).
L05-C1 CONFIRMED (eligibility_render decision_instruction: verifier stages no policy_for; `if stage != "abstract_primary": raise`; policy_for read at adjudication guidance (ADJUDICATES[stage]); ft_screener prompt has no policy render). rerun: changed policies → ft prompts identical, adjudication guidance differs. latent on live.
L05-C2 CONFIRMED (only `cb.review != review_id` in load_codebook_for; run paths use load_codebook/load_codebook_beside). MERGE with W1-F1-C6; kin L04-C1.
L05-C3 CONFIRMED (INVALID_SNIPPET_RE applied to snippet alone: locator bridged, auditor returns invalid_snippet before locate, extractor _has_invalid_snippet). rerun.
L05-C4 CONFIRMED (SEMANTIC_KEYS lacks canonical_absence_sentinel; used in prompts). weak 1; pin-affecting if fixed. extends C34.
L05-C5 CONFIRMED Class 2 (rerun: empty verifier tests render; ValueError at first abstract-less paper).
L05-C7 CONFIRMED Class 2 (no isinstance str check; str(s) for sentinels). rerun.
L07-C1 CONFIRMED (pymupdf/vision always append `<!-- Page N -->`; judge `if not stripped or (is_reroute and len<thr)`; rerun: 1-page marker-only verdict PASS, ≥2 pages SHATTERED). trigger conditional (blank per-page output).
L07-C2 CONFIRMED (short-circuit returns ParsedDocument w/o attempts; parse_all_pdfs `accepted = next(... , None)` → else update_status PARSED). extends B-F01 (12f saw the missing-file variant; this is the gate-FAILED variant).
L07-C3 CONFIRMED (outer `except BaseException: if attempts and not attempts_committed: _commit_attempts` → commit(); inner write handler is `except Exception`). extends C56 half B note in 12f_triage ("commits ledger rows on an interrupt") + B-F01.
L07-C4 CONFIRMED (re-route `except Exception: _record_error; break` → least-bad accepted → PDF_EXCLUDED PARSE_QUALITY). extends C56.
L07-C5 CONFIRMED Class 2 (_REROUTE keys: GLYPH_DENSITY, REPLACEMENT_DENSITY, SHATTERED, EMPTY_TEXT; CRITERIA includes FONT_EXPOSURE).
L07-C6 lines CONFIRMED (_DESCENDANT regex); consequence conditional, NOT established on any PDF — rests on reader for the trace-name join.
L07-C7 CONFIRMED docstring vs resolver greatest version; latent (no production caller per reader — not re-checked beyond grep below).
L07-C8 CONFIRMED Class 2 (strip_links_to_temp call inside except handler w/o try; mkstemp no unlink on error).
L07-C9 CONFIRMED Class 2 (initial scanned route calls parse_with_vision w/o cap; cap only in re-route loop).
L07-C10 CONFIRMED Class 2 (no-PDF branch: warning + stats, before try). extends D20.
W1-F1-C1 CONFIRMED (extractor _extract_selected: the try covers progress.report/logger after commit; except → record_failure extract_pass2). rerun (fault injected). real-world trigger not established.
W1-F1-C2 CONFIRMED lines (`last_error = exc … raise last_error`; attempts=attempt or 1). rerun with stubs. policy question; extends A-11. weak.
W1-F1-C3 CONFIRMED rerun → MERGE with L02-C3.
W1-F2-C1 CONFIRMED (runner.run: only gate is `kind == "data" and not include_data`; no freshness test, no id; __main__ help "Never applied on a fresh database regardless of this flag"; CLAUDE.md "never executed on a fresh database"). rerun on fake migrations.
W1-F2-C2 CONFIRMED (`except sqlite3.OperationalError: return {}` in receipts). rerun: under lock receipts {} and check_drift []. NOTE: bears on ruling assumption A1 (MigrationDrift refusal) — drift check is defeated when the receipt read faults.
W1-F2-C3 CONFIRMED Class 2 (module.run_migration then _write_receipt separate; only run_migration inside MigrationError wrapper).
