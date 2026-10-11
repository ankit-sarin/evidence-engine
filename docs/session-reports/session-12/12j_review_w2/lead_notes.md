# 12j-REVIEW-W2 — lead notes (W2-c): how each Class 1 / 2 candidate was re-derived

Method: for each candidate the lead read the deciding lines in the file at HEAD `0e3b823` (grep / sed
on the named function) and, where the reader wrote a reproducer, reran it from the reader's
directory with `PYTHONPATH` set to the repository. Rerun outputs: `lead_rerun/*.out` (stdout).
21 reproducer scripts, all exit 0: 19 outputs byte-identical to the readers'; 2 differ only in
timestamps and a timestamped session name (`L08_2_bypass_record_overwritten`,
`L11_1_dash_m_relaunch`). The lead did not re-argue a reader's search for a ruling that accepts
a behaviour as built. Class 3 rows rest on the reader's quote.

L08-C1/C2 CONFIRMED rerun. Both importers: `except Exception as exc: conn.rollback(); rm.close_run(...)` — an interrupt skips it; `workflow.ensure_workflow_table` ends in `conn.commit()`.
L08-C3 CONFIRMED rerun. `bypass_stage` and `complete_stage` are each one `UPDATE workflow_state SET status = …, completed_at = ?, metadata = ?`; the bypass note lives in `metadata`.
L08-C4 CONFIRMED code. `advance_stage`: `if force and not can_advance_to(...)`: bypass; otherwise `complete_stage(conn, stage_name, metadata=note)`.
L08-C8 code only: grep finds no comparison of the spec's review id with the database in either importer. The condition (does `open_run` refuse) was NOT established by the reader or the lead.
L09a-C1 CONFIRMED rerun. `_find_col`: `if c.lower() in header.lower()` over `["PI_decision", "DECISION"]`, same in both adjudicators.
L09a-C2 CONFIRMED rerun. `_apply_abstract_decisions`: the INSERT into `abstract_screening_adjudication` precedes `if paper_row and paper_row["status"] == "ABSTRACT_SCREEN_FLAGGED":`.
L09a-C3 / L09b-C2 / L13-C1 MERGED (one defect family: a returned file is joined on a key alone). Rerun L09a_1, L09a_2, L13_1. `exportJSON` emits per-card records keyed by `data-pid`; the workbook builder has no protection, token or export id (grep: none).
L09a-C4 CONFIRMED rerun. `_import_abstract_json` validates `if not paper_id:` only.
L09a-C5 / L13-C4 MERGED. `title_val = row[title_col].value; if not title_val:` in both xlsx importers.
L09a-C7 / L09b-C8 MERGED. Both generators: `db_path = DATA_ROOT / review_name / "review.db"`.
L09a-C8 / L09b-C1 MERGED (pass 2 selected and dropped, two sites). Rerun L09b_1. `_query_abstract_flagged`: `ORDER BY pass_number` then `if r["pass_number"] == 1:`.
L09b-C3, C4, C9 code only: the page script was read in the template (`STORAGE_KEY = "…_adjudication_" + REVIEW`; the `keydown` branches call `setDecision(parseInt(cards[focusedIdx].dataset.pid), …)` with no `e.repeat` test; `}} catch(e) {{}}`). No JS runtime on the box; none of the three was executed.
L09b-C5 CONFIRMED code. `batchExclude` calls `setDecision(pid, "EXCLUDE")` per card; nothing marks the batch.
L09b-C6 CONFIRMED rerun. `p["abstract"][:400]`; `(p["abstract"] or "")[:300]`.
L10-C1 CONFIRMED rerun. `ee_part = ee_identifier or "EE-000"`; on a collision the rename is skipped (`continue`) and the later loop still writes `download_status = 'success'` with `pdf_dir / canon`.
L10-C2 CONFIRMED rerun. `_match_file_to_paper`: a bare integer stem → `paper_index`; `^(EE-\d+)` → `ee_index`.
L10-C3 / C4 CONFIRMED rerun. Selection `… OR p.pdf_quality_check_status != 'AI_CHECKED'`; the per-paper `except Exception as exc:` in `run_quality_check`.
L10-C5 CONFIRMED rerun. `is_complete = data.get("complete", False)` is logged, never a condition; no read of `review` or `mode`.
L10-C6 CONFIRMED code: no `open_run` in `pdf_quality_check.py` (grep).
L10-C7 CONFIRMED rerun. The page has `<option value="ACQUIRED">`; it is not in `VALID_DISPOSITIONS`.
L10-C8 / C9 CONFIRMED code. `return {"oa_status": f"error:{exc}", …}`; selection `AND oa_status IS NULL`; one `conn.commit()` after the loop.
L10-C10 / C11 CONFIRMED code. `if not title: return None`; `" AND ".join(spec.search_strategy.query_terms)`. Whether NCBI caps the paged fetch was not established (needs a network call).
L11-C1 code: `_get_ollama_env` runs `systemctl show ollama --property=Environment` (the unit's configured value). The trigger is conditional.
L11-C2 CONFIRMED rerun. `cmd_parts = [sys.executable] + sys.argv`.
L11-C3 code: the relaunch passes argv only. The consequence rests on tmux's documented behaviour (not run; no tmux server was running on the box when checked).
L11-C4 CONFIRMED code. `max_retries=0, wall_timeout=60.0`.
L11-C9 code: `print(line, file=self.file, flush=True)`, unguarded. The consequence is Wave 1 row 8's.
L12-C1, C2, C5, C6, C7, C8 CONFIRMED rerun (API keys unset in the lead's environment; stubbed transports). `parse_response_to_spans`: `span["value"] = self._codebook.canonical_absence_sentinel; span["source_snippet"] = ""` when `span.get("value") is None` — which a missing key also satisfies — and `span["value"] = str(span["value"])` for a non-string.
L12-C3 CONFIRMED code. `if … "source_snippet" not in span: span["source_snippet"] = ""`; no citation guard on the path.
L12-C4 CONFIRMED code. `send`: `rm.record_call(…, outcome="completed")` after the call returns, inside `if self.run_id is not None:`.
L12-C12 code: `self.run_id: int | None = None`; the opt-in check is in the script.
L13-C2 / C5 CONFIRMED rerun. `f"{authors[0].split()[-1]} et al."`; `cell.text = str(val) if val else ""`.
L14-C1 … C5 CONFIRMED rerun (one script). `align_arms`: `val_a = fields_a.get(fname)` over the union of the two arms' fields for shared papers only; `_tokens`: `re.findall(r"[0-9a-z]+", …)`; `parse_number` discards the unit word on every field; the multi-value branch returns a `set`.
L14-C9 CONFIRMED code. `except Exception: return None` in `_get_field_def`; `except Exception: return False` in `_is_categorical`.
