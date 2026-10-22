#!/bin/bash
# 12u-U16A — every anchor the read-out quotes, located by content (fixed-string grep), at HEAD.
# usage, from the repository root: bash anchors_u16a.sh   (stdout: one block per anchor)
a() { echo "== $1"; grep -nF -- "$3" "$2" | head -3 | sed "s#^#$2:#"; }
a U16-advance       scripts/run_pipeline.py 'if audited > 0:'
a U16-advance-call  scripts/run_pipeline.py 'complete_stage(conn, "AI_AUDIT_COMPLETE_STAGE",'
a U16-swallow       scripts/run_pipeline.py 'pass  # workflow table may not exist'
a U16-spec-arm      scripts/run_pipeline.py 'report = audit_run(db._conn, spec, run_id=run_id, arm=spec.extraction_models.arm,'
a U16-nothing-todo  engine/agents/audit_events.py 'claims = live_claim_events(conn, pid, arm, event_types=("asserted",))'
a U16-text-refusal  engine/agents/audit_events.py 'logger.warning("Paper %d: audit skipped — %s; parsed text uid(s) %s (%s)",'
a U16-stage-doc     engine/adjudication/workflow.py 'AI_AUDIT_COMPLETE_STAGE   — auto: run_pipeline, once any corpus paper is audited_ai'
a sel-held          engine/core/selection.py 'held = reuse_key(arm, paper_id, ref.sha256) in _live_reuse_keys(conn, paper_id, arm)'
a sel-reported      engine/core/selection.py 'unreadable.append((paper_id, exc.reason_code, ref.stored_path))'
a sel-refused       engine/core/selection.py 'skipped_refused.append((paper_id, exc.reason_code))'
a key-scheme        engine/core/reuse_key.py '`(arm, paper_id, parsed_text_sha256)`.'
a resolver-current  engine/core/parsed_text.py 'ORDER BY parsed_text_version DESC LIMIT 1'
a resolver-verify   engine/core/parsed_text.py 'if observed != ref.sha256:'
a loop-reread       engine/agents/extractor.py 'paper_text = read_parsed_text(ref)'
a loop-refusal      engine/agents/extractor.py 'record_failure(pid, exc, "extract_pass1")        # R122: event, no abort count'
a refusal-once      engine/agents/extractor.py 'if (state.processing, state.processing_reason) == ("extraction_failed", reason_code):'
a refusal-telemetry engine/agents/extractor.py 'kind="claimed_papers_text_unreadable",'
a d24-current       engine/core/extraction_events.py 'current_text = resolve_parsed_text(conn, outcome.paper_id)'
a d24-noref         engine/core/extraction_events.py 'if key is None:'
a d24-stale         engine/core/extraction_events.py 'if ev.payload.get(PAYLOAD_REUSE_KEY) != key}'
a sel-claim-inputs  engine/core/selection.py 'f"JOIN claim_inputs ci ON fe.extraction_uid = ci.extraction_uid "'
a audit-by-uid      engine/agents/audit_events.py 'return resolve_parsed_text_by_uid(conn, uids[0])'
a audit-admitted    engine/agents/audit_events.py 'if processing not in AUDIT_ADMITTED_STATES:'
a parse-versioned   engine/parsers/pdf_parser.py 'md_filename = f"{paper_id}_v{version}.md"'
a parse-noclobber   engine/parsers/pdf_parser.py 'A parse never'
a parse-stage-sel   scripts/run_pipeline.py 'pdf_acquired = db.get_papers_by_status("PDF_ACQUIRED")'
a reparse-entry     engine/parsers/pdf_parser.py 'def reparse_papers('
a import-nolink     engine/adjudication/import_extraction_entry.py 'os.link(tmp, md_path)          # FileExistsError if md_path exists (R-l, R-k)'
a legacy-no-decl    engine/core/extraction_events.py 'elif state == escape_token:'
a spec-elicitation  review_specs/surgical_autonomy.yaml 'elicitation: false'
a prisma-included   engine/exporters/prisma.py 'if state.processing == "audited_ai":'
a ready             engine/core/effective.py 'and self.processing in COMPLETED_PROCESSING_STATES)'
echo "== callers of audit_run / _advance_extraction_workflow (engine, scripts, analysis)"
grep -rn "audit_run(\|_advance_extraction_workflow(" engine scripts analysis --include=*.py | grep -v "def audit_run\|def _advance_extraction_workflow"
echo "== callers of reparse_papers (engine, scripts, analysis)"
grep -rn "reparse_papers(" engine scripts analysis --include=*.py | grep -v "def reparse_papers" || echo "(none)"
echo "== legacy_record's body: outcome kinds it constructs (DECLINED expected absent)"
awk '/^def legacy_record/{f=1;next} /^def /{f=0} f' engine/core/extraction_events.py | grep -oE "FieldOutcome\(name, [A-Z_]+|_value\(" | sort | uniq -c
