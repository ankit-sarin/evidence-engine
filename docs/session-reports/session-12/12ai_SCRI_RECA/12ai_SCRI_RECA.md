# 12ai-SCRI-RECA — publication census of ~/scratch/12ai-scri/ (read-only; to scratch, not committed)

Written 2026-11-19T16:48:37Z. SRC `/home/ankitsarin/scratch/12ai-scri`, its list `SHA256SUMS` 1d834e29bd0b10a05bc536677f481f5b4cd6963850bfffdc3208d5317ab074d2. Every count below is a helper's; a classification by reading is marked CC's reading.

Helpers (each run with `python3 -I -B`):

- `/home/ankitsarin/scratch/12ai-scri-reca/helpers/assemble.py` sha256 `336ea520c1d1dbd53ad5daf60578724e1657c0a31813e678dce277f406ee659a`
- `/home/ankitsarin/scratch/12ai-scri-reca/helpers/census.py` sha256 `3f2a6f2cfe6bc2e6d8bcad1db6cdafa318c5be61d63ec4302432720d981d8ff4`
- `/home/ankitsarin/scratch/12ai-scri-reca/helpers/check_item0_reca.py` sha256 `452690fc20651aebea5ff34b25f3a9c817e0b174afbdde10aa2dd6c030f2aa9c`
- `/home/ankitsarin/scratch/12ai-scri-reca/helpers/control.py` sha256 `b8ee4bf05a2d086a6e5bb6926e101385e50b09a1f0fc032e8a5e9154fb5015ba`
- `/home/ankitsarin/scratch/12ai-scri-reca/helpers/form_compare.py` sha256 `04edeee8c96b7c480a1b20fda49bb803d4c44f6f5c7d723ec10bc1d44b32a177`
- `/home/ankitsarin/scratch/12ai-scri-reca/helpers/kinds.py` sha256 `7e1e832bdce4dbb6b32ec29235d5849a199c8166cc33afffc7c64f6201357a26`
- `/home/ankitsarin/projects/evidence-engine/docs/session-reports/session-12/12ah_OLRA/rec_scan.py` sha256 `14328ba6ba6c8c593b8c8039c7f88dc89ca44acdfc7d67dabe69c4621a86080b` (the scan, 2b; unedited)
- `/home/ankitsarin/scratch/12ai-open/helpers/scri_check.py` sha256 `f040822cb33378a468538a222025488fbc0c313ab71606a3ac004015fa75faf7` (2a)

Tables beside this file: `3a_names_by_file.tsv`, `3b_distinct_paths.tsv`, `3b_paths_by_group_file.tsv`, `3c_citations_by_source.tsv`, `3c_scri_md_refs.tsv`, `3d_s4_records_with_committed_copy_entries.tsv`, `3d_s4_records_with_committed_copy_kinds.tsv`, `3d_s4_records_without_committed_copy_entries.tsv`, `3d_s4_records_without_committed_copy_kinds.tsv`, `3e_mainstatus_ignored.tsv`, `3e_untracked_ignored_names.tsv`, `3f_patch_numstat.tsv`, `3g_groups.tsv`, `3h_evidence_listed.tsv`.

## Step 2 — scan and control

```
PASS scanner exit 1 on the fixture (stop-class hits present)
CONTROL: PASS
RESULT: stop-class hits 0; reading-class hits 10; files 97
```

## 3a — other lanes' scratch entries

s1_toplevel.tsv header: `entry⇥files⇥bytes⇥newest_mtime_utc⇥in_scope`. The column `in_scope` marks scope: `yes` on 161 rows, `no (name and size only)` on 22.

The 22 out-of-scope entries, in full, once: `brand-theme-B`, `brand-theme-C`, `brand-theme-D`, `brand-theme-E`, `brand-theme-F`, `cm-read-1`, `cm105a`, `cm106a`, `cm106b`, `cm106b_failfirst_before.txt`, `cm106b_step0.marker`, `cm106b_step0_mtimes.txt`, `cm106d`, `council-A`, `dgx-infra-OHC-1`, `helloai-A`, `inv-a`, `inv-b`, `lists-A`, `llm-council-LCH-1`, `ollama-OLR-1`, `opreports-A`.

Also counted: the four home folders named in the committed 12ai-OPEN brief (`~/ebr-scratch`, `~/fd2-4-scratch`, `~/fd2-d2-scratch`, `~/phi-scratch`), and two prefix forms.

| file | lines naming each (name: lines) |
|---|---|
| 12ai_SCRI.md | council-A: 1; dgx-infra-OHC-1: 1; helloai-A: 1; inv-a: 1; lists-A: 1; brand-theme-* (prefix form): 1; cm* (prefix form): 1; cm-read-1: 1; cm106b: 1; llm-council-LCH-1: 1; ollama-OLR-1: 1; opreports-A: 1 |
| g1_after.tsv | brand-theme-B: 1; brand-theme-C: 1; brand-theme-D: 1; brand-theme-E: 1; brand-theme-F: 1; cm-read-1: 1; cm105a: 1; cm106a: 1; cm106b: 1; cm106b_failfirst_before.txt: 1; cm106b_step0.marker: 1; cm106b_step0_mtimes.txt: 1; cm106d: 1; council-A: 1; dgx-infra-OHC-1: 1; helloai-A: 1; inv-a: 1; inv-b: 1; lists-A: 1; llm-council-LCH-1: 1; ollama-OLR-1: 1; opreports-A: 1 |
| g1_before.tsv | brand-theme-B: 1; brand-theme-C: 1; brand-theme-D: 1; brand-theme-E: 1; brand-theme-F: 1; cm-read-1: 1; cm105a: 1; cm106a: 1; cm106b: 1; cm106b_failfirst_before.txt: 1; cm106b_step0.marker: 1; cm106b_step0_mtimes.txt: 1; cm106d: 1; council-A: 1; dgx-infra-OHC-1: 1; helloai-A: 1; inv-a: 1; inv-b: 1; lists-A: 1; llm-council-LCH-1: 1; ollama-OLR-1: 1; opreports-A: 1 |
| s1_toplevel.tsv | brand-theme-B: 1; brand-theme-C: 1; brand-theme-D: 1; brand-theme-E: 1; brand-theme-F: 1; cm-read-1: 1; cm105a: 1; cm106a: 1; cm106b: 1; cm106b_failfirst_before.txt: 1; cm106b_step0.marker: 1; cm106b_step0_mtimes.txt: 1; cm106d: 1; council-A: 1; dgx-infra-OHC-1: 1; helloai-A: 1; inv-a: 1; inv-b: 1; lists-A: 1; llm-council-LCH-1: 1; ollama-OLR-1: 1; opreports-A: 1 |

Files naming an out-of-scope entry by its full name: 4: 12ai_SCRI.md, g1_after.tsv, g1_before.tsv, s1_toplevel.tsv. Files naming a home folder: none. Table: `3a_names_by_file.tsv`.

## 3b — paths outside this lane

Every absolute or `~/` path not under `~/projects/evidence-engine` or an in-scope scratch entry; `~` read as `/home/ankitsarin`; grouped by the first three components. Per group: total occurrences, files (count, example line). A match after `)`, `*`, `}` or `…` is a fragment of a relative path and is not counted; a file that is not valid UTF-8 is not searched.

| group | occurrences | files: paths (example line) |
|---|---|---|
| `/api/ps` | 4 | patches/12ae-close__pre_clone.patch: 1 (:50); patches/12r-open__pc_clone.patch: 1 (:44); patches/12s-close__clone2.patch: 1 (:93); patches/12s-open__clone.patch: 1 (:39) |
| `/api/tags` | 1 | patches/12s-close__clone2.patch: 1 (:93) |
| `/api/version` | 1 | patches/12s-close__clone2.patch: 1 (:93) |
| `/dev/null` | 1 | patches/12r-open__pc_clone.patch: 1 (:88) |
| `/exit` | 1 | patches/12ag-n46a__regen_clone.patch: 1 (:6) |
| `/home/ankitsarin/claude-config` | 6 | patches/12ag-n46a__regen_clone.patch: 1 (:6); patches/12r-open__pc_clone.patch: 3 (:35); patches/12s-open__clone.patch: 2 (:49) |
| `/home/ankitsarin/claude-session-archive` | 1 | patches/12ag-n46a__regen_clone.patch: 1 (:31) |
| `/home/ankitsarin/scratch` | 10 | 12ai_SCRI.md: 1 (:79); s2_citations.tsv: 9 (:28) |
| `/proc/locks` | 1 | patches/12r-open__pc_clone.patch: 1 (:15) |
| `/wrap` | 11 | patches/12ag-n46a__regen_clone.patch: 3 (:5); patches/12r-open__pc_clone.patch: 4 (:57); patches/12s-open__clone.patch: 4 (:58) |

Distinct paths per group, in full:

- `/api/ps` (1): `/api/ps`
- `/api/tags` (1): `/api/tags`
- `/api/version` (1): `/api/version`
- `/dev/null` (1): `/dev/null`
- `/exit` (1): `/exit`
- `/home/ankitsarin/claude-config` (1): `/home/ankitsarin/claude-config/primer.md`
- `/home/ankitsarin/claude-session-archive` (1): `/home/ankitsarin/claude-session-archive/`
- `/home/ankitsarin/scratch` (8): `/home/ankitsarin/scratch/`, `/home/ankitsarin/scratch/backup-review-10b/`, `/home/ankitsarin/scratch/ee_smoke_11c_input/`, `/home/ankitsarin/scratch/p3ii-fresh/fresh_p3ii/review.db`, `/home/ankitsarin/scratch/rehearsal-10b-2/fresh/fresh10b2/review.db`, `/home/ankitsarin/scratch/rehearsal-10b-2/source_copy.db`, `/home/ankitsarin/scratch/rehearsal-10b/fresh/fresh10b/review.db`, `/home/ankitsarin/scratch/rehearsal-10b/source_copy.db`
- `/proc/locks` (1): `/proc/locks`
- `/wrap` (1): `/wrap`

Tables: `3b_paths_by_group_file.tsv`, `3b_distinct_paths.tsv`.

## 3c — citations and quoted text from files this repository does not track

s2_citations.tsv: 121 lines; header lines: `# label = CC reading (LEDGER / CONSUMER / HISTORY), not a computed value; CONSUMER takes precedence where a ledger row also governs (ledger_row column filled)` / `source_file:line⇥cited_path⇥exists_now⇥label_reading⇥ledger_row⇥reader`; data rows 119.
Columns: `source_file:line`, `cited_path`, `exists_now`, `label_reading`, `ledger_row`, `reader`. A column carrying the cited line's text: none; every row carries the line NUMBER in `source_file:line`, and the path the line cites in `cited_path`, not the line's text. Bytes of cited-line text: 0.
Distinct `cited_path` values: 93 (the read-out's S2 gives "119 citations of 93 distinct paths").

| source file | rows | tracked at HEAD | carries | bytes of `cited_path` in its rows |
|---|---|---|---|---|
| docs/plan/ENGINE_REFACTOR_PLAN.md | 49 | yes | line number + cited path | 1348 |
| docs/plan/STANDING_RULES.md | 4 | yes | line number + cited path | 92 |
| docs/session-reports/session-12/12ah_close/retention_close.txt | 19 | yes | line number + cited path | 1104 |
| primer.md | 47 | no | line number + cited path | 1383 |

12ai_SCRI.md, every line that references primer.md, CLAUDE.md, primer-history/ or another repository's file (helper: `3c_scri_md_refs.tsv`; whether a line QUOTES is the helper's test, a double-quoted span of 3+ characters or a fenced block):

| line | file referenced | tracked at HEAD | quotes text |
|---|---|---|---|
| 37 | primer.md (this repo) | no (ignored) | number/name only |
| 37 | CLAUDE.md (this repo) | yes | number/name only |
| 37 | surgical-cv primer.md | no (another repository) | number/name only |
| 37 | surgical-cv CLAUDE.md | no (another repository) | number/name only |
| 37 | claude-config primer.md | no (another repository) | number/name only |
| 53 | primer.md (this repo) | no (ignored) | number/name only |
| 53 | CLAUDE.md (this repo) | yes | number/name only |
| 80 | primer.md (this repo) | no (ignored) | number/name only |
| 142 | primer.md (this repo) | no (ignored) | number/name only |
| 144 | fence open |  |  |
| 152 | fence close |  |  |
| 154 | primer.md (this repo) | no (ignored) | quote |
| 155 | primer-history/… | no | quote |
| 171 | CLAUDE.md (this repo) | yes | number/name only |
| 174 | CLAUDE.md (this repo) | yes | number/name only |
| 179 | primer.md (this repo) | no (ignored) | number/name only |
| 198 | primer.md (this repo) | no (ignored) | quote |
| 218 | primer.md (this repo) | no (ignored) | number/name only |

## 3d — the two large tables

### s4_records_with_committed_copy.tsv

- 2508415 B; 12960 rows + 1 header line; header `path⇥bytes⇥sha256⇥committed_path_at_HEAD`; columns ['path', 'bytes', 'sha256', 'committed_path_at_HEAD'].
- Distinct top-level entries: 154 (rows per entry: `3d_s4_records_with_committed_copy_entries.tsv`); largest: 12y-u18c 2780, 12w-v1a 2110, 12aa-u20c 1062, 12w-u17b 896, 12z-u19b 800, 12f 743, 12aa-u20b 354, 12u-u16c 317.
- Rows whose path is not under an in-scope entry (E3): 0.
- Rows by kind (CC's rule, `kinds.py`; s4_unique_by_kind.tsv's names): json 751 / 21072763 B; logs 798 / 46342091 B; other 331 / 9766216 B; paper texts 6 / 0 B; pytest basetemp content 5781 / 190830900 B; scripts 1838 / 14919945 B; sqlite db/sidecar/bak 82 / 0 B; text/md/tsv 3373 / 101148346 B.
- The first five rows, whole:
```
12aa-close/lane_line_12aa.txt	6446	5a752f9907ccbc7236de0a46fe0bc464498255fd3f929b3ecf53ee7bd007723e	docs/session-reports/session-12/12aa_close/lane_line_12aa.txt
12aa-close/dbchecks.err	0	e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855	analysis/__init__.py
12aa-close/pins.err	0	e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855	analysis/__init__.py
12aa-close/recount.err	0	e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855	analysis/__init__.py
12aa-close/recount_run.json	1679	569584d105a9609e3e6fd823d20cf03ab47d42ab82c38fe4611547d84b49afb0	docs/session-reports/session-12/12aa_close/recount_close_run.json
```
- Ten rows drawn with `random.Random(20261119).sample(range(rows), 10)`, whole (row index from 0 after the header):
```
[338] 12aa-u20b/bt_coupled/test_t4_each_main_installs_the3/intr/extraction_codebook.yaml	36732	89dbfa91b5033f2b3dff713e36c7ecd0aa086739b5832059948b6f55b29ead82	data/surgical_autonomy/extraction_codebook.yaml
[3551] 12i-w1/repro/L05/L05_6_ellipsis_regex.out	198	dc6a4cf6a4ab67392f86ef190dbd38178ccf215f9919df00ef77e342f4fced04	docs/session-reports/session-12/12i_review_w1/lead_rerun/L05_6_ellipsis_regex.out
[4160] 12q-close/pins_journal.txt	17	19c4e28b2f54ea8f217db4e622b0a5c758efa7b77ce2dc0394b6d159568c0b2e	docs/session-reports/session-12/12ag_GATEUNIT_B/journal/ollama_journal_window.txt
[7880] 12w-v1a/r2/basetemp/C_V1_supp/test_t6_an_input_that_does_not0/xev/extraction_codebook.yaml	36732	89dbfa91b5033f2b3dff713e36c7ecd0aa086739b5832059948b6f55b29ead82	data/surgical_autonomy/extraction_codebook.yaml
[8462] 12w-v1a/r2/basetemp/C_supp/test_b10_a_failure_at_the_pape0/xev/extraction_codebook.yaml	36732	89dbfa91b5033f2b3dff713e36c7ecd0aa086739b5832059948b6f55b29ead82	data/surgical_autonomy/extraction_codebook.yaml
[9268] 12y-u18c/parent_tree/tests/test_parse_failures.py	12581	f09312044a2fca801f867f1b391106d3b1db5679e5319d77c9bbe57d7b8c6e42	tests/test_parse_failures.py
[9997] 12y-u18c/parent_tree/docs/session-reports/session-12/12v_U38A/r699.txt	4700	8f1513c746db5db4c0da0950657f1a66381b3c922b22a7fa37f7844e787c5d42	docs/session-reports/session-12/12v_U38A/r699.txt
[11553] 12y-u18c/parent_tree/docs/session-reports/session-12/12n_U12_C2A/import_closure_control.py	2250	25ffc99e6c59ae1b0238f3e777723a06a9ba6a443dcd095c4482f56c408f51c2	docs/session-reports/session-12/12n_U12_C2A/import_closure_control.py
[11706] 12y-u18c/parent_tree/docs/session-reports/screen-auth-2g/full/arm_D/worker_verifier.log	15955	65beb4afa50dadf24394ae8005f77aedcd9c1b7be442fdecc53452c24eec423f	docs/session-reports/screen-auth-2g/full/arm_D/worker_verifier.log
[12612] 12z-u19b/bt/coupled3/test_run_faults_leave_the_extr17/runfault/extraction_codebook.yaml	36732	89dbfa91b5033f2b3dff713e36c7ecd0aa086739b5832059948b6f55b29ead82	data/surgical_autonomy/extraction_codebook.yaml
```
- Paper-text names (CC's broad rule: under parsed_text/ or texts/, or .pdf): 6 rows, 3 distinct names; twenty drawn with `random.Random(20261119)`, whole: `1.pdf`, `empty.pdf`, `t1.md`.
- Paths holding an e-mail address: 0. Paths with patient, phi or mrn as a whole component (any case, extension ignored): 0. Wider reading, the word as a token anywhere in the path (split on non-alphanumerics): 0.

### s4_records_without_committed_copy.tsv

- 4378258 B; 30462 rows + 1 header line; header `path⇥bytes⇥sha256`; columns ['path', 'bytes', 'sha256'].
- Distinct top-level entries: 159 (rows per entry: `3d_s4_records_without_committed_copy_entries.tsv`); largest: 12w-v1a 8802, 12aa-u20c 6227, 12w-u17b 5789, 12z-u19b 1482, 12aa-u20b 1108, 12u-u16c 640, 12v-u38b 602, 12y-u18c 504.
- Rows whose path is not under an in-scope entry (E3): 0.
- Rows by kind (CC's rule, `kinds.py`; s4_unique_by_kind.tsv's names): json 230 / 12464901 B; logs 126 / 21196261 B; other 205 / 7799461 B; paper texts 11975 / 31149663 B; patches 11 / 206907 B; pytest basetemp content 7557 / 28141529 B; scripts 745 / 7479616 B; sqlite db/sidecar/bak 7218 / 5166967080 B; text/md/tsv 2395 / 406300822 B.
- The first five rows, whole:
```
10b-close/10b-close-uncommitted.patch	51946	87ff924e77f952fe1580558b34b938cce20af096a9463764262387abd6020499
10b-close/closure.md	12999	80f1e28f5988962f946d06ea982c88bfa5dbf96098f529e5ca7cc42f8b8c0aa5
10b-close/smoke10b_tree_after.sha256	2640	9741dc9cd3c98229d3ebf34d5a276c11a9920a983ad2f8b927e7b6e08e610f1b
10b-close/smoke10b_fingerprint_before_move.json	77898	a016855317fe1177e6bbc05d6ee3276d55525161036139a4de9d7aabe1dc40a4
10b-close/retire.txt	2897	27e884677616f0711f9875521a8115d624ae813cac69901974bf078e347cb671
```
- Ten rows drawn with `random.Random(20261119).sample(range(rows), 10)`, whole (row index from 0 after the header):
```
[677] 12aa-u20b/bt_coupled/test_run_faults_leave_the_extr15/runfault/review.db	425984	dec804e7d2e6770dfdd0d112d3632732c9c9e39d0b2a17cc61db003013ddab7b
[7102] 12aa-u20c/bt_c/test_the_judges_full_text_path0/ollama.service	38	94c24fd62a1cd5a0a483bb0a453dcb2b1f2efe6790551d69df2e89711e91710c
[15760] 12w-u17b/census/basetemp/test_digital_clean_docling_nev0/test_ocr/review.db	421888	332d470e12c2a9ac81e45d462db713311c24062c2f1c4266aeb4a09fc84d054a
[16924] 12w-u17b/census/basetemp/test_a_fresh_database_carries_0/m020/extraction_codebook.yaml	508	92ab79d204e0133bf2958994f6a77c2d0d2992f0d0674d644b35bd9ecd66dd4d
[19995] 12w-v1a/basetemp/head_supp/test_t7_each_a14_refusal_is_ex1/xev/review.db	425984	1ef5fc1df8b370c99a577d4a4501b0b1c4d59ff63e8380660e1e8ef136293246
[23106] 12w-v1a/r2/basetemp/A_V2p_supp/test_a_duplicate_on_attempt_1_1/dup/telemetry/extraction_calls.jsonl	10387	ec41cb2e2f9973f661bd1c3ebed04dee349d3591f0e04aa39c47ea6798b318e2
[23413] 12w-v1a/r2/basetemp/head_supp/test_run_faults_leave_the_extr15/runfault/parsed_text/7_v1.md	45	2f9d7d0566f96acac89fa8e6a50d51e79ffb36657982c97b33eaaffb565e2d33
[25224] 12w-v1a/r2/basetemp/C_V2p_supp/test_a_record_under_the_floor_1/floor/parsed_text/9_v1.md	45	2f9d7d0566f96acac89fa8e6a50d51e79ffb36657982c97b33eaaffb565e2d33
[26020] 12w-v1a/r2/basetemp/B_V1_supp/test_run_faults_leave_the_extr8/runfault/parsed_text/7_v1.md	45	2f9d7d0566f96acac89fa8e6a50d51e79ffb36657982c97b33eaaffb565e2d33
[27686] 12x-close/build/build_item0.py	3245	52cc53b961bb6f0d18c66e40f43367969b31d855c2328c74f302d4b91ee18c67
```
- Paper-text names (CC's broad rule: under parsed_text/ or texts/, or .pdf): 11975 rows, 253 distinct names; twenty drawn with `random.Random(20261119)`, whole: `06_live748_v2.md`, `1004_v1.md`, `1041_v1.md`, `1050_v1.md`, `1055_v1.md`, `17_v1.md`, `28_v1.md`, `35_v1.md`, `41_v1.md`, `4_v2.md`, `5_v1.md`, `74_v1.md`, `77_v1.md`, `799_v1.md`, `83_v1.md`, `89_v1.md`, `99_v1.md`, `EE-001_Lukas_2020.pdf`, `paper.pdf`, `t2.md`.
- Paths holding an e-mail address: 0. Paths with patient, phi or mrn as a whole component (any case, extension ignored): 0. Wider reading, the word as a token anywhere in the path (split on non-alphanumerics): 0.

s4_unique_by_kind.tsv against CC's rule on s4_records_without_committed_copy.tsv:

| kind | table files / bytes | CC's rule files / bytes |
|---|---|---|
| sqlite db/sidecar/bak | 7218 / 5166967080 | 7218 / 5166967080 |
| text/md/tsv | 2423 / 409517123 | 2395 / 406300822 |
| pytest basetemp content | 16492 / 39682411 | 7557 / 28141529 |
| logs | 98 / 17970207 | 126 / 21196261 |
| paper texts | 2103 / 16590031 | 11975 / 31149663 |
| json | 634 / 13960002 | 230 / 12464901 |
| other | 708 / 9321741 | 205 / 7799461 |
| scripts | 775 / 7490738 | 745 / 7479616 |
| patches | 11 / 206907 | 11 / 206907 |

## 3e — the ignored and untracked names

g1_mainstatus_before.txt: 102 lines; 102 with the `!!` mark; other lines: 0. Every `!!` line, whole:
```
!! .claude/
!! .pytest_cache/
!! .venv/
!! analysis/__pycache__/
!! analysis/eval/__pycache__/
!! analysis/eval/elicit01/__pycache__/
!! analysis/eval/parse01/__pycache__/
!! analysis/paper1/__pycache__/
!! analysis/paper1/logs/
!! analysis/paper1/reports/judge_codebook_smoke_20260603T111714Z.md
!! analysis/paper1/reports/judge_codebook_smoke_20260603T111714Z_checkpoint.json
!! analysis/paper1/reports/judge_codebook_smoke_20260603T111714Z_results.json
!! analysis/paper1/reports/judge_codebook_smoke_20260603T144634Z.md
!! analysis/paper1/reports/judge_codebook_smoke_20260603T144634Z_checkpoint.json
!! analysis/paper1/reports/judge_codebook_smoke_20260603T144634Z_results.json
!! analysis/paper1/reports/judge_codebook_smoke_20260603T180638Z.md
!! analysis/paper1/reports/judge_codebook_smoke_20260603T180638Z_checkpoint.json
!! analysis/paper1/reports/judge_codebook_smoke_20260603T180638Z_results.json
!! analysis/provenance/__pycache__/
!! artifacts/paper1/pi_audit/pi_audit_key_2026-04-22T18-35-42Z.xlsx
!! artifacts/paper1/pi_audit/pi_audit_results_2026-04-22T19-05-36Z.json
!! artifacts/paper1/pi_audit/pi_audit_results_2026-04-22T19-05-36Z.xlsx
!! artifacts/paper1/pi_audit/pi_audit_workbook_2026-04-22T18-35-42Z.xlsx
!! "artifacts/paper1/pi_audit/pi_audit_workbook_2026-04-22T19-05-36Z COMPLETED.xlsx"
!! artifacts/paper1/pi_audit_v2/
!! data/.gitkeep
!! data/backups/
!! data/surgical_autonomy/Extraction_Codebook_v2.md
!! data/surgical_autonomy/Extraction_Workbook_v2_A.xlsx
!! data/surgical_autonomy/adjudication/
!! data/surgical_autonomy/adjudication_categories.yaml
!! data/surgical_autonomy/analysis/
!! data/surgical_autonomy/auditor_eval_results.json
!! data/surgical_autonomy/concordance_pdfs/
!! data/surgical_autonomy/docling_parse.log
!! data/surgical_autonomy/eval/
!! data/surgical_autonomy/expanded_search/
!! data/surgical_autonomy/exports/
!! data/surgical_autonomy/extract_log.txt
!! data/surgical_autonomy/extraction_audit_queue.html
!! data/surgical_autonomy/extraction_test_artifacts/
!! data/surgical_autonomy/extraction_v2_log.txt
!! data/surgical_autonomy/ft_adjudication_queue.html
!! data/surgical_autonomy/ft_adjudication_queue.json
!! data/surgical_autonomy/ft_screening.log
!! data/surgical_autonomy/logs/
!! data/surgical_autonomy/parse_log.txt
!! data/surgical_autonomy/parse_resume.log
!! data/surgical_autonomy/parsed_text/
!! data/surgical_autonomy/pdf_acquisition/
!! data/surgical_autonomy/pdf_quality_check.log
!! data/surgical_autonomy/pdfs/
!! data/surgical_autonomy/q8_validation_results.json
!! data/surgical_autonomy/reextract_run.log
!! data/surgical_autonomy/review.db
!! data/surgical_autonomy/review.db-shm
!! data/surgical_autonomy/review.db-wal
!! data/surgical_autonomy/review.db.bak-input-identity-01-phase3-pre-write-20260924-204138
!! data/surgical_autonomy/review.db.bak-manifest-01-phase3-pre-write-20260923-161020
!! data/surgical_autonomy/review.db.bak-migration-02-phase3-pre-write-20260929-172104
!! data/surgical_autonomy/review.db.bak-pre-migrations01-20260921-002807
!! data/surgical_autonomy/review.db.bak-readers-01-phase3-pre-write-20260922-165453
!! data/surgical_autonomy/review_backup_v1_schema.db
!! data/surgical_autonomy/review_backup_v1_schema.db-shm
!! data/surgical_autonomy/review_backup_v1_schema.db-wal
!! data/surgical_autonomy/reviews.db
!! data/surgical_autonomy/run4_extraction.log
!! data/surgical_autonomy/run5.log
!! data/surgical_autonomy/run6_extract.log
!! data/surgical_autonomy/run6_local_resume.log
!! data/surgical_autonomy/surgical_autonomy.db
!! data/surgical_autonomy/surgical_autonomy_ft_adjudication_decisions.json
!! docs/session-reports/session-12/12q_close/__pycache__/
!! docs/session-reports/session-12/12v_U38A/phase1/tests/__pycache__/
!! docs/session-reports/session-12/12w_U17A/repro/__pycache__/
!! docs/session-reports/session-12/12w_U17B/phase1/__pycache__/
!! docs/session-reports/tools/__pycache__/
!! engine/__pycache__/
!! engine/acquisition/__pycache__/
!! engine/adjudication/__pycache__/
!! engine/agents/__pycache__/
!! engine/analysis/__pycache__/
!! engine/cloud/__pycache__/
!! engine/core/__pycache__/
!! engine/elicitation/__pycache__/
!! engine/exporters/__pycache__/
!! engine/migrations/__pycache__/
!! engine/parsers/__pycache__/
!! engine/search/__pycache__/
!! engine/tools/__pycache__/
!! engine/utils/__pycache__/
!! engine/validators/__pycache__/
!! evidence_engine.db
!! logs/
!! primer-history/
!! primer.md
!! scripts/__pycache__/
!! scripts/pdf_acquisition/__pycache__/
!! tests/__pycache__/
!! tests/analysis/paper1/__pycache__/
!! tests/analysis/provenance/__pycache__/
!! tests/migrations/__pycache__/
```

s3_untracked_ignored.tsv: 353 rows; distinct paths not under `__pycache__` or `.venv`: 24, whole:
```
docs/session-reports/session-12/12ab_close/recount_12ab_close.json
docs/session-reports/session-12/12ab_close/recount_12ab_close.py
docs/session-reports/session-12/12ac_close/recount_12ac_close.json
docs/session-reports/session-12/12ac_close/recount_12ac_close.py
docs/session-reports/session-12/12ad_close/recount_12ad_close.json
docs/session-reports/session-12/12ad_close/recount_12ad_close.py
docs/session-reports/session-12/12ae_close/recount_12ae_close.json
docs/session-reports/session-12/12ae_close/recount_12ae_close.py
docs/session-reports/session-12/12af_close/recount_12af_close.json
docs/session-reports/session-12/12af_close/recount_12af_close.py
docs/session-reports/session-12/12ag_open/r804.txt
docs/session-reports/session-12/12ag_open/r805.txt
docs/session-reports/session-12/12s_close/recount_12s_close.json
docs/session-reports/session-12/12s_close/recount_12s_close.py
docs/session-reports/session-12/12v_U37A/recount_12v_r694.json
docs/session-reports/session-12/12v_U37A/recount_12v_r694.py
docs/session-reports/session-12/12w_close/r707.txt
docs/session-reports/session-12/12w_close/r708.txt
docs/session-reports/session-12/12w_close/recount_12w_r707.json
docs/session-reports/session-12/12w_close/recount_12w_r707.py
tests/test_audit_admitted_set.py
tests/test_audit_stage_completion.py
tests/test_u21_no_overlap.py
tests/test_u24_preflight_spec.py
```
clone_status/ (55 files): distinct paths not under `__pycache__` or `.venv`: 34, whole:
```
CLAUDE.md
docs/architecture/modules.md
docs/inventory/entry_points.json
docs/inventory/entry_points.md
docs/plan/ENGINE_REFACTOR_PLAN.md
docs/plan/STANDING_RULES.md
docs/plan/standing_rules_glosses.json
docs/plan/standing_rules_ids.txt
docs/session-reports/session-12/12ab_close/
docs/session-reports/session-12/12ac_close/
docs/session-reports/session-12/12ad_close/
docs/session-reports/session-12/12ae_close/
docs/session-reports/session-12/12ae_close/recount_12ae_close.json
docs/session-reports/session-12/12af_close/
docs/session-reports/session-12/12ag_open/r804.txt
docs/session-reports/session-12/12ag_open/r805.txt
docs/session-reports/session-12/12q_close/lane_line_12q.txt
docs/session-reports/session-12/12s_close/
docs/session-reports/session-12/12v_U37A/recount_12v_r694.json
docs/session-reports/session-12/12v_U37A/recount_12v_r694.py
docs/session-reports/session-12/12w_close/
docs/session-reports/session-12/12w_close/recount_12w_r707.json
engine/agents/ft_screener.py
engine/agents/screener.py
engine/utils/ollama_preflight.py
tests/test_audit_admitted_set.py
tests/test_audit_events.py
tests/test_audit_stage_completion.py
tests/test_auditor.py
tests/test_locator.py
tests/test_ollama_preflight.py
tests/test_screener.py
tests/test_u21_no_overlap.py
tests/test_u24_preflight_spec.py
```

## 3f — the patches

| patch | bytes | git apply --numstat (+, −, path) | touched paths outside docs/ tests/ engine/ scripts/ |
|---|---|---|---|
| patches/12ab-close__clone.patch | 27347 | +144 −0 docs/plan/ENGINE_REFACTOR_PLAN.md | none |
| patches/12ab-u21p__clone.patch | 14448 | +62 −0 docs/plan/ENGINE_REFACTOR_PLAN.md; +16 −8 docs/session-reports/session-12/12w_close/recount_12w_r707.json | none |
| patches/12ac-close__clone.patch | 27678 | +125 −0 docs/plan/ENGINE_REFACTOR_PLAN.md | none |
| patches/12ae-close__pre_clone.patch | 55744 | +144 −2 docs/plan/ENGINE_REFACTOR_PLAN.md | none |
| patches/12ae-u24b__invclone.patch | 15221 | +1 −1 CLAUDE.md; +2 −2 docs/architecture/modules.md; +47 −20 docs/inventory/entry_points.json; +9 −13 docs/inventory/entry_points.md; +1 −0 engine/agents/ft_screener.py; +2 −2 engine/agents/screener.py; +9 −1 engine/utils/ollama_preflight.py; +1 −0 tests/test_ollama_preflight.py; +2 −0 tests/test_screener.py | CLAUDE.md |
| patches/12af-u29a__clone.patch | 1407 | +16 −9 docs/session-reports/session-12/12ae_close/recount_12ae_close.json | none |
| patches/12af-u30a__clone.patch | 1407 | +16 −9 docs/session-reports/session-12/12ae_close/recount_12ae_close.json | none |
| patches/12ag-n46a__regen_clone.patch | 40622 | +138 −0 docs/plan/ENGINE_REFACTOR_PLAN.md; +53 −21 docs/plan/STANDING_RULES.md; +3 −1 docs/plan/standing_rules_glosses.json; +2 −0 docs/plan/standing_rules_ids.txt | none |
| patches/12r-open__pc_clone.patch | 17022 | +69 −0 docs/plan/ENGINE_REFACTOR_PLAN.md; +10 −0 docs/session-reports/session-12/12q_close/lane_line_12q.txt | none |
| patches/12s-close__clone2.patch | 52672 | +183 −2 docs/plan/ENGINE_REFACTOR_PLAN.md | none |
| patches/12s-open__clone.patch | 9501 | +63 −1 docs/plan/ENGINE_REFACTOR_PLAN.md | none |
| patches/12s-u14b__clone.patch | 28251 | +94 −2 docs/plan/ENGINE_REFACTOR_PLAN.md | none |
| patches/12s-u14b__oldclone.patch | 12030 | +17 −0 tests/test_audit_events.py; +15 −0 tests/test_auditor.py; +160 −1 tests/test_locator.py | none |
| patches/12s-u15a__clone.patch | 43543 | +74 −4 docs/plan/ENGINE_REFACTOR_PLAN.md | none |
| patches/12u-close__clone.patch | 34427 | +175 −2 docs/plan/ENGINE_REFACTOR_PLAN.md | none |
| patches/12w-close__clone.patch | 23813 | +125 −1 docs/plan/ENGINE_REFACTOR_PLAN.md | none |
| patches/12w-u17b__clone.patch | 6621 | +3 −1 docs/plan/ENGINE_REFACTOR_PLAN.md | none |

Every touched path outside the four directories (E5): ['CLAUDE.md'].
The untracked JSON `untracked/12ae-close__pre_clone/docs/session-reports/session-12/12ae_close/recount_12ae_close.json`: 1962 B; top-level keys: ['baseline_12ad', 'baseline_json_sha256', 'by_package_and_class', 'class_moves', 'closed_this_session_still_counted', 'expected', 'expected_holds', 'fix_units', 'fix_units_dated_lines', 'fix_units_literal_12w', 'now', 'r510_session_12ae', 'rows_opened'].

## 3g — bytes and encoding

Files that are not valid UTF-8: ['g3_random.bin'] (E2).

| group | files | bytes |
|---|---|---|
| 12ai_SCRI.md and the list | 2 | 32092 |
| the two large tables | 2 | 6886673 |
| the other tables and gate files | 20 | 141541 |
| clone_status/ | 55 | 3577 |
| patches/ | 17 | 411754 |
| untracked/ | 1 | 1962 |
| total | 97 | 7477599 |
- `s4_records_with_committed_copy.tsv` under `gzip -9 -c … | wc -c`: 395319 B (of 2508415 B).
- `s4_records_without_committed_copy.tsv` under `gzip -9 -c … | wc -c`: 1029732 B (of 4378258 B).

## 3h — what the list requires

| named on line | name | SHA256SUMS names it |
|---|---|---|
| 13 | `g1_before.tsv` | yes |
| 13 | `g1_after.tsv` | yes |
| 13 | `g1_diff.txt` | yes |
| 14 | `g1_mainstatus_before.txt` | yes |
| 14 | `g1_mainstatus_after.txt` | yes |
| 14 | `s1_toplevel.tsv` | yes |
| 14 | `s2_citations.tsv` | yes |
| 15 | `s3_clones.tsv` | yes |
| 15 | `s3_clone_sizes.tsv` | yes |
| 15 | `s3_modified_files.tsv` | yes |
| 15 | `s3_untracked_ignored.tsv` | yes |
| 16 | `s3_dirty_sentences.tsv` | yes |
| 16 | `clone_status/` | 55 of 55 files under it |
| 16 | `patches/` | 17 of 17 files under it |
| 17 | `untracked/` | 1 of 1 files under it |
| 17 | `s4_entries.tsv` | yes |
| 17 | `s4_records_with_committed_copy.tsv` | yes |
| 18 | `s4_records_without_committed_copy.tsv` | yes |
| 18 | `s4_unique_by_kind.tsv` | yes |
| 18 | `s6_files.tsv` | yes |
| 18 | `s7_classes.tsv` | yes |
| 19 | `s7_class_totals.tsv` | yes |
| 19 | `g3_match_proof.txt` | yes |
| 19 | `g3_random.bin` | yes |
| 19 | `g4_numstat_check.tsv` | yes |
| 19 | `SHA256SUMS` | no (the list itself) |

If the two large tables alone were left out, the lines of SHA256SUMS naming no committed file would be 2: `s4_records_with_committed_copy.tsv  2508415  1a0af4c3…`; `s4_records_without_committed_copy.tsv  4378258  4648045f…`; the other 94 lines name files that would be committed.

## The closing table

| group | files | bytes | from outside this lane (3a–3e) | scan |
|---|---|---|---|---|
| 12ai_SCRI.md and the list | 2 | 32092 | names: brand-theme-* (prefix form), cm* (prefix form), cm-read-1, cm106b, council-A, dgx-infra-OHC-1, helloai-A, inv-a, lists-A, llm-council-LCH-1, ollama-OLR-1, opreports-A; path groups: /home/ankitsarin/scratch | stop 0, reading 2 |
| the two large tables | 2 | 6886673 | names: none; path groups: none | stop 0, reading 0 |
| the other tables and gate files | 20 | 141541 | names: brand-theme-B, brand-theme-C, brand-theme-D, brand-theme-E, brand-theme-F, cm-read-1, cm105a, cm106a, cm106b, cm106b_failfirst_before.txt, cm106b_step0.marker, cm106b_step0_mtimes.txt, cm106d, council-A, dgx-infra-OHC-1, helloai-A, inv-a, inv-b, lists-A, llm-council-LCH-1, ollama-OLR-1, opreports-A; path groups: /home/ankitsarin/scratch | stop 0, reading 0 |
| clone_status/ | 55 | 3577 | names: none; path groups: none | stop 0, reading 0 |
| patches/ | 17 | 411754 | names: none; path groups: /api/ps, /api/tags, /api/version, /dev/null, /exit, /home/ankitsarin/claude-config, /home/ankitsarin/claude-session-archive, /proc/locks, /wrap | stop 0, reading 8 |
| untracked/ | 1 | 1962 | names: none; path groups: none | stop 0, reading 0 |
