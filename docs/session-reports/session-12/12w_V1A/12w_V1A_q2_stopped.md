# 12w-V1A — INT-v12-1's Phase A, stopped at Q2 (R705(g))

HEAD at the reads: `287a9c7bf951517e6ff0c8f82301b01177ffc3e7` (item 0: R705 as Block 103), parent
`c5ab505`. The worktree was made from this HEAD; it differs from `c5ab505` only in the plan and
`12w_V1A/r705.txt`. Live spec `aeb90ba8…8b33`, unchanged. No engine, test, spec or codebook change on
main; no model call; no Ollama request (the pin and capture scripts replace `fetch_model_digest` and
`ollama_chat` with functions that raise).

**Why this record stops at Q2.** Two clauses of the brief give different answers about the same
measurement:

- The row (R1): "A structure change made only inside run_pass1 or elicit … would not be seen by the
  render unless render_messages is changed with it". **Under V1 this holds:** the render does not see
  the change (the elicited pin and the `elicitation_pass1` prompt_hash are byte-identical to HEAD).
- Q2: "If the pin moves or a test fails under V1, the row's premise is false: report it so, with the
  evidence." **Under V1 an existing test fails:**
  `tests/test_elicitation_pipeline.py::test_the_elicited_requests_are_byte_identical`.

So the row's premise is false by the brief's test but true as the row states it. Q4 ("only if Q2
shows the gap is real; else state 'not needed'") depends on which reading holds. Per CONTRADICTION
HANDLING ("STOP at that step … do not adapt"), Q3–Q5 and Q7 are not started. Q6's three results
were measured in the same runs as Q2 and are recorded in §4.

## 1 — I1, I2, I3 (all hold)

- **I1 (MEASURED).** `git show c5ab505:docs/plan/ENGINE_REFACTOR_PLAN.md | tail -n 1` gives "The decision
  log in this file ends at R704.", and `grep -c '^\*\*Block 103\.\*\*'` gives 0.
- **I2 (READ).** The two paths are §2's. They are different code paths at the level of the list of
  attempts: only `render_messages` concatenates attempt 1 and attempt 2. They share the builder of
  each attempt's list (`pass1_messages`) and of its prompt string (`pass1_prompt`).
- **I3 (MEASURED).** `pin_tuples.py` inserts `"."` at `sys.path[0]`, so run from the worktree root with
  main's `.venv/bin/python` it imports the worktree's engine. `scripts/run_pin.py` runs it under `runpy`
  and lists every `engine.*` module it imported (`i3_stderr.txt`): 38 modules, `engine.__file__` =
  `…/12w-v1a/wt/engine/__init__.py`, none outside the worktree. The site-packages carry no
  evidence-engine `.pth`. The HEAD output `pins/pin_head.json` is byte-identical (`cmp`) to
  `12w_U17B/pin_tuples_u17.json`. Under pytest, the candidate's
  `test_the_engine_under_test_is_this_tree` passed in every run.

## 2 — Q1: the two paths, quoted at HEAD

**The runtime** (`engine/elicitation/pipeline.py`). `elicit` calls `run_pass1` with no feedback. If
fields failed, it calls `run_pass1` again with `feedback = build_feedback_block(first, codebook)`:

```python
def run_pass1(unit_map, codebook, field_names, paper_id, *, feedback: str = "", cfg):
    prompt = pass1_prompt(unit_map, codebook, field_names, feedback)
    response, req_hash = ollama_chat(paper_id=paper_id, messages=pass1_messages(prompt),
                                     return_request_hash=True, **cfg.kwargs())

def pass1_prompt(unit_map, codebook, field_names, feedback: str = "") -> str:
    return build_pass1_prompt(unit_map, codebook, field_names) + feedback

def pass1_messages(prompt: str) -> list[dict]:
    return [{"role": "system", "content": SYSTEM_PASS1},
            {"role": "user", "content": prompt}]
```

**The hash** (`engine/core/effective_config.py`). `prompt_hash` calls `render_messages`:

```python
    if stage == "elicitation_pass1":
        from engine.elicitation import pipeline as el
        messages = el.pass1_messages(el.sentinel_pass1_prompt(spec, codebook_path))
        if el.MAX_PASS1_ATTEMPTS > 1:
            feedback = el.sentinel_feedback_block(spec, codebook_path)
            messages += el.pass1_messages(
                el.sentinel_pass1_prompt(spec, codebook_path, feedback=feedback))
        return messages
```

`sentinel_pass1_prompt` → `pass1_prompt(build_unit_map(0, SENTINEL_TEXT), …, feedback)`.
`sentinel_feedback_block` → `build_feedback_block(sentinel_pass1_result(…), codebook)`.

**Shared by both sides:**
- `pass1_messages`: the list for one attempt, i.e. its number of messages, their roles and their order;
- `pass1_prompt`: the user turn, with the feedback appended to the string;
- `build_pass1_prompt` and `build_feedback_block`: the texts.

**Decided on one side only:**
- **Runtime:** that `run_pass1` sends `pass1_messages(prompt)` unchanged. Any message added, removed or
  reordered at this call site is invisible to the render.
- **Hash:** that the stage's hashed list is attempt 1's list followed by attempt 2's.

## 3 — Q2: V1–V3 in a worktree (MEASURED)

**Method.** One git worktree of HEAD at `~/scratch/12w-v1a/wt`, made with `--detach`. The only file
added was a copy of the live codebook at `wt/data/surgical_autonomy/` (sha256 `89dbfa91…ad82`, equal
to main's), because the tests read it relative to cwd. There was no `review.db` and no link to main's
`data/`.

Each variant starts from a clean worktree, applies its diff, runs `scripts/measure.sh`, then resets
with `git checkout -- .`, confirmed clean:
- the pin: `pin_tuples.py` with the live spec and codebook;
- the runtime capture: `scripts/capture_attempts.py`, one fixed synthetic paper, with the fixed
  feedback = `sentinel_feedback_block`, through a stand-in `ollama_chat`;
- the four named test files plus the candidate (`*_named.txt`);
- a supplementary run over the 31 test files that import `engine.elicitation` (`*_supp.txt`).

Every pytest run passed `--basetemp` under `~/scratch/12w-v1a/basetemp/`.

**The variants:**
- **V1** (`diffs/V1_runtime_only.diff`): `run_pass1` sends attempt 1's two messages plus the feedback
  block as a third `user` message, instead of appending it to the user turn. `render_messages` is
  untouched.
- **V2** (`diffs/V2_hash_only.diff`): the same change in `render_messages` only.
- **V3** (`diffs/V3_both.diff`): both.

| | elicited pin | `elicitation_pass1` prompt_hash | non-elicited pin | runtime attempt 2 (messages; sha256 of the kwargs) | runtime attempt 1 kwargs | named + candidate | 31-file supplement |
|---|---|---|---|---|---|---|---|
| HEAD | `d70bf688…59f0` | `5939ce6f…b713` | `d3672ab6…ff5c` | 2 [system, user 35,965 ch]; `307077c9…71a3` | `32c5900d…092a` | 62 passed | 480 passed |
| V1 | **`d70bf688…59f0` (file `cmp`-identical to HEAD)** | `5939ce6f…b713` | `d3672ab6…ff5c` | **3** [system, user 31,914, user 4,051]; `123f6630…94db2` | `32c5900d…092a` | 2 failed, 60 passed | 2 failed, 478 passed |
| V2 | `8f66789a…ec0f` | `5754f6cf…cca0` | `d3672ab6…ff5c` | 2; `307077c9…71a3` (= HEAD) | `32c5900d…092a` | 2 failed, 60 passed | 2 failed, 478 passed |
| V3 | `8f66789a…ec0f` | `5754f6cf…cca0` | `d3672ab6…ff5c` | 3; `123f6630…94db2` | `32c5900d…092a` | 2 failed, 60 passed | 2 failed, 478 passed |

In both runs, each variant's two failures were the same ids:
- **V1:** `test_elicitation_pipeline.py::test_the_elicited_requests_are_byte_identical` (`assert
  'c7a734c141a4...1e2d8f82bb53d' == 'ec969f5f5b5e...8d16e9b3cb08b'`) and the candidate.
- **V2:** `test_prompt_hash_coverage.py::test_elicitation_pass1_hashes_both_attempts` (the roles
  assertion: "Left contains one more item: 'user'") and the candidate.
- **V3:** both of those existing tests. The candidate passes.

V2 moves exactly one prompt hash across both pins (`elicitation_pass1`, elicited); the non-elicited
pin never moves.

**The plain answer.**
- **Under V1 the elicited pin does not move.** The render and the hash do not see a structure change
  made only in `run_pass1`.
- **Under V1 an existing test fails.** `test_the_elicited_requests_are_byte_identical` records one
  digest, `ELICITED_REQUESTS_SHA256`, over all the kwargs a fixed elicited extraction sends (attempt 1,
  attempt 2, Pass 2). Any change to those bytes fails it, structural or not. Its comment reads: "A
  change to this digest is a change to what Run 7 sends, which is a PI fork (12c-E-PASS2-R2), never a
  test update."
- **By the brief's stated test, the row's premise is false.** By the row's own words, the claim about
  the render holds. §0 explains why this record stops here.

## 4 — Q6: the acceptance-test candidate (MEASURED)

`repro/test_v1a_attempt2_structure.py` (sha256 `d6884f81…be82`). It was copied into the worktree's
`tests/` for each run so the suite's fences apply, its sha256 was recorded each time
(`tests/*_repro_sha.txt`), and the copy was removed after each run. It runs `run_pass1` for attempt 1
and attempt 2 over the sentinel unit map, with the sentinel feedback and a stand-in `ollama_chat`, and
asserts the concatenated runtime lists equal `render_messages("elicitation_pass1")`, roles first, then
byte for byte.

| tree | result |
|---|---|
| HEAD | PASSED |
| V1 | FAILED ("Left contains one more item: 'user'": the left operand is the runtime list, which has the extra message) |
| V3 | PASSED |

It also fails under V2 ("Right contains one more item": the rendered list has it), because it ties the two sides together and
fails whichever side changes alone. Unlike the byte-identity digest, it is not re-recorded when a
text changes on both sides, and it says which side differs.

## 5 — Findings

1. **The pin does not see V1; a recorded-digest test does.** The first is the V1 row of §3: the
   elicited pin is `cmp`-identical to HEAD. The second is
   `test_the_elicited_requests_are_byte_identical` failing with "assert 'c7a734c141a4...1e2d8f82bb53d'
   == 'ec969f5f5b5e...8d16e9b3cb08b'". The suite catches the change; the arm pin, and with it the reuse
   key and `ArmPinMismatch`, do not.
2. **That test catches every runtime byte change, not structure as such.** It is one digest over a
   fixed fixture's three calls. A structure change made alongside a ruled text change would be
   recorded in the same new digest, and the pin would still not see it.
3. **The brief's rule and the row's text disagree on V1** (§0). This is the stop.
4. **No other test in the 31 files that import `engine.elicitation` catches V1.** The supplement fails
   only the same two ids. `test_request_capture.py::test_elicitation_pass1` captures attempt 1 only,
   so it passes under V1.
5. **V1 also changes a telemetry field.** `pass1_prompt_chars` is `len(prompt)`, and under V1 the
   prompt no longer includes the feedback. This is in the diff, not measured further.
6. **A timestamp I could not explain (a reading, not a fault).** The codebook copy in the worktree has
   birth time 00:11:59.755Z, but the `cp` that wrote it set its mtime to 00:13:31Z. With `data/` moved
   away, neither the pin script nor the capture script created anything under it (rerun twice,
   `find data` → "No such file or directory"). Its sha256 equals main's before and after.

## 6 — Open questions for the ruling

1. Which reading governs Q4:
   - the brief's ("a test fails under V1 ⇒ premise false ⇒ Q4 not needed"); or
   - the row's (the render and the pin do not see V1 ⇒ the pin-coverage gap R694(g) names is real ⇒
     Q4's options are measured)?
2. If the row's reading governs, Q3–Q7 resume from a fresh worktree of the then-HEAD under the same
   method. The V1–V3 diffs and the candidate are reusable as they stand.

## 7 — Worktree

`git worktree remove ~/scratch/12w-v1a/wt` exited 0. `git worktree list` now shows only
`/home/ankitsarin/projects/evidence-engine  287a9c7 [main]`. The diffs, captures, pin outputs and test
logs are beside this file.
