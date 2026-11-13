RULING [evidence-engine WR5-B] — stop at step 1 upheld; resume at step 2
STATUS: the stop is correct and the cause is the architect's: B1's
  "open with a TASK title" described a layout the architect had not
  read. No R number; not transcribed into the decision log.
ASSUMPTIONS IN THIS RULING:
  READ (the WR5-B stop report, as printed):
    "766: TASK [evidence-engine 12y-U18B] — item 0"
    "862: TASK [evidence-engine 12y-U18B, resumed under R722] —"
    "909: TASK [evidence-engine 12y-U18B, resumed under R722 and R723] —"
    "1039: TASK [evidence-engine 12y-U18B, resumed at step 2 under R724] —"
    "1517: TASK [evidence-engine 12y-U18C] — item 0"
    "M2 is 21"; "M3 is 71 transcripts and 375 matching turns"
  (a) B1 is restated: each of the five turns contains a TASK title
      naming 12y-U18B or 12y-U18C. The five titles quoted above meet
      it. S1 is those five turns.
  (b) Region rule, for every set. Split each brief text at the first
      line that, after leading whitespace, begins "TASK [". Region PRE
      is the text before that line; region BRIEF is that line onward.
      Every check runs on both regions. Every hit carries its region,
      and every table gives the two regions as separate columns. A
      text with no such line is counted "unsplit" and reported apart.
      Report how many texts hold more than one such line.
  (c) S2 is the committed lane brief files, excluding every file whose
      name begins brief_WR5. Recount it under that rule.
  (d) New set S4: the committed WR5 brief files, this one included.
      All checks, both regions, reported apart from S2. No bar applies
      to S4.
  (e) S3 excludes every turn whose TASK title names WR5. The recounts
      of M2 and M3 are accepted as reported.
  (f) Brief-file comparison (R784(g)(v)): the architect's copies hash
      to the reported values. brief_WR5-A.md: sha256
      dee1182c33918ac78db9da6e84cb53beca3fe69370345f0cab278fce6199e5e1,
      6113 bytes, 110 lines. brief_WR5-B.md: sha256
      9fa7df245e2e660d9d058c408ae64f6ea47be0553c60e5f951b837beedbd41cc,
      8973 bytes, 162 lines. No finding.

TASK [evidence-engine WR5-B, resumed at step 2 under the ruling above] — Prototype brief checker
PURPOSE: Resume. Code in scratch plus the evidence directory. The
  brief of record is docs/session-reports/WR5-B/brief_WR5-B.md at
  commit 957f917a47c5720977bae5b48e762b46b31d8391; it stands in full
  except where the ruling above, in this block, changes it.
MACHINE: DGX    SESSION DIRECTORY: /home/ankitsarin/projects/evidence-engine
SKILL: none
PERMISSIONS: auto mode, commit steps included  |  blast-radius: low
  Touches and STOP list: as the brief of record, plus one new file for
  this paste (item 0 below).

ASSUMPTION LEDGER (load-bearing claims only):
  MEASURED (WR5-A, section 3): one committed brief file carries an
    "_R1" suffix. Its name form is the model for this paste's file.
  INFERRED — expectation (report a mismatch, continue):
    X3. Region PRE will draw most of the K10 and K13 hits. To be
        measured; no stop rests on it.

CONTRACT (deltas only):
  Item 0. Write this paste once, in R784(g)(v)'s form, into
    docs/session-reports/WR5-B/, named in the form the existing "_R1"
    brief file takes (read and quote that file's name). Report sha256,
    bytes and lines. Commit it alone.
  Step 1 is closed by ruling clauses (a) and (e).
  Steps 2 and 3: as the brief of record. The prototype takes the
    region rule of clause (b) as part of its contract, with its own
    control pair: one text that must split and one that must report
    "unsplit".
  Step 4: sets S1, S2 (clause c), S3 (clause e) and S4 (clause d),
    each under the region rule. The hits files list S1, S2 and S4 hits
    verbatim with region; S3 is counts only.

ACCEPTANCE GATES (this task only):
  G1 to G5 of the brief of record stand. G1's allowed paths gain the
  item-0 file above.
  G6. Every table shows region PRE and region BRIEF separately, and
      the two sum to the whole-text figure. Expected: sums agree.

OUT OF SCOPE, CONSTRAINTS, CONTRADICTION HANDLING:
  As the brief of record.

FINAL REPORT FORMAT:
  As the brief of record, sections 1 to 7, with these changes: section
  1 reports this paste's file; section 4 gives each set by region and
  adds S4; section 5 lists, per stop brief, the check ids that hit in
  region BRIEF and, apart, in region PRE; add the "unsplit" and
  multi-title counts.

HANDBACK:
  Stop and report. Do not install, tune or extend the checker without
  explicit sign-off. Await the next brief or ruling in this session.
  STOP-WR5-B-R1
