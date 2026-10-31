"""12ab-U21A §3 — arithmetic only, from the constants in engine/utils/ollama_client.py: a silent hang (no response byte), three
attempts, the restart refused. Attempt k is sent at (k-1)(L+delay). If L < read it ends at send+read, abandoned after L;
if L >= read the HTTP read timeout ends it at send+read and the watchdog does not fire. Not a measurement of a server."""
READ, DELAY = 900, 30
for name, L in (("explicit 60 s (preflight, with max_retries=0)", 60), ("8b", 300), ("27b / default", 600), ("32b", 900), ("70b", 1200)):
    if L < READ:
        sends = [i * (L + DELAY) for i in range(3)]; raised = sends[2] + L; ends = [s + READ for s in sends]
        events = sorted(set(sends + ends)); peak = max(sum(1 for s, e in zip(sends, ends) if s <= t < e) for t in events)
        inflight = sum(1 for s, e in zip(sends, ends) if s <= raised < e)
        print(f"L={L:>5} {name}: sends {sends}; surfaces at {raised} s; read deadlines {ends}; peak in flight {peak}; in flight at the raise {inflight}; arm: watchdog")
    elif L == READ:
        sends = [i * (L + DELAY) for i in range(3)]
        print(f"L={L:>5} {name}: sends {sends}; surfaces at {sends[2] + L} s; each deadline equal to the watchdog's; peak in flight 1, up to the race; arm: whichever fires first (N23)")
    else:
        sends = [i * (READ + DELAY) for i in range(3)]
        print(f"L={L:>5} {name}: sends {sends}; surfaces at {sends[2] + READ} s; peak in flight 1; arm: HTTP read timeout, the watchdog never fires, the restart path is not reached")
