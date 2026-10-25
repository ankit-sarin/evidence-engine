"""Run pin_tuples.py from cwd under runpy, then report to stderr which engine modules it imported."""
import runpy, sys
script = sys.argv[1]; sys.argv = [script] + sys.argv[2:]
try:
    runpy.run_path(script, run_name="__main__")
finally:
    mods = {n: getattr(m, "__file__", None) for n, m in sys.modules.items()
            if n == "engine" or n.startswith("engine.")}
    print("I3 sys.path[0:2]:", sys.path[0:2], file=sys.stderr)
    print("I3 engine modules imported:", len(mods), file=sys.stderr)
    bad = sorted(n for n, f in mods.items() if f and not f.startswith(sys.argv[0].rsplit("/docs/", 1)[0]))
    print("I3 engine.__file__:", mods.get("engine"), file=sys.stderr)
    print("I3 engine.elicitation.pipeline:", mods.get("engine.elicitation.pipeline"), file=sys.stderr)
    print("I3 modules outside this tree:", bad, file=sys.stderr)
