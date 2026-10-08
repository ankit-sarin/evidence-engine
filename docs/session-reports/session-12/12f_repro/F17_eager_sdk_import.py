"""F17 — does schema import / fresh ReviewDatabase construction import the provider SDKs?
Each probe in its own interpreter; databases only under the cwd (scratch)."""
import subprocess, sys, shutil, os
shutil.rmtree("f17_data", ignore_errors=True)
CHK = "import sys; print('   openai:', 'openai' in sys.modules, '| anthropic:', 'anthropic' in sys.modules, '| engine.cloud.openai_extractor:', 'engine.cloud.openai_extractor' in sys.modules)"
probes = [
 ("import engine.core.database (no construction)", "import engine.core.database"),
 ("import engine.cloud.schema", "import engine.cloud.schema"),
 ("import engine.cloud.base", "import engine.cloud.base"),
 ("FRESH ReviewDatabase('f17', data_root=./f17_data)", "from pathlib import Path; from engine.core.database import ReviewDatabase; d=ReviewDatabase('f17', data_root=Path('f17_data')); d._conn.close()"),
 ("SECOND construction on the same (now receipted) database", "from pathlib import Path; from engine.core.database import ReviewDatabase; d=ReviewDatabase('f17', data_root=Path('f17_data')); d._conn.close()"),
 ("FRESH construction with openai+anthropic made unimportable", "import sys; sys.modules['openai']=None; sys.modules['anthropic']=None\nfrom pathlib import Path; from engine.core.database import ReviewDatabase\ntry:\n    ReviewDatabase('f17b', data_root=Path('f17_data'))\n    print('   constructed OK')\nexcept BaseException as e:\n    print('   construction FAILED:', type(e).__name__, e)"),
]
for label, code in probes:
    print("---", label)
    full = code + ("\n" + CHK if "unimportable" not in label else "")
    r = subprocess.run([sys.executable, "-c", full], capture_output=True, text=True)
    print(r.stdout.rstrip() or "   (no stdout)")
    if r.returncode: print("   rc", r.returncode, r.stderr.strip().splitlines()[-1] if r.stderr.strip() else "")
