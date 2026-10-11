"""L12-6: a non-integer retry-after header aborts the whole run with ValueError. No request is made."""
import logging, tempfile
from pathlib import Path
from unittest.mock import patch
import httpx, openai
from engine.cloud.openai_extractor import OpenAIExtractor
logging.basicConfig(level=logging.CRITICAL)
ex = object.__new__(OpenAIExtractor)
ex.arm_name = "syn_arm"; ex.model_string = "m"
ex.get_pending_papers = lambda arm: [{"paper_id": i} for i in (1, 2, 3)]
ex.load_parsed_text = lambda pid: "text"
seen = []
def boom(pid, text):
    seen.append(pid)
    resp = httpx.Response(429, headers={"retry-after": "1.5"}, request=httpx.Request("POST", "http://stub.invalid"))
    raise openai.RateLimitError("rate limited", response=resp, body=None)
ex.extract_with_completeness = boom
ex.run_distribution_check = lambda s: {"skipped": True}
with patch("time.sleep"):
    try:
        ex.run()
        print("run returned")
    except Exception as e:
        print(f"run() raised {type(e).__name__}: {e} | papers attempted: {seen} of 3")
