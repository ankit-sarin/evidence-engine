"""L12-2: --max-cost counts only STORED papers' final attempt. Stubbed extract_paper, no client."""
import logging, tempfile
from pathlib import Path
from engine.cloud.openai_extractor import OpenAIExtractor

logging.basicConfig(level=logging.CRITICAL)
def make(script):
    ex = object.__new__(OpenAIExtractor)
    ex.arm_name = "syn_arm"; ex.model_string = "syn-model"
    ex.expected_fields = ("a", "b"); ex._last_salvage = None
    ex._review_dir = Path(tempfile.mkdtemp(dir="."))
    ex.calls = 0; ex.stored = []
    ex.get_pending_papers = lambda arm: [{"paper_id": i} for i in range(1, 6)]
    ex.load_parsed_text = lambda pid: "text"
    def extract_paper(pid, text):
        ex.calls += 1
        full = script(pid, ex.calls)
        spans = [{"field_name": "a"}, {"field_name": "b"}] if full else [{"field_name": "a"}]
        return {"spans": spans, "cost_usd": 1.00, "extracted_data": {}, "reasoning_trace": "",
                "prompt_text": "", "input_tokens": 0, "output_tokens": 0, "reasoning_tokens": 0}
    ex.extract_paper = extract_paper
    ex.store_result = lambda **kw: ex.stored.append((kw["paper_id"], kw["cost_usd"]))
    ex.run_distribution_check = lambda stats: {"skipped": True}
    return ex

# A: every response incomplete -> nothing stored, ceiling never trips
ex = make(lambda pid, n: False)
stats = ex.run(max_cost_usd=0.50)
print(f"A: ceiling $0.50 | paid calls={ex.calls} at $1.00 each = ${ex.calls:.2f} | "
      f"stats.total_cost=${stats['total_cost']:.2f} | failed={stats['failed']} | loop ran all 5 papers")

# B: each paper complete only on its 3rd attempt -> $3 spent per paper, $1 counted and stored
per = {}
def third(pid, n):
    per[pid] = per.get(pid, 0) + 1
    return per[pid] == 3
ex = make(third)
stats = ex.run(max_cost_usd=2.50)
print(f"B: ceiling $2.50 | paid calls={ex.calls} = ${ex.calls:.2f} | stats.total_cost=${stats['total_cost']:.2f} | "
      f"stored (paper, cost_usd)={ex.stored}")
