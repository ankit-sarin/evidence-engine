"""F08 — real engine.search.openalex._paginate_with_retry. No network: the HTTP boundary is faked."""
import time, requests
import engine.search.openalex as oa
import pyalex, pyalex.api as api
time.sleep = lambda s: None            # zero the backoff
oa.time.sleep = lambda s: None
print("pyalex", pyalex.__version__, "| Paginator is a class with __next__:",
      isinstance(api.Paginator, type) and hasattr(api.Paginator, "__next__"))

# (A) B's reproduction: a plain generator that yields page 1 then raises on page 2
class GenQuery:
    def paginate(self, per_page):
        def g():
            yield ["p1-a", "p1-b"]
            raise requests.ConnectionError("transient page-2 failure")
            yield ["p2-a"]
        return g()
try:
    print("(A) generator       ->", list(oa._paginate_with_retry(GenQuery())), "| no exception propagated")
except Exception as e:
    print("(A) generator raised", type(e).__name__, e)

# (B) the REAL pyalex Works().paginate() Paginator, with only _get_from_url faked
class Page(list):
    def __init__(self, items, meta): super().__init__(items); self.meta = meta
def run_real(fail_plan, label):
    calls = []
    pages = {"*": (["p1-a", "p1-b"], "c2"), "c2": (["p2-a", "p2-b"], "c3"), "c3": (["p3-a"], None)}
    def fake(self, url, session=None):
        cur = self.params["cursor"]; calls.append(cur)
        if fail_plan.get(cur, 0) > 0:
            fail_plan[cur] -= 1
            raise requests.ConnectionError(f"transient failure at cursor {cur}")
        items, nxt = pages[cur]
        return Page(items, {"next_cursor": nxt, "count": 5})
    orig = api.BaseOpenAlex._get_from_url
    api.BaseOpenAlex._get_from_url = fake
    try:
        q = oa.Works().search("x")
        print(f"({label}) paginator type:", type(q.paginate(per_page=200)).__name__)
        try:
            out = list(oa._paginate_with_retry(q))
            print(f"({label}) returned", [list(p) for p in out], "| cursors requested:", calls)
        except Exception as e:
            print(f"({label}) RAISED", type(e).__name__, e, "| cursors requested:", calls)
    finally:
        api.BaseOpenAlex._get_from_url = orig
run_real({"c2": 1}, "B1 real paginator, page 2 fails once")
run_real({"c2": 2}, "B2 real paginator, page 2 fails twice")
run_real({"c2": 3}, "B3 real paginator, page 2 fails 3x (= _MAX_RETRIES)")

# (C) default n_max on the call the wrapper makes
import inspect
print("(C) Works.paginate signature:", inspect.signature(api.BaseOpenAlex.paginate))
n = {"i": 0}
def big(self, url, session=None):
    n["i"] += 1
    return Page(["w"] * 200, {"next_cursor": f"c{n['i']}", "count": 25000})
orig = api.BaseOpenAlex._get_from_url; api.BaseOpenAlex._get_from_url = big
try:
    got = sum(len(p) for p in oa._paginate_with_retry(oa.Works().search("x")))
    print(f"(C) advertised meta.count=25000, wrapper yielded {got} works in {n['i']} requests, no error")
finally:
    api.BaseOpenAlex._get_from_url = orig
