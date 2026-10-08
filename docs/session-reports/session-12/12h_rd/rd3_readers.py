"""12h RD-3 — every reader of payload_json.state_at_write (AST and grep). Read-only.

usage: rd3_readers.py <repo_root>     (stdout: JSON)

Three censuses over engine/, analysis/, scripts/, tests/ and the committed
scripts under docs/:
  A. every AST string constant containing "state_at_write" (code or docstring),
     with its enclosing function and whether it is a Store (dict key / setdefault /
     subscript assignment), a Load (subscript read / .get) or prose (docstring,
     comment-like constant, test id);
  B. every string key read off a field-event payload: `<x>.payload.get("k")`,
     `<x>.payload["k"]`, and the same on a Name bound from json.loads(...) of a
     payload_json column, per function;
  C. every place a whole payload object escapes un-keyed (passed as an argument,
     returned, stored) — the sites a keyed census cannot see, to be read by hand;
  plus a plain-text grep of the same trees for the token, so a non-.py reader
  (template, YAML, SQL file) cannot hide from the AST.
"""
import ast, json, os, sys

root = sys.argv[1]
TREES = ["engine", "analysis", "scripts", "tests", "docs"]
TOKEN = "state_at_write"


def py_files():
    for t in TREES:
        for d, _, fs in os.walk(os.path.join(root, t)):
            for f in sorted(fs):
                if f.endswith(".py"):
                    yield os.path.relpath(os.path.join(d, f), root)


class V(ast.NodeVisitor):
    def __init__(self, path):
        self.path, self.stack = path, []
        self.token, self.keys, self.escapes = [], [], []
        self.payload_names = set()

    def fn(self):
        return ".".join(self.stack) or "<module>"

    def visit_FunctionDef(self, n):
        self.stack.append(n.name); self.generic_visit(n); self.stack.pop()
    visit_AsyncFunctionDef = visit_ClassDef = visit_FunctionDef

    def visit_Assign(self, n):
        # payload = json.loads(<something naming payload_json>)
        src = ast.unparse(n.value)
        if "json.loads" in src and "payload" in src:
            for t in n.targets:
                if isinstance(t, ast.Name):
                    self.payload_names.add(t.id)
        self.generic_visit(n)

    def _is_payload(self, node):
        if isinstance(node, ast.Attribute) and node.attr == "payload":
            return True
        if isinstance(node, ast.Name) and node.id in self.payload_names:
            return True
        return False

    def visit_Call(self, n):
        f = n.func
        if (isinstance(f, ast.Attribute) and f.attr in ("get", "setdefault", "pop")
                and self._is_payload(f.value) and n.args):
            k = n.args[0]
            self.keys.append({"file": self.path, "function": self.fn(), "access": f.attr,
                              "key": k.value if isinstance(k, ast.Constant)
                              else "<name> " + ast.unparse(k),
                              "on": ast.unparse(f.value)})
        for a in list(n.args) + [k.value for k in n.keywords]:
            if self._is_payload(a):
                self.escapes.append({"file": self.path, "function": self.fn(),
                                     "how": "argument to " + ast.unparse(f),
                                     "expr": ast.unparse(a)})
        self.generic_visit(n)

    def _comp(self, n):
        if self._is_payload(n.elt):
            self.escapes.append({"file": self.path, "function": self.fn(),
                                 "how": "element of a comprehension",
                                 "expr": ast.unparse(n)[:160]})
        self.generic_visit(n)
    visit_ListComp = visit_GeneratorExp = visit_SetComp = _comp

    def visit_Subscript(self, n):
        if (isinstance(n.value, ast.Call) and "json.loads" in ast.unparse(n.value.func)
                and isinstance(n.slice, ast.Constant)):
            self.keys.append({"file": self.path, "function": self.fn(),
                              "access": "subscript-on-json.loads",
                              "key": n.slice.value, "on": ast.unparse(n.value)[:80]})
        if self._is_payload(n.value) and isinstance(n.slice, ast.Constant):
            self.keys.append({"file": self.path, "function": self.fn(),
                              "access": "subscript-" + type(n.ctx).__name__,
                              "key": n.slice.value, "on": ast.unparse(n.value)})
        self.generic_visit(n)

    def visit_Return(self, n):
        if n.value is not None and self._is_payload(n.value):
            self.escapes.append({"file": self.path, "function": self.fn(),
                                 "how": "returned", "expr": ast.unparse(n.value)})
        self.generic_visit(n)

    def visit_Constant(self, n):
        if isinstance(n.value, str) and TOKEN in n.value:
            self.token.append({"file": self.path, "function": self.fn(),
                               "is_whole_literal": n.value == TOKEN,
                               "excerpt": n.value if len(n.value) < 90 else
                               "…" + n.value[max(0, n.value.find(TOKEN) - 60):
                                             n.value.find(TOKEN) + 40] + "…"})


token, keys, escapes = [], [], []
for p in py_files():
    src = open(os.path.join(root, p), encoding="utf-8").read()
    v = V(p)
    v.visit(ast.parse(src))
    # second pass so names bound later in the file are known
    v2 = V(p); v2.payload_names = set(v.payload_names); v2.visit(ast.parse(src))
    token += v2.token; keys += v2.keys; escapes += v2.escapes

grep = []
for t in TREES + ["review_specs"]:
    for d, _, fs in os.walk(os.path.join(root, t)):
        for f in sorted(fs):
            path = os.path.join(d, f)
            try:
                txt = open(path, encoding="utf-8").read()
            except (UnicodeDecodeError, OSError):
                continue
            n = txt.count(TOKEN)
            if n:
                grep.append({"file": os.path.relpath(path, root), "occurrences": n})

by_tree = {}
for g in grep:
    top = g["file"].split(os.sep)[0]
    by_tree.setdefault(top, {"files": 0, "occurrences": 0})
    by_tree[top]["files"] += 1; by_tree[top]["occurrences"] += g["occurrences"]

key_table = {}
for k in keys:
    key_table.setdefault(str(k["key"]), set()).add(f'{k["file"]}::{k["function"]} ({k["access"]})')

print(json.dumps({
    "token": TOKEN,
    "A_ast_constants": token,
    "B_payload_keys_read": {k: sorted(v) for k, v in sorted(key_table.items())},
    "B_state_at_write_keyed_reads": [k for k in keys if k["key"] == TOKEN],
    "C_whole_payload_escapes": escapes,
    "grep_files": sorted(grep, key=lambda g: g["file"]),
    "grep_by_tree": by_tree,
}, indent=1, sort_keys=True, ensure_ascii=False))
