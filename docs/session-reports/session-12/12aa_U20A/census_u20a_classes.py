"""12aa-U20A — the five census classes over tests/, by AST (R722(a), R723(c), R724(a), R725(b)).
Read-only; imports no scanned module. usage, from the repository root: census_u20a_classes.py (stdout)."""
import ast,re,collections
from pathlib import Path
hits=collections.defaultdict(list)
def scopes(t):
    out={}
    def w(n,name):
        for c in ast.iter_child_nodes(n):
            nm=f"{name}::{c.name}" if isinstance(c,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)) else name
            out[id(c)]=nm; w(c,nm)
    w(t,''); return out
for p in sorted(Path('tests').rglob('*.py')):
    src=p.read_text(encoding='utf-8'); t=ast.parse(src); sc=scopes(t)
    for n in ast.walk(t):
        if isinstance(n,ast.Call):
            f=ast.unparse(n.func)
            if f.endswith('setattr') and len(n.args)>=2:
                a=ast.unparse(n.args[0]); b=ast.unparse(n.args[1]).strip("'\"")
                if b=='_client' and a in('oc','_oc','ollama_client','client_mod'): hits['ii_module_alias_client_replaced'].append((str(p),n.lineno,sc.get(id(n),'')))
                if b in('effective_ceiling','n_ctx_train','server_context_length','_check_input_fits','_check_input_was_read'): hits['v_whole_function'].append((str(p),n.lineno,sc.get(id(n),''),b))
                if b in('SERVICE_UNIT_FILE','SERVICE_DROPIN_DIR','SERVER_DEFAULT_CTX','_CEILING_CACHE'): hits['i_patch_name'].append((str(p),n.lineno,sc.get(id(n),''),b))
            if f in('patch','patch.object','mock.patch') and n.args:
                s=' '.join(ast.unparse(x) for x in n.args[:2])
                if re.search(r'effective_ceiling|n_ctx_train|server_context_length|_check_input|_CEILING_CACHE|SERVICE_|ollama_client\._client\b|\._client\'',s): hits['i_patch_string'].append((str(p),n.lineno,sc.get(id(n),''),s[:90]))
        if isinstance(n,(ast.FunctionDef)) and n.name=='show': hits['show_fake_def'].append((str(p),n.lineno,sc.get(id(n),'')))
        if isinstance(n,ast.Compare) and 'shows' in ast.unparse(n.left): hits['iii_show_count_pin'].append((str(p),n.lineno,sc.get(id(n),''),ast.unparse(n)[:70]))
        if isinstance(n,ast.Attribute) and n.attr in('parameters','modelfile') and 'show' in src: hits['show_field_read'].append((str(p),n.lineno,sc.get(id(n),'')))
for k,v in sorted(hits.items()):
    print(f'## {k}: {len(v)} hits in {len({x[0] for x in v})} files')
    for x in v: print('  ',*x)
