"""12v-U37R step 4b — the committed form of each request: the full request stays in scratch (it carries the
paper's whole numbered text, which this repository does not hold; the 12r-U13-PROBE precedent), and the
committed copy replaces the body of "## Numbered Paper Text" with its sha256 and length. Everything else —
model, kwargs, system message, the instruction text before the paper and the feedback block after it — is
kept verbatim, and the full request's hash is recorded beside it.
usage, from the repository root: redact_requests.py <full_requests_dir> <out_dir>   (stdout: index JSON)"""
import hashlib, json, os, sys
SRC, OUT = sys.argv[1:3]
H = "## Numbered Paper Text\n"
index = {}
for cid in sorted(os.listdir(SRC)):
    for arm in ("old", "new"):
        p = os.path.join(SRC, cid, f"{arm}.json")
        raw = open(p, "rb").read(); f = json.loads(raw)
        user = f["request"]["messages"][1]["content"]
        i = user.index(H) + len(H); j = user.index("\n\n## Your previous response", i)
        body = user[i:j]
        red = dict(f); red["request"] = dict(f["request"])
        msgs = [dict(m) for m in f["request"]["messages"]]
        msgs[1]["content"] = (user[:i] + f"⟦paper text: {len(body)} chars, sha256 "
                              f"{hashlib.sha256(body.encode('utf-8')).hexdigest()}⟧" + user[j:])
        red["request"]["messages"] = msgs
        red["full_request_file_sha256"] = hashlib.sha256(raw).hexdigest()
        os.makedirs(os.path.join(OUT, cid), exist_ok=True)
        with open(os.path.join(OUT, cid, f"{arm}.{f['request_hash'][:16]}.json"), "w", encoding="utf-8") as fh:
            json.dump(red, fh, ensure_ascii=False, indent=1)
        index[f"{cid}/{arm}"] = {"request_hash": f["request_hash"], "full_request_file_sha256": red["full_request_file_sha256"],
                                 "paper_text_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(), "paper_text_chars": len(body)}
print(json.dumps(index, indent=1, sort_keys=True))
