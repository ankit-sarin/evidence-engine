#!/usr/bin/env python3
"""12ah-OLRA-REC step 1b — credential scan written for this brief (R794(i)). Read-only on what it scans.

usage: python3 -I rec_scan.py <out.txt> <file>...
Scans every line of every named file. A file that is not valid UTF-8 is read as bytes (latin-1 mapping, so every byte is
one character) and the output says so; it is still scanned. No matched value is written in clear: a hit is printed as
file, line number, class and a mask (the first four characters of the match, then its length). Exit 0 if no stop-class
hit, 1 if any, 2 on a usage error.

STOP classes (a hit stops the brief):
  private_key_block   -----BEGIN <anything> PRIVATE KEY-----  (PEM, OpenSSH, RSA, EC, DSA, ENCRYPTED)
  key_anthropic       sk-ant- followed by 20 or more of [A-Za-z0-9_-]
  key_openai          sk- or sk-proj- followed by 20 or more of [A-Za-z0-9_-]  (not sk-ant-, which is key_anthropic)
  key_google          AIza followed by 35 of [0-9A-Za-z_-]
  key_aws             AKIA or ASIA followed by 16 of [0-9A-Z]
  key_github          gh[pousr]_ followed by 36 or more of [A-Za-z0-9], or github_pat_ followed by 22 or more of [A-Za-z0-9_]
  key_huggingface     hf_ followed by 30 or more of [A-Za-z0-9]
  key_slack           xox[abprs]- followed by 10 or more of [A-Za-z0-9-]
  key_tailscale       tskey- followed by 10 or more of [A-Za-z0-9-]
  auth_header         "bearer" or "basic" (any case) followed by whitespace and a token of 16 or more of [A-Za-z0-9._~+/-]
                      (then any "=" padding) that holds at least one letter and at least one digit
  url_userinfo        scheme://user:password@  (a URL carrying a user and a password)
  secret_assignment   a name containing password, passwd, secret, token, api_key, api-key, apikey or "api key" (any case),
                      then ":" or "=" (optionally "=="/":="), then a non-empty LITERAL: a quoted string ('...' or "...") with
                      at least one character, or an unquoted value of 8 or more non-space characters holding at least one
                      letter and at least one digit. A value that starts with $, a call or attribute reference (holds "(" or
                      starts with a name followed by "."), or a value of only digits is not a literal for this class.
  ssh_key_content     an authorized_keys / public-key line: ssh-rsa, ssh-ed25519, ssh-dss or ecdsa-sha2-<curve> followed by
                      whitespace and AAAA plus 20 or more base64 characters (private-key file content is private_key_block)
READING classes (counted and listed by mask, never a stop):
  email               local@domain.tld
  ipv4_non_loopback   a dotted quad of 0-255 octets not in 127.0.0.0/8
  sudoers_rule        a line holding NOPASSWD, or "ALL=(" , or a "Defaults" line naming sudo options, or the word sudoers
  abs_path_outside    an absolute path of two or more components not under /home/ankitsarin/ (the home directory, which
                      holds the repository) and not part of a URL
"""
import re, sys
from pathlib import Path

STOP = [
    ("private_key_block", re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")),
    ("key_anthropic", re.compile(r"sk-ant-[A-Za-z0-9_-]{20,}")),
    ("key_openai", re.compile(r"\bsk-(?!ant-)(?:proj-)?[A-Za-z0-9_-]{20,}")),
    ("key_google", re.compile(r"AIza[0-9A-Za-z_-]{35}")),
    ("key_aws", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("key_github", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,}|\bgithub_pat_[A-Za-z0-9_]{22,}")),
    ("key_huggingface", re.compile(r"\bhf_[A-Za-z0-9]{30,}")),
    ("key_slack", re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}")),
    ("key_tailscale", re.compile(r"\btskey-[A-Za-z0-9-]{10,}")),
    ("auth_header", re.compile(r"(?i)\b(?:bearer|basic)\s+((?=[A-Za-z0-9._~+/-]*[A-Za-z])(?=[A-Za-z0-9._~+/-]*[0-9])[A-Za-z0-9._~+/-]{16,}=*)")),
    ("url_userinfo", re.compile(r"\b[A-Za-z][A-Za-z0-9+.-]*://[^\s/:@\"'`]+:[^\s/@\"'`]+@")),
    ("secret_assignment", re.compile(
        r"(?i)[A-Za-z0-9_.-]*(?:password|passwd|secret|token|api[_-]?key|api key)[A-Za-z0-9_.-]*[\"']?\s*(?::=|==|=|:)\s*"
        r"(\"[^\"\n]+\"|'[^'\n]+'|(?![$])(?![A-Za-z_][A-Za-z0-9_]*\.)(?![^\s]*\()(?=[^\s]*[A-Za-z])(?=[^\s]*[0-9])[^\s,;\"'`)\]}]{8,})")),
    ("ssh_key_content", re.compile(r"\b(?:ssh-rsa|ssh-ed25519|ssh-dss|ecdsa-sha2-[a-z0-9]+)\s+AAAA[0-9A-Za-z+/]{20,}={0,3}")),
]
OCT = r"(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)"
READ = [
    ("email", re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")),
    ("ipv4_non_loopback", re.compile(rf"(?<![\d.])(?!127\.){OCT}\.{OCT}\.{OCT}\.{OCT}(?![\d.])")),
    ("sudoers_rule", re.compile(r"NOPASSWD|ALL=\(|^\s*Defaults\s|sudoers")),
    ("abs_path_outside", re.compile(r"(?<![\w.~:/-])/(?!home/ankitsarin/)[A-Za-z0-9_.@+-]+/[A-Za-z0-9_.@+/-]+")),
]


def mask(s: str) -> str:
    return f"{s[:4]}…[{len(s)}]"


def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__); return 2
    out, files = Path(sys.argv[1]), sys.argv[2:]
    lines_out, tot = [], {n: 0 for n, _ in STOP + READ}
    for f in files:
        raw = Path(f).read_bytes()
        try:
            text, enc = raw.decode("utf-8"), "utf-8"
        except UnicodeDecodeError:
            text, enc = raw.decode("latin-1"), "NOT valid UTF-8; read as bytes (latin-1)"
        L = text.split("\n")
        if L and L[-1] == "":
            L = L[:-1]
        hits = []
        for i, line in enumerate(L, 1):
            for kind, classes in (("STOP", STOP), ("READING", READ)):
                for name, rx in classes:
                    for m in rx.finditer(line):
                        val = m.group(m.lastindex) if m.lastindex else m.group(0)
                        hits.append(f"  {kind:7} {Path(f).name}:{i} {name} {mask(val)}"); tot[name] += 1
        lines_out.append(f"{Path(f).name}: {len(raw)} B, {len(L)} lines, {enc}; hits {len(hits)} "
                         f"(stop {sum(h.startswith('  STOP') for h in hits)}, reading {sum(h.startswith('  READING') for h in hits)})")
        lines_out += hits
    stop_total = sum(tot[n] for n, _ in STOP)
    lines_out.append("-- totals per class")
    lines_out += [f"  {'STOP   ' if any(n == s for s, _ in STOP) else 'READING'} {n}: {c}" for n, c in tot.items()]
    lines_out.append(f"RESULT: stop-class hits {stop_total}; reading-class hits {sum(tot[n] for n, _ in READ)}; files {len(files)}")
    out.write_text("\n".join(lines_out) + "\n", encoding="utf-8")
    print(lines_out[-1])
    return 0 if stop_total == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
