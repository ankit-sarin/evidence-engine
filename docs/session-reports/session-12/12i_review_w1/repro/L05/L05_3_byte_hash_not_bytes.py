"""Codebook.sha256 hashes decoded-then-re-encoded text, not the file's bytes."""
from _common import CODEBOOK, write
from engine.core.codebook import load_codebook, compute_codebook_sha256
p = write("crlf.yaml", CODEBOOK, newline="\r\n")
cb = load_codebook(p)
print("file has CRLF:", b"\r\n" in p.read_bytes())
print("Codebook.sha256           :", cb.sha256)
print("compute_codebook_sha256() :", compute_codebook_sha256(p))
print("equal:", cb.sha256 == compute_codebook_sha256(p))
