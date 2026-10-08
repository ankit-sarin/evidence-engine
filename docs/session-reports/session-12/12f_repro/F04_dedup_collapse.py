"""F04 — drive the real engine.search.dedup.deduplicate with synthetic citations. No network."""
from engine.search.dedup import deduplicate
from engine.search.models import Citation

def C(src, title, doi=None, pmid=None, **kw):
    return Citation(source=src, title=title, doi=doi, pmid=pmid, raw_data={"origin": f"{src}:{doi or pmid or title}"}, **kw)

def show(label, r):
    print(f"--- {label}")
    print(f"    unique={len(r.unique_citations)} duplicates={r.stats['duplicates_found']}")
    for c in r.unique_citations:
        print(f"    kept: source={c.source} pmid={c.pmid} doi={c.doi} title={c.title!r} raw={c.raw_data}")
    for p in r.duplicate_pairs:
        print(f"    pair: {p}")

T = "Autonomous suturing in robotic surgery: a randomized trial"
# B-1: same title, two distinct non-empty DOIs (pubmed vs openalex)
show("B1 same title, distinct DOIs, pubmed+openalex",
     deduplicate([C("pubmed", T, doi="10.1000/aaa", pmid="111")], [C("openalex", T, doi="10.1000/bbb", pmid="222")]))
# B-1 variant: both from openalex
show("B1b same title, distinct DOIs, both openalex",
     deduplicate([], [C("openalex", T, doi="10.1000/aaa"), C("openalex", T, doi="10.1000/bbb")]))
# B-2: two identical PubMed entries
show("B2 two identical PubMed entries",
     deduplicate([C("pubmed", T, doi="10.1000/aaa", pmid="111"), C("pubmed", T, doi="10.1000/aaa", pmid="111")], []))
# fuzzy: near-identical titles (Part 1 / Part 2), distinct DOIs and PMIDs
show("FUZZY companion papers, distinct DOI+PMID",
     deduplicate([C("pubmed", T + " - part 1", doi="10.1000/p1", pmid="301")],
                 [C("openalex", T + " - part 2", doi="10.1000/p2", pmid="302")]))
# index not refreshed after a merge adds an identifier
show("INDEX pubmed rec without DOI gains DOI X by title merge; later openalex rec with DOI X, other title",
     deduplicate([C("pubmed", T, pmid="401")],
                 [C("openalex", T, doi="10.1000/x"),
                  C("openalex", "Erratum to an entirely different heading about livers", doi="10.1000/X")]))
