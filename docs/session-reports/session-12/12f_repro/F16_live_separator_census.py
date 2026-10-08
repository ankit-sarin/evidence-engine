"""F16 — the one authorised live read: mode=ro, count TEXT cells containing U+001F / U+001E."""
import sqlite3
c = sqlite3.connect("file:data/surgical_autonomy/review.db?mode=ro", uri=True)
tabs = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
ncols = ncells = 0; hits = []
for t in tabs:
    for col in [r[1] for r in c.execute(f'PRAGMA table_info("{t}")')]:
        n, us, rs = c.execute(
            f'SELECT count(*), coalesce(sum(instr("{col}", char(31))>0),0), coalesce(sum(instr("{col}", char(30))>0),0) '
            f'FROM "{t}" WHERE typeof("{col}")=\'text\'').fetchone()
        ncols += 1; ncells += n
        if us or rs: hits.append((t, col, us, rs))
print(f"tables={len(tabs)} columns={ncols} text_cells_scanned={ncells}")
print("non-zero (table, column, cells_with_US, cells_with_RS):", hits if hits else "NONE — all 0")
# sqlite_master text is hashed by the schema hash with the same separators
m = c.execute("SELECT coalesce(sum(instr(coalesce(sql,'')||name||tbl_name, char(31))>0),0), coalesce(sum(instr(coalesce(sql,'')||name||tbl_name, char(30))>0),0) FROM sqlite_master").fetchone()
print("sqlite_master rows with US / RS:", m)
c.close()
