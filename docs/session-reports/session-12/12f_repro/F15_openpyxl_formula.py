"""F15 — openpyxl / csv behaviour for formula-leading strings. Pure library check, no repo code."""
import csv, io, openpyxl
print("openpyxl", openpyxl.__version__)
wb = openpyxl.Workbook(); ws = wb.active
cases = ["=1+1", "+1+1", "-1+1", "@SUM(1,1)", " =1+1", "\t=1+1", "\r=1+1", "=", "-5", "plain",
         '=HYPERLINK("http://x","click")']
for s in cases:
    ws.append([s])
for s, row in zip(cases, ws.iter_rows()):
    c = row[0]
    print(f"append {s!r:36} -> data_type={c.data_type!r} value={c.value!r}")
# direct assignment path (ws.cell(value=...)) and the explicit-text escape hatch
c = ws.cell(row=50, column=1, value="=1+1"); print("ws.cell(value='=1+1') ->", c.data_type)
c.data_type = "s"; print("after c.data_type='s'   ->", c.data_type, repr(c.value))
c2 = ws.cell(row=51, column=1, value="'=1+1"); print("leading apostrophe      ->", c2.data_type, repr(c2.value))
c3 = ws.cell(row=52, column=1, value="=1+1"); c3.quotePrefix = True
print("quotePrefix only (no data_type change) ->", c3.data_type)
# round trip: what a reader of the saved file sees
buf = io.BytesIO(); wb.save(buf); buf.seek(0)
ws2 = openpyxl.load_workbook(buf).active
print("reloaded:", [(ws2.cell(row=i + 1, column=1).data_type, ws2.cell(row=i + 1, column=1).value) for i in range(len(cases))])
print("reloaded r50/r51:", ws2.cell(row=50, column=1).data_type, ws2.cell(row=51, column=1).data_type)
# csv: quoting does not neutralise
out = io.StringIO(); csv.writer(out).writerows([[s] for s in cases[:6]])
print("csv.writer output:", repr(out.getvalue()))
# adjacent: control characters are refused outright by openpyxl
try:
    ws.append(["snippet with a vertical tab \x0b from a PDF"])
    print("control char: accepted")
except Exception as e:
    print("control char ->", type(e).__name__, e)
