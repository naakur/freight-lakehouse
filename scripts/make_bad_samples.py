"""
Creates practice files that SHOULD be rejected or quarantined.
Drop them into  inbox/<source>/  and run the batch_ingest job to see the quarantine paths work.

Usage:  python scripts/make_bad_samples.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "freight_etl"))
from utilities.schemas import SOURCES

out = os.path.join(os.path.dirname(__file__), "..", "sample_inbox_bad")
os.makedirs(out, exist_ok=True)
inv_header = ",".join(SOURCES["invoices"])


def write(name, text):
    with open(os.path.join(out, name), "w", newline="") as f:
        f.write(text)


# --- FILE-level problems: preflight should quarantine the whole file ---
write("invoices_wrong_header.csv", "inv_no,carrier,amount\nINV1,C001,10.00\n")   # header mismatch
write("shipments_empty.csv", "")                                                  # 0 bytes
write("invoices_notes.txt", "this is not a csv\n")                                # wrong extension
write("gl_codes_header_only.csv", ",".join(SOURCES["gl_codes"]) + "\n")           # no data rows

# --- ROW-level problems: file passes preflight, silver quarantines the bad rows ---
rows = [
    # 1 GOOD row
    "INV99000001,C001,SHP0000001,2026-08-01,100.0,0.6,60.00,0.12,7.20,,0.00,67.20,USD",
    # 2 negative total
    "INV99000002,C001,SHP0000002,2026-08-01,100.0,0.6,60.00,0.12,7.20,,0.00,-67.20,USD",
    # 3 impossible date
    "INV99000003,C001,SHP0000003,2026-13-45,100.0,0.6,60.00,0.12,7.20,,0.00,67.20,USD",
    # 4 weight is text
    "INV99000004,C001,SHP0000004,2026-08-01,heavy,0.6,60.00,0.12,7.20,,0.00,67.20,USD",
    # 5 missing invoice number
    ",C002,SHP0000005,2026-08-01,100.0,0.6,60.00,0.12,7.20,,0.00,67.20,USD",
    # 6 two extra columns at the end
    "INV99000006,C001,SHP0000006,2026-08-01,100.0,0.6,60.00,0.12,7.20,,0.00,67.20,USD,oops,extra",
]
write("invoices_bad_rows.csv", inv_header + "\n" + "\n".join(rows) + "\n")
print("created files in", os.path.abspath(out))
