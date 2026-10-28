"""
Creates the carrier change feed (CDC) practice files in sample_cdc/.

A CDC feed = a list of CHANGES to a table, not the table itself:
    op = I (insert) | U (update) | D (delete)   and   change_ts = when it happened in the source system.

Files (send them in two waves to see history being rewritten):
  carriers_cdc_0001.csv  initial load of 8 carriers
  carriers_cdc_0002.csv  updates, a new carrier and a delete                       } wave A
  carriers_cdc_0003.csv  a LATE record (older timestamp, arrives last), a newer update,
                         and 2 broken rows (bad op, missing timestamp)             } wave B
"""
import os

HEADER = "carrier_id,carrier_name,scac,payment_terms,op,change_ts"
OUT = os.path.join(os.path.dirname(__file__), "..", "sample_cdc")
os.makedirs(OUT, exist_ok=True)

FILES = {
    "carriers_cdc_0001.csv": [
        "C001,Atlas Freight Lines,ATLS,NET30,I,2026-01-01 08:00:00",
        "C002,BlueRiver Logistics,BRVL,NET45,I,2026-01-01 08:00:00",
        "C003,Cardinal Express,CRDX,NET30,I,2026-01-01 08:00:00",
        "C004,Delta Haul Co,DLTH,NET30,I,2026-01-01 08:00:00",
        "C005,Evergreen Transport,EVGT,NET30,I,2026-01-01 08:00:00",
        "C006,Frontier Cargo,FRNT,NET45,I,2026-01-01 08:00:00",
        "C007,Granite State Carriers,GRNS,NET30,I,2026-01-01 08:00:00",
        "C008,Harbor Line Freight,HRBL,NET30,I,2026-01-01 08:00:00",
    ],
    "carriers_cdc_0002.csv": [
        "C001,Atlas Freight Lines,ATLS,NET45,U,2026-04-01 09:00:00",              # payment terms change
        "C003,Cardinal Express Logistics,CRDX,NET30,U,2026-05-15 10:30:00",       # rebrand
        "C005,Evergreen Transport,EVGT,NET60,U,2026-06-01 09:00:00",              # payment terms change
        "C009,Iron Ridge Freight,IRDG,NET30,I,2026-06-10 11:00:00",               # new carrier
        "C008,,,,D,2026-07-01 00:00:00",                                          # contract ended -> delete
    ],
    "carriers_cdc_0003.csv": [
        "C001,Atlas Freight Lines,ATLS,NET60,U,2026-08-01 09:00:00",              # newer update
        "C001,Atlas Freight Lines,ATLS,NET40,U,2026-02-15 09:00:00",              # LATE: happened before the NET45 change
        "C006,Frontier Cargo,FRNT,NET45,X,2026-08-05 09:00:00",                   # broken: op X
        "C007,Granite State Carriers,GRNS,NET30,U,",                              # broken: no timestamp
    ],
}

for name, rows in FILES.items():
    with open(os.path.join(OUT, name), "w", newline="") as f:
        f.write(HEADER + "\n" + "\n".join(rows) + "\n")
print("created", list(FILES), "in", os.path.abspath(OUT))
