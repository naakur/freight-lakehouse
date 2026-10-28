#!/usr/bin/env python3
"""
Freight audit synthetic data generator (stdlib only, seeded, reproducible).

Outputs (under --out):
  batch/carriers.csv
  batch/rate_cards.csv            (2 versions per lane -> SCD2 practice)
  batch/accessorial_codes.csv
  batch/gl_codes.csv
  batch/shipments.csv
  batch/invoices.csv              (one line per shipment, with injected errors)
  truth/invoice_anomaly_labels.csv (ground truth to validate your audit rules)
  stream/kafka_batch_0001.json ... (Kafka-style envelope, JSON lines, ONE multiplexed
                                    feed carrying 4 topics, arrival-ordered)
      topics: freight.tracking.events, freight.invoice.status,
              freight.rate.changes, freight.claims.filed
      envelope: topic, partition, offset, key, value (JSON string), timestamp, headers
      traits: duplicates, late events, schema v1->v2 evolution (tracking adds geo),
              nested JSON (claims), ~0.3% malformed values

Usage:
  python generate_freight_data.py --shipments 3000 --seed 42 --out ./data
"""
import argparse
import csv
import json
import os
import random
import zlib
from datetime import date, datetime, timedelta

REGIONS = ["NE", "SE", "MW", "SW", "W", "NW"]
SERVICES = {"GROUND": (5, 1.0), "EXPRESS": (2, 1.9), "ECONOMY": (7, 0.8)}  # transit days, rate multiplier
CARRIERS = [
    ("C001", "Atlas Freight Lines", "ATLS"), ("C002", "BlueRiver Logistics", "BRVL"),
    ("C003", "Cardinal Express", "CRDX"), ("C004", "Delta Haul Co", "DLTH"),
    ("C005", "Evergreen Transport", "EVGT"), ("C006", "Frontier Cargo", "FRNT"),
    ("C007", "Granite State Carriers", "GRNS"), ("C008", "Harbor Line Freight", "HRBL"),
]
ACCESSORIALS = [  # code, description, flat amount
    ("LIFT", "Liftgate service", 45.0), ("RESI", "Residential delivery", 60.0),
    ("INSD", "Inside delivery", 85.0), ("APPT", "Delivery appointment", 30.0),
    ("DETN", "Detention", 120.0), ("HAZM", "Hazmat handling", 150.0),
]
GL_CODES = [
    ("6100", "Freight - Inbound Raw Materials"), ("6110", "Freight - Outbound Customer"),
    ("6120", "Freight - Inter-Plant Transfer"), ("6130", "Freight - Expedited"),
]
RATE_V1 = (date(2026, 1, 1), date(2026, 6, 30))
RATE_V2 = (date(2026, 7, 1), date(9999, 12, 31))
ANOMALY_RATE = 0.12


def d(x):
    return x.isoformat()


def write_csv(path, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shipments", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="./data")
    ap.add_argument("--stream-batches", type=int, default=20)
    a = ap.parse_args()
    rnd = random.Random(a.seed)
    out = a.out

    # ---------- reference data ----------
    write_csv(f"{out}/batch/carriers.csv", ["carrier_id", "carrier_name", "scac"], CARRIERS)
    write_csv(f"{out}/batch/accessorial_codes.csv",
              ["accessorial_code", "description", "flat_amount"], ACCESSORIALS)
    write_csv(f"{out}/batch/gl_codes.csv", ["gl_code", "gl_description"], GL_CODES)

    # rate cards: carrier x lane x service, 2 versions (v2 ~ +3..6% rate, new fuel %)
    rate_rows, rate_lookup = [], {}
    for cid, _, _ in CARRIERS:
        for o in REGIONS:
            for dst in REGIONS:
                for svc, (_, mult) in SERVICES.items():
                    base = rnd.uniform(0.18, 0.42) * mult
                    mn = round(rnd.uniform(60, 140) * mult, 2)
                    fuel1 = round(rnd.uniform(0.10, 0.18), 3)
                    for ver, (ef, et) in ((1, RATE_V1), (2, RATE_V2)):
                        rate = round(base * (1 if ver == 1 else rnd.uniform(1.03, 1.06)), 4)
                        fuel = fuel1 if ver == 1 else round(fuel1 + rnd.uniform(0.005, 0.02), 3)
                        rate_rows.append([f"{cid}-{o}-{dst}-{svc}-v{ver}", cid, o, dst, svc,
                                          rate, mn, fuel, d(ef), d(et)])
                        rate_lookup[(cid, o, dst, svc, ver)] = (rate, mn, fuel)
    write_csv(f"{out}/batch/rate_cards.csv",
              ["rate_id", "carrier_id", "origin_region", "dest_region", "service_level",
               "rate_per_lb", "min_charge", "fuel_surcharge_pct", "effective_from", "effective_to"],
              rate_rows)

    # ---------- shipments ----------
    ship_rows, ships = [], []
    start, span = date(2026, 3, 1), 214  # Mar 1 .. Sep 30
    for i in range(1, a.shipments + 1):
        sid = f"SHP{i:07d}"
        cid = rnd.choice(CARRIERS)[0]
        o, dst = rnd.choice(REGIONS), rnd.choice(REGIONS)
        svc = rnd.choices(list(SERVICES), weights=[60, 15, 25])[0]
        ship_date = start + timedelta(days=rnd.randrange(span))
        weight = round(rnd.lognormvariate(5.3, 0.9), 1)           # ~200 lb median
        weight = max(10.0, min(weight, 12000.0))
        pieces = max(1, int(weight // rnd.randint(40, 120)))
        acc = [c for c, _, _ in ACCESSORIALS if rnd.random() < 0.10]
        promised = ship_date + timedelta(days=SERVICES[svc][0])
        gl = rnd.choices([g for g, _ in GL_CODES], weights=[30, 50, 12, 8])[0]
        row = [sid, cid, f"PO{rnd.randint(100000, 999999)}", o, dst, svc, d(ship_date),
               d(promised), weight, pieces, "|".join(acc), gl]
        ship_rows.append(row)
        ships.append(dict(sid=sid, cid=cid, o=o, dst=dst, svc=svc, ship_date=ship_date,
                          promised=promised, weight=weight, acc=acc, gl=gl))
    write_csv(f"{out}/batch/shipments.csv",
              ["shipment_id", "carrier_id", "po_number", "origin_region", "dest_region",
               "service_level", "ship_date", "promised_delivery_date", "weight_lbs",
               "pieces", "accessorials", "gl_code"], ship_rows)

    # ---------- invoices (+ injected anomalies) ----------
    acc_amt = {c: amt for c, _, amt in ACCESSORIALS}
    inv_rows, labels, n_inv, inv_meta = [], [], 0, []
    kinds = ["OVERCHARGE_RATE", "DUPLICATE_INVOICE", "FUEL_VARIANCE",
             "WEIGHT_MISMATCH", "UNAUTHORIZED_ACCESSORIAL"]
    for s in ships:
        ver = 1 if s["ship_date"] <= RATE_V1[1] else 2
        rate, mn, fuel = rate_lookup[(s["cid"], s["o"], s["dst"], s["svc"], ver)]
        billed_w, billed_rate, billed_fuel, billed_acc = s["weight"], rate, fuel, list(s["acc"])
        kind = rnd.choice(kinds) if rnd.random() < ANOMALY_RATE else None
        if kind == "OVERCHARGE_RATE":
            billed_rate = round(rate * rnd.uniform(1.06, 1.20), 4)
        elif kind == "FUEL_VARIANCE":
            billed_fuel = round(fuel + rnd.uniform(0.03, 0.08), 3)
        elif kind == "WEIGHT_MISMATCH":
            billed_w = round(s["weight"] * rnd.uniform(1.10, 1.35), 1)
        elif kind == "UNAUTHORIZED_ACCESSORIAL":
            extra = [c for c in acc_amt if c not in s["acc"]]
            billed_acc.append(rnd.choice(extra))
        base = round(max(mn, billed_w * billed_rate), 2)
        fuel_amt = round(base * billed_fuel, 2)
        acc_total = round(sum(acc_amt[c] for c in billed_acc), 2)
        total = round(base + fuel_amt + acc_total, 2)
        n_inv += 1
        inv_no = f"INV{n_inv:08d}"
        inv_date = s["ship_date"] + timedelta(days=rnd.randint(7, 25))
        row = [inv_no, s["cid"], s["sid"], d(inv_date), billed_w, billed_rate, base,
               billed_fuel, fuel_amt, "|".join(billed_acc), acc_total, total, "USD"]
        inv_rows.append(row)
        inv_meta.append((inv_no, s, inv_date, total, None if kind == "DUPLICATE_INVOICE" else kind))
        if kind:
            labels.append([inv_no, s["sid"], kind])
        if kind == "DUPLICATE_INVOICE":
            n_inv += 1
            dup_no = f"INV{n_inv:08d}"
            dup = list(row)
            dup[0] = dup_no
            dup[3] = d(inv_date + timedelta(days=rnd.randint(1, 10)))
            inv_rows.append(dup)
            inv_meta.append((dup_no, s, inv_date + timedelta(days=2), total, "DUPLICATE_INVOICE"))
            labels.append([dup_no, s["sid"], "DUPLICATE_INVOICE"])
            labels.pop(-2)  # the original is legitimate; only the repeat is the anomaly
    rnd.shuffle(inv_rows)
    write_csv(f"{out}/batch/invoices.csv",
              ["invoice_number", "carrier_id", "shipment_id", "invoice_date", "billed_weight_lbs",
               "billed_rate_per_lb", "base_charge", "fuel_surcharge_pct", "fuel_amount",
               "accessorials", "accessorial_amount", "total_amount", "currency"], inv_rows)
    write_csv(f"{out}/truth/invoice_anomaly_labels.csv",
              ["invoice_number", "shipment_id", "anomaly_type"], labels)

    # ---------- streaming: multiplexed Kafka-style topics ----------
    T_TRK, T_INV = "freight.tracking.events", "freight.invoice.status"
    T_RATE, T_CLM = "freight.rate.changes", "freight.claims.filed"
    PARTS = 3
    msgs = []  # (arrival_ts, topic, key, schema_version, payload)

    def emit(topic, key, event_ts, payload, ver="1", late_p=0.04):
        delay = timedelta(minutes=rnd.randint(1, 20))
        if rnd.random() < late_p:
            delay = timedelta(hours=rnd.randint(3, 40))
        arr = event_ts + delay
        msgs.append((arr, topic, key, ver, payload))
        if rnd.random() < 0.02:  # at-least-once redelivery of identical payload
            msgs.append((arr + timedelta(minutes=rnd.randint(1, 30)), topic, key, ver, dict(payload)))

    CUTOVER = datetime(2026, 6, 15)  # tracking schema v2 (adds geo) starts here
    eid = cid_n = 0
    for s in ships:
        late_ship = rnd.random() < 0.15
        t0 = datetime.combine(s["ship_date"], datetime.min.time()) + timedelta(
            hours=rnd.randint(8, 17), minutes=rnd.randint(0, 59))
        transit_days = SERVICES[s["svc"]][0]
        deliver = t0 + timedelta(days=transit_days + (rnd.randint(1, 4) if late_ship else rnd.randint(-1, 0)),
                                 hours=rnd.randint(-3, 3))
        plan = [("PICKED_UP", t0, s["o"]),
                ("IN_TRANSIT", t0 + (deliver - t0) * 0.35, "HUB-" + s["o"]),
                ("IN_TRANSIT", t0 + (deliver - t0) * 0.65, "HUB-" + s["dst"]),
                ("OUT_FOR_DELIVERY", deliver - timedelta(hours=rnd.randint(2, 6)), s["dst"]),
                ("DELIVERED", deliver, s["dst"])]
        if rnd.random() < 0.05:
            plan.insert(3, ("EXCEPTION", t0 + (deliver - t0) * 0.8, "HUB-" + s["dst"]))
        for status, ts, loc in plan:
            eid += 1
            ts = ts.replace(microsecond=0)
            p = {"event_id": f"EVT{eid:09d}", "shipment_id": s["sid"], "carrier_id": s["cid"],
                 "status": status, "event_ts": ts.isoformat(), "location": loc}
            ver = "1"
            if ts >= CUTOVER:
                ver = "2"
                p["geo"] = {"lat": round(rnd.uniform(25, 48), 4), "lon": round(rnd.uniform(-123, -70), 4)}
            emit(T_TRK, s["sid"], ts, p, ver)
        # late delivery -> some become claims (nested JSON)
        if deliver.date() > s["promised"] and rnd.random() < 0.35:
            cid_n += 1
            filed = (deliver + timedelta(days=rnd.randint(1, 3))).replace(microsecond=0)
            docs = [{"doc_type": rnd.choice(["POD", "BOL", "PHOTO"]),
                     "uri": f"vol://claims/{s['sid']}_{k}.pdf"} for k in range(rnd.randint(0, 2))]
            emit(T_CLM, s["sid"], filed, {
                "claim_id": f"CLM{cid_n:07d}", "shipment_id": s["sid"], "carrier_id": s["cid"],
                "claim_type": "LATE_DELIVERY", "filed_ts": filed.isoformat(),
                "amount": {"currency": "USD", "value": round(rnd.uniform(50, 400), 2)},
                "reason": "Delivered after promised date", "documents": docs}, "1")
        elif rnd.random() < 0.004:
            cid_n += 1
            filed = (deliver + timedelta(days=rnd.randint(1, 5))).replace(microsecond=0)
            emit(T_CLM, s["sid"], filed, {
                "claim_id": f"CLM{cid_n:07d}", "shipment_id": s["sid"], "carrier_id": s["cid"],
                "claim_type": "DAMAGE", "filed_ts": filed.isoformat(),
                "amount": {"currency": "USD", "value": round(rnd.uniform(200, 3000), 2)},
                "reason": "Damaged in transit", "documents": [{"doc_type": "PHOTO",
                "uri": f"vol://claims/{s['sid']}_dmg.jpg"}]}, "1")

    # invoice lifecycle
    reason_map = {"OVERCHARGE_RATE": "Rate exceeds contract", "FUEL_VARIANCE": "Fuel surcharge mismatch",
                  "WEIGHT_MISMATCH": "Billed weight mismatch",
                  "UNAUTHORIZED_ACCESSORIAL": "Accessorial not authorized",
                  "DUPLICATE_INVOICE": "Duplicate invoice"}
    for inv_no, s, inv_date, total, kind in inv_meta:
        t = datetime.combine(inv_date, datetime.min.time()) + timedelta(hours=rnd.randint(6, 20),
                                                                         minutes=rnd.randint(0, 59))
        def inv_evt(status, ts, reason=None):
            global_n = len(msgs)
            emit(T_INV, inv_no, ts.replace(microsecond=0), {
                "status_event_id": f"IST{global_n:09d}", "invoice_number": inv_no,
                "shipment_id": s["sid"], "carrier_id": s["cid"], "status": status,
                "status_ts": ts.replace(microsecond=0).isoformat(), "total_amount": total,
                "reason": reason}, "1")
        inv_evt("RECEIVED", t)
        dispute_p = 0.8 if kind == "DUPLICATE_INVOICE" else 0.55
        if kind and rnd.random() < dispute_p:
            inv_evt("DISPUTED", t + timedelta(days=rnd.randint(2, 6)), reason_map[kind])
        else:
            ta = t + timedelta(days=rnd.randint(2, 8))
            inv_evt("APPROVED", ta)
            inv_evt("PAID", ta + timedelta(days=rnd.randint(20, 35)))

    # rate change notices (low volume, different schema)
    nid = 0
    for _ in range(60):
        cid_ = rnd.choice(CARRIERS)[0]
        o, dst, svc = rnd.choice(REGIONS), rnd.choice(REGIONS), rnd.choice(list(SERVICES))
        old, new = rate_lookup[(cid_, o, dst, svc, 1)][0], rate_lookup[(cid_, o, dst, svc, 2)][0]
        ann = datetime(2026, 5, 1) + timedelta(days=rnd.randint(0, 50), hours=rnd.randint(8, 17))
        nid += 1
        emit(T_RATE, cid_, ann, {
            "notice_id": f"RCN{nid:05d}", "carrier_id": cid_, "origin_region": o, "dest_region": dst,
            "service_level": svc, "old_rate_per_lb": old, "new_rate_per_lb": new,
            "effective_date": "2026-07-01", "announced_ts": ann.isoformat()}, "1", late_p=0.0)

    # ---- assign partition/offset per topic, write multiplexed Kafka-style envelope ----
    msgs.sort(key=lambda m: m[0])
    offsets, counts, bad = {}, {}, 0
    per = -(-len(msgs) // a.stream_batches)
    os.makedirs(f"{out}/stream", exist_ok=True)
    for b in range(a.stream_batches):
        chunk = msgs[b * per:(b + 1) * per]
        if not chunk:
            break
        with open(f"{out}/stream/kafka_batch_{b + 1:04d}.json", "w") as f:
            for arr, topic, key, ver, payload in chunk:
                part = zlib.crc32(key.encode()) % PARTS
                off = offsets.get((topic, part), 0)
                offsets[(topic, part)] = off + 1
                counts[topic] = counts.get(topic, 0) + 1
                value = json.dumps(payload)
                if topic in (T_TRK, T_INV) and rnd.random() < 0.003:
                    value = value[: len(value) // 2]      # truncated / malformed
                    bad += 1
                f.write(json.dumps({"topic": topic, "partition": part, "offset": off, "key": key,
                                    "value": value, "timestamp": arr.replace(microsecond=0).isoformat(),
                                    "headers": {"schema_version": ver}}) + "\n")
    events = msgs
    print("topic counts:", counts, "malformed:", bad)

    print(f"shipments={len(ships)} invoices={len(inv_rows)} anomalies={len(labels)} "
          f"events={len(events)} rate_rows={len(rate_rows)}")


if __name__ == "__main__":
    main()
