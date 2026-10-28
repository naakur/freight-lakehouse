"""
Replays the generated freight messages (stream/kafka_batch_*.json) into Confluent Cloud topics.

Credentials come from ENVIRONMENT VARIABLES only - never from a file in the repo:
    KAFKA_BOOTSTRAP   e.g. pkc-xxxxx.region.provider.confluent.cloud:9092
    KAFKA_API_KEY
    KAFKA_API_SECRET

Usage (PowerShell):
    $env:KAFKA_BOOTSTRAP="..."; $env:KAFKA_API_KEY="..."; $env:KAFKA_API_SECRET="..."
    python scripts/kafka_producer.py --data-dir "D:\\...\\data\\stream" --files 2          # small first try
    python scripts/kafka_producer.py --data-dir "D:\\...\\data\\stream"                     # everything
    python scripts/kafka_producer.py --data-dir ... --skip-files 2                         # the rest, after a 2-file test
    python scripts/kafka_producer.py --data-dir ... --dry-run                              # no Kafka, just count

At the end it writes a MANIFEST (how many messages were really delivered per topic).
Upload the manifest to the landing volume; the stream audit compares it with bronze:
    databricks fs cp manifests/<file>.json dbfs:/Volumes/freight_dev/landing/raw/stream_manifest/
"""
import argparse
import glob
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone


def to_epoch_ms(iso_text: str) -> int:
    """'2026-03-01T08:25:00' -> milliseconds since 1970 (treated as UTC)."""
    return int(datetime.fromisoformat(iso_text).replace(tzinfo=timezone.utc).timestamp() * 1000)


def build_producer():
    from confluent_kafka import Producer  # imported here so --dry-run needs no install
    return Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
        "acks": "all",                 # wait until the broker has safely stored the message
        "enable.idempotence": True,    # a network retry will not create a duplicate
        "linger.ms": 50,               # small batching for speed
    })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True, help="folder containing kafka_batch_*.json")
    ap.add_argument("--files", type=int, default=0, help="only send N files (0 = all of them)")
    ap.add_argument("--skip-files", type=int, default=0, help="skip the first K files (to send the rest after a test)")
    ap.add_argument("--manifest-dir", default="manifests")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    files = sorted(glob.glob(os.path.join(a.data_dir, "kafka_batch_*.json")))
    files = files[a.skip_files:]
    if a.files:
        files = files[: a.files]
    if not files:
        sys.exit(f"No kafka_batch_*.json files found in {a.data_dir}")

    producer = None if a.dry_run else build_producer()
    sent, failed = Counter(), Counter()

    def on_delivery(err, msg):
        # Called by the client for every message: count what REALLY reached the broker
        if err is None:
            sent[msg.topic()] += 1
        else:
            failed[msg.topic()] += 1

    for path in files:
        with open(path) as f:
            for line in f:
                m = json.loads(line)
                if a.dry_run:
                    sent[m["topic"]] += 1
                    continue
                producer.produce(
                    topic=m["topic"],
                    key=m["key"],                                   # same key -> same partition -> per-key ordering
                    value=m["value"],                               # the JSON text (some are deliberately broken)
                    headers=[("schema_version", m["headers"]["schema_version"].encode())],
                    timestamp=to_epoch_ms(m["timestamp"]),
                    on_delivery=on_delivery,
                )
                producer.poll(0)                                    # serve delivery callbacks
        print("read", os.path.basename(path))

    if producer:
        producer.flush(60)                                          # wait for outstanding messages

    os.makedirs(a.manifest_dir, exist_ok=True)
    name = f"manifest_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.json"
    manifest = {"sent": dict(sent), "failed": dict(failed), "dry_run": a.dry_run}
    with open(os.path.join(a.manifest_dir, name), "w") as f:
        json.dump(manifest, f, indent=2)
    print(json.dumps(manifest, indent=2))
    print("manifest:", os.path.join(a.manifest_dir, name))
    if failed:
        sys.exit("Some messages failed - see the manifest")


if __name__ == "__main__":
    main()
