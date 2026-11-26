"""Checks that an emulator holds every resource of a stratus manifest.

    python3 verify.py <manifest.yaml> [endpoint]

Counts tables, queues, streams, buckets and state machines and compares their
names with the manifest's; exits 1 on a difference. Buckets the benchmark
itself creates (bench-moto-ready) are ignored.
"""

import sys

import boto3
import yaml

ENDPOINT = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:4566"


def client(service):
    return boto3.client(service, endpoint_url=ENDPOINT, region_name="eu-central-1",
                        aws_access_key_id="test", aws_secret_access_key="test")


def found():
    tables = client("dynamodb").get_paginator("list_tables").paginate().build_full_result()["TableNames"]
    queues = [u.rsplit("/", 1)[1] for u in client("sqs").list_queues(MaxResults=1000).get("QueueUrls", [])]
    streams = client("kinesis").list_streams()["StreamNames"]
    buckets = [b["Name"] for b in client("s3").list_buckets()["Buckets"] if b["Name"] != "bench-moto-ready"]
    machines = [m["name"] for m in client("stepfunctions").list_state_machines()["stateMachines"]]
    return {"tables": tables, "queues": queues, "streams": streams, "buckets": buckets, "state machines": machines}


def wanted(manifest):
    def names(service, kind):
        return [r["name"] for r in ((manifest.get(service) or {}).get(kind) or [])]
    return {"tables": names("dynamodb", "tables"), "queues": names("sqs", "queues"),
            "streams": names("kinesis", "streams"), "buckets": names("s3", "buckets"),
            "state machines": names("stepFunctions", "stateMachines")}


def main():
    with open(sys.argv[1]) as f:
        want = wanted(yaml.safe_load(f) or {})
    have = found()
    ok = True
    for kind, names in want.items():
        same = sorted(names) == sorted(have[kind])
        ok = ok and same
        print(f"{kind}: manifest {len(names)}, emulator {len(have[kind])}{'' if same else '  MISMATCH'}")
    print(f"total: manifest {sum(map(len, want.values()))}, emulator {sum(map(len, have.values()))}")
    sys.exit(0 if ok else 1)


main()
