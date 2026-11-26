"""LocalStack init hook: creates the resources of a stratus manifest.

LocalStack runs it from /etc/localstack/init/ready.d once its services are up.
The manifest path is in STRATUS_MANIFEST. Resources are created with boto3 in
this process, eight at a time, so no AWS CLI start-up is counted: this is the
fastest way LocalStack can be given the same resources stratus starts with.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor

import boto3
import yaml

ENDPOINT = "http://localhost:4566"
REGION = os.environ.get("DEFAULT_REGION", "eu-central-1")
ACCOUNT = "000000000000"


def client(service):
    return boto3.client(service, endpoint_url=ENDPOINT, region_name=REGION,
                        aws_access_key_id="test", aws_secret_access_key="test")


def run_all(fns):
    with ThreadPoolExecutor(max_workers=8) as pool:
        for f in [pool.submit(fn) for fn in fns]:
            f.result()  # an error fails the init, and LocalStack reports it


def queue_arn(name):
    return f"arn:aws:sqs:{REGION}:{ACCOUNT}:{name}"


def create_queue(q):
    attrs = {}
    for key, attr in (("visibilityTimeoutSeconds", "VisibilityTimeout"), ("delaySeconds", "DelaySeconds"),
                      ("receiveWaitTimeSeconds", "ReceiveMessageWaitTimeSeconds")):
        if key in q:
            attrs[attr] = str(q[key])
    if q["name"].endswith(".fifo"):
        attrs["FifoQueue"] = "true"
        if q.get("contentBasedDeduplication"):
            attrs["ContentBasedDeduplication"] = "true"
    if "deadLetter" in q:
        dl = q["deadLetter"]
        attrs["RedrivePolicy"] = json.dumps({"deadLetterTargetArn": queue_arn(dl["queue"]),
                                             "maxReceiveCount": str(dl["maxReceiveCount"])})
    client("sqs").create_queue(QueueName=q["name"], Attributes=attrs)


def create_stream(s):
    k = client("kinesis")
    k.create_stream(StreamName=s["name"], ShardCount=s.get("shards", 1))
    k.get_waiter("stream_exists").wait(StreamName=s["name"])


def create_bucket(b):
    s3 = client("s3")
    s3.create_bucket(Bucket=b["name"], CreateBucketConfiguration={"LocationConstraint": REGION})
    if b.get("versioning"):
        s3.put_bucket_versioning(Bucket=b["name"], VersioningConfiguration={"Status": "Enabled"})
    if b.get("lifecycle"):
        s3.put_bucket_lifecycle_configuration(Bucket=b["name"], LifecycleConfiguration={"Rules": [
            {"ID": f"rule-{i}", "Status": "Enabled", "Filter": {"Prefix": r.get("prefix", "")},
             "Expiration": {"Days": r["expirationDays"]}}
            for i, r in enumerate(b["lifecycle"])]})
    if b.get("notifications"):
        configs = []
        for n in b["notifications"]:
            rules = [{"Name": k, "Value": n[k]} for k in ("prefix", "suffix") if k in n]
            c = {"QueueArn": queue_arn(n["queue"]), "Events": n.get("events", ["s3:ObjectCreated:*"])}
            if rules:
                c["Filter"] = {"Key": {"FilterRules": rules}}
            configs.append(c)
        s3.put_bucket_notification_configuration(Bucket=b["name"],
                                                 NotificationConfiguration={"QueueConfigurations": configs})


def key_schema(index, attrs):
    out = []
    for kind, field in (("HASH", "partitionKey"), ("RANGE", "sortKey")):
        if field in index:
            k = index[field]
            attrs[k["name"]] = k.get("type", "S")
            out.append({"AttributeName": k["name"], "KeyType": kind})
    return out


def projection(index):
    p = {"ProjectionType": index.get("projection", "ALL")}
    if index.get("nonKeyAttributes"):
        p["NonKeyAttributes"] = index["nonKeyAttributes"]
    return p


def create_table(t):
    attrs = {}
    req = {"TableName": t["name"], "BillingMode": "PAY_PER_REQUEST", "KeySchema": key_schema(t, attrs)}
    if t.get("localIndexes"):
        req["LocalSecondaryIndexes"] = [
            {"IndexName": i["name"], "Projection": projection(i),
             "KeySchema": key_schema({"partitionKey": t["partitionKey"], **i}, attrs)}
            for i in t["localIndexes"]]
    if t.get("globalIndexes"):
        req["GlobalSecondaryIndexes"] = [
            {"IndexName": i["name"], "Projection": projection(i), "KeySchema": key_schema(i, attrs)}
            for i in t["globalIndexes"]]
    req["AttributeDefinitions"] = [{"AttributeName": n, "AttributeType": ty} for n, ty in attrs.items()]
    ddb = client("dynamodb")
    ddb.create_table(**req)
    if t.get("ttlAttribute"):
        ddb.update_time_to_live(TableName=t["name"], TimeToLiveSpecification={
            "Enabled": True, "AttributeName": t["ttlAttribute"]})


def create_state_machine(m, base):
    definition = m.get("definition")
    if definition is None:
        with open(os.path.join(base, m["definitionFile"])) as f:
            definition = f.read()
    for name, value in (m.get("substitutions") or {}).items():
        definition = definition.replace("${" + name + "}", value)
    client("stepfunctions").create_state_machine(
        name=m["name"], definition=definition, type=m.get("type", "STANDARD"),
        roleArn=f"arn:aws:iam::{ACCOUNT}:role/stratus-bench")


def main():
    path = os.environ["STRATUS_MANIFEST"]
    with open(path) as f:
        manifest = yaml.safe_load(f) or {}
    base = os.path.dirname(path)
    queues = (manifest.get("sqs") or {}).get("queues") or []
    dlqs = {q["deadLetter"]["queue"] for q in queues if "deadLetter" in q}
    # Dead-letter queues first: a redrive policy names an existing queue.
    run_all([lambda q=q: create_queue(q) for q in queues if q["name"] in dlqs])
    run_all([lambda q=q: create_queue(q) for q in queues if q["name"] not in dlqs])
    run_all(
        [lambda s=s: create_stream(s) for s in ((manifest.get("kinesis") or {}).get("streams") or [])]
        + [lambda b=b: create_bucket(b) for b in ((manifest.get("s3") or {}).get("buckets") or [])]
        + [lambda t=t: create_table(t) for t in ((manifest.get("dynamodb") or {}).get("tables") or [])]
        + [lambda m=m: create_state_machine(m, base)
           for m in ((manifest.get("stepFunctions") or {}).get("stateMachines") or [])])


main()
