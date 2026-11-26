#!/bin/sh
# Entrypoint for moto in the benchmark. moto has no manifest and no init hooks,
# so this starts moto_server, then creates the manifest's resources inside the
# container with the same boto3 script LocalStack runs as its init hook
# (bench/localstack/ready.d/from-manifest.py, eight at a time), and last
# creates the bucket bench-moto-ready, which the benchmark polls for.
#
#   docker run -e STRATUS_MANIFEST=/work/m.yaml -v "$PWD:/work:ro" \
#     --entrypoint sh motoserver/moto:<tag> /work/bench/moto/start.sh
set -eu
# The manifest's ARNs and queue URLs use LocalStack's and stratus's account.
export MOTO_ACCOUNT_ID=${MOTO_ACCOUNT_ID:-000000000000}
moto_server -H 0.0.0.0 -p 4566 &
moto=$!
python3 - "${STRATUS_MANIFEST:-}" <<'PY'
import os, runpy, sys, time, urllib.request

while True:
    try:
        urllib.request.urlopen("http://localhost:4566/moto-api/", timeout=1)
        break
    except Exception:
        time.sleep(0.01)
if sys.argv[1]:
    runpy.run_path("/work/bench/localstack/ready.d/from-manifest.py")
import boto3
boto3.client("s3", endpoint_url="http://localhost:4566", region_name="us-east-1",
             aws_access_key_id="test", aws_secret_access_key="test").create_bucket(Bucket="bench-moto-ready")
PY
wait $moto
