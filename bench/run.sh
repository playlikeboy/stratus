#!/usr/bin/env bash
# Compares stratus with LocalStack: start-up time with a manifest's resources,
# then throughput, latency and memory under load. Both run in Docker with the
# same CPU and memory limits. Results go to bench/results/.
#
#   bench/run.sh                      # defaults below
#   CPUS=2 MEMORY=4g REPS=3 bench/run.sh
#   PHASES=load bench/run.sh          # only the load phase (or PHASES=startup)
set -euo pipefail

cd "$(dirname "$0")/.."
CPUS=${CPUS:-1}
MEMORY=${MEMORY:-2g}
REPS=${REPS:-5}
DURATION=${DURATION:-8s}
CONC=${CONC:-1,8,32,128}
LOCALSTACK_IMAGE=${LOCALSTACK_IMAGE:-localstack/localstack:community-archive}
STRATUS_IMAGE=${STRATUS_IMAGE:-stratus:bench}
OUT=bench/results
PORT=4566
LIMITS=(--cpus "$CPUS" --memory "$MEMORY" --memory-swap "$MEMORY")
LOCALSTACK_ENV=(-e SERVICES=s3,kinesis,dynamodb,sqs,stepfunctions,sts -e PERSISTENCE=0 -e DEBUG=0
  -e USE_SINGLE_REGION=1 -e DEFAULT_REGION=eu-central-1)

PHASES=${PHASES:-startup load}
mkdir -p "$OUT"
docker build -q -t "$STRATUS_IMAGE" . >/dev/null
go build -o "$OUT/.startup" ./bench/startup
go build -o "$OUT/.load" ./bench/load

startup() { # manifest label
  local manifest=$1 tag=$2
  "$OUT/.startup" -label "stratus $tag" -reps "$REPS" -port $PORT -out "$OUT/startup.csv" \
    -ready-url "http://localhost:$PORT/_stratus/ready" -- \
    "${LIMITS[@]}" -v "$PWD:/work:ro" "$STRATUS_IMAGE" -manifest "/work/$manifest"
  "$OUT/.startup" -label "localstack $tag" -reps "$REPS" -port $PORT -out "$OUT/startup.csv" \
    -ready-url "http://localhost:$PORT/_localstack/init/ready" -ready-match '"completed": true' -- \
    "${LIMITS[@]}" "${LOCALSTACK_ENV[@]}" -e "STRATUS_MANIFEST=/work/$manifest" -v "$PWD:/work:ro" \
    -v "$PWD/bench/localstack/ready.d:/etc/localstack/init/ready.d:ro" "$LOCALSTACK_IMAGE"
}

load() { # label ready-url docker-run-args...
  local label=$1 ready=$2
  shift 2
  docker rm -f stratus-bench-load >/dev/null 2>&1 || true
  docker run -d --name stratus-bench-load -p $PORT:4566 "${LIMITS[@]}" "$@" >/dev/null
  until curl -sf "$ready" >/dev/null; do sleep 0.2; done
  "$OUT/.load" -endpoint "http://localhost:$PORT" -label "$label" -out "$OUT/load.csv" \
    -duration "$DURATION" -conc "$CONC" -container stratus-bench-load || true
  # An emulator killed for exceeding the memory limit shows here.
  docker inspect -f "$label: OOMKilled={{.State.OOMKilled}} exit={{.State.ExitCode}}" stratus-bench-load \
    | tee -a "$OUT/oom.txt"
  docker rm -f -v stratus-bench-load >/dev/null
}

if [[ " $PHASES " == *" startup "* ]]; then
  rm -f "$OUT/startup.csv"
  startup examples/manifest.yaml example
  startup bench/manifest-large.yaml large
fi
if [[ " $PHASES " == *" load "* ]]; then
  rm -f "$OUT/load.csv" "$OUT/oom.txt"
  # A volume at /data, as the README advises: object bytes skip the
  # container's overlay layer.
  load stratus "http://localhost:$PORT/_stratus/ready" -v /data "$STRATUS_IMAGE"
  load localstack "http://localhost:$PORT/_localstack/health" "${LOCALSTACK_ENV[@]}" "$LOCALSTACK_IMAGE"
fi
echo "limits: --cpus $CPUS --memory $MEMORY; $(docker info --format '{{.NCPU}} CPUs, {{.OperatingSystem}}')" \
  > "$OUT/environment.txt"
rm -f "$OUT/.startup" "$OUT/.load"
echo "results in $OUT/"
