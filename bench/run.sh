#!/usr/bin/env bash
# Compares stratus with LocalStack and moto: start-up time with a manifest's
# resources, then throughput, latency and memory under load. All run in Docker
# with the same CPU and memory limits. Results go to bench/results/.
#
#   bench/run.sh                      # defaults below
#   CPUS=2 MEMORY=4g REPS=3 bench/run.sh
#   PHASES=load bench/run.sh          # only the load phase (or PHASES=startup)
#   EMULATORS="stratus moto" bench/run.sh
set -euo pipefail

cd "$(dirname "$0")/.."
CPUS=${CPUS:-1}
MEMORY=${MEMORY:-2g}
REPS=${REPS:-5}
DURATION=${DURATION:-8s}
CONC=${CONC:-1,8,32,128}
LOCALSTACK_IMAGE=${LOCALSTACK_IMAGE:-localstack/localstack:community-archive}
STRATUS_IMAGE=${STRATUS_IMAGE:-stratus:bench}
MOTO_IMAGE=${MOTO_IMAGE:-motoserver/moto:5.2.6}
EMULATORS=${EMULATORS:-stratus localstack moto}
OUT=bench/results
PORT=${PORT:-4566}
LIMITS=(--cpus "$CPUS" --memory "$MEMORY" --memory-swap "$MEMORY")
LOCALSTACK_ENV=(-e SERVICES=s3,kinesis,dynamodb,sqs,stepfunctions,sts -e PERSISTENCE=0 -e DEBUG=0
  -e USE_SINGLE_REGION=1 -e DEFAULT_REGION=eu-central-1)
# moto has no init hooks: bench/moto/start.sh starts moto_server on 4566 and
# creates the manifest's resources with LocalStack's script, inside the container.
MOTO_RUN=(-e DEFAULT_REGION=eu-central-1 -v "$PWD:/work:ro" --entrypoint sh "$MOTO_IMAGE" /work/bench/moto/start.sh)

PHASES=${PHASES:-startup load}
mkdir -p "$OUT"
docker build -q -t "$STRATUS_IMAGE" . >/dev/null
go build -o "$OUT/.startup" ./bench/startup
go build -o "$OUT/.load" ./bench/load

on() { [[ " $EMULATORS " == *" $1 "* ]]; }

startup() { # manifest label
  local manifest=$1 tag=$2
  on stratus && "$OUT/.startup" -label "stratus $tag" -reps "$REPS" -port $PORT -out "$OUT/startup.csv" \
    -ready-url "http://localhost:$PORT/_stratus/ready" -- \
    "${LIMITS[@]}" -v "$PWD:/work:ro" "$STRATUS_IMAGE" -manifest "/work/$manifest"
  on localstack && "$OUT/.startup" -label "localstack $tag" -reps "$REPS" -port $PORT -out "$OUT/startup.csv" \
    -ready-url "http://localhost:$PORT/_localstack/init/ready" -ready-match '"completed": true' -- \
    "${LIMITS[@]}" "${LOCALSTACK_ENV[@]}" -e "STRATUS_MANIFEST=/work/$manifest" -v "$PWD:/work:ro" \
    -v "$PWD/bench/localstack/ready.d:/etc/localstack/init/ready.d:ro" "$LOCALSTACK_IMAGE"
  on moto && "$OUT/.startup" -label "moto $tag" -reps "$REPS" -port $PORT -out "$OUT/startup.csv" \
    -ready-url "http://localhost:$PORT/bench-moto-ready" -- \
    "${LIMITS[@]}" -e "STRATUS_MANIFEST=/work/$manifest" "${MOTO_RUN[@]}"
  if on moto; then verify_moto "$manifest"; fi
}

# verify_moto checks once that moto holds every resource of the manifest.
verify_moto() { # manifest
  docker rm -f stratus-bench-verify >/dev/null 2>&1 || true
  docker run -d --name stratus-bench-verify "${LIMITS[@]}" -e "STRATUS_MANIFEST=/work/$1" "${MOTO_RUN[@]}" >/dev/null
  until docker exec stratus-bench-verify python3 -c \
    'import urllib.request; urllib.request.urlopen("http://localhost:4566/bench-moto-ready")' 2>/dev/null; do sleep 0.2; done
  docker exec stratus-bench-verify python3 /work/bench/moto/verify.py "/work/$1" | sed "s|^|moto $1: |" \
    | tee -a "$OUT/verify.txt"
  docker rm -f stratus-bench-verify >/dev/null
}

load() { # label ready-url docker-run-args...
  local label=$1 ready=$2
  shift 2
  docker rm -f stratus-bench-load >/dev/null 2>&1 || true
  docker run -d --name stratus-bench-load -p $PORT:4566 "${LIMITS[@]}" "$@" >/dev/null
  until curl -sf -m 1 "$ready" >/dev/null; do sleep 0.2; done
  "$OUT/.load" -endpoint "http://localhost:$PORT" -label "$label" -out "$OUT/load.csv" \
    -duration "$DURATION" -conc "$CONC" -container stratus-bench-load || true
  # An emulator killed for exceeding the memory limit shows here.
  docker inspect -f "$label: OOMKilled={{.State.OOMKilled}} exit={{.State.ExitCode}}" stratus-bench-load \
    | tee -a "$OUT/oom.txt"
  docker rm -f -v stratus-bench-load >/dev/null
}

if [[ " $PHASES " == *" startup "* ]]; then
  rm -f "$OUT/startup.csv" "$OUT/verify.txt"
  startup examples/manifest.yaml example
  startup bench/manifest-large.yaml large
fi
if [[ " $PHASES " == *" load "* ]]; then
  rm -f "$OUT/load.csv" "$OUT/oom.txt"
  # A volume at /data, as the README advises: object bytes skip the
  # container's overlay layer.
  if on stratus; then load stratus "http://localhost:$PORT/_stratus/ready" -v /data "$STRATUS_IMAGE"; fi
  if on localstack; then
    load localstack "http://localhost:$PORT/_localstack/health" "${LOCALSTACK_ENV[@]}" "$LOCALSTACK_IMAGE"
  fi
  if on moto; then load moto "http://localhost:$PORT/bench-moto-ready" "${MOTO_RUN[@]}"; fi
fi
echo "limits: --cpus $CPUS --memory $MEMORY; $(docker info --format '{{.NCPU}} CPUs, {{.OperatingSystem}}')" \
  > "$OUT/environment.txt"
rm -f "$OUT/.startup" "$OUT/.load"
echo "results in $OUT/"
