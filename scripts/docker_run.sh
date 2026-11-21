#!/bin/sh
# Run the image the way the scorer does: docker_run.sh DATA_DIR OUTPUT_DIR
# Same flags as the scorer box, except the resource limits come from CPUS / MEM.
#
# /scratch is always a fresh Docker-managed volume, owned by uid 1000. Host bind
# mounts are unreliable under VM-based Docker (Colima, Docker Desktop): a host
# path outside the VM's shared folders (macOS mktemp and tempfile dirs live in
# /var/folders, Colima shares only $HOME) appears in the container as an empty,
# root-owned directory that uid 1000 cannot write.
#
# /output is bind-mounted when a probe shows that uid 1000 can write to it and
# that the host sees the write. Otherwise it is a Docker volume and the results
# are copied to OUTPUT_DIR with `docker cp`, so the host user owns them.
# Set OUTPUT_MODE=bind or OUTPUT_MODE=volume to skip the probe.
#
# Every volume and helper container is removed on exit, whatever the outcome.
set -eu
IMAGE="${IMAGE:-shopping-sessions:latest}"
CPUS="${CPUS:-8}"
MEM="${MEM:-24g}"
PLATFORM="${PLATFORM:-linux/amd64}"
OUTPUT_MODE="${OUTPUT_MODE:-auto}"
DATA=$(cd "$1" && pwd)
mkdir -p "$2"; OUT=$(cd "$2" && pwd)

TAG="shopping-$$-$(date +%s)-$(od -An -N4 -tx4 /dev/urandom | tr -d ' ')"
SCRATCH_VOL="$TAG-scratch"
OUT_VOL="$TAG-output"
COPY_CTR="$TAG-copy"

cleanup() {
  docker rm -f "$COPY_CTR" >/dev/null 2>&1 || true
  docker volume rm -f "$SCRATCH_VOL" "$OUT_VOL" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# A fresh volume owned by uid 1000 (only this helper runs as root; the pipeline never does).
# It also checks that DATA_DIR is really visible inside the VM: an unshared host path would
# otherwise mount as an empty /data and the run would "succeed" with empty tables.
new_volume() {
  docker volume create "$1" >/dev/null
  docker run --rm --platform "$PLATFORM" --network none --user 0:0 \
    -v "$1":/v -v "$DATA":/data:ro,z --entrypoint sh "$IMAGE" -c \
    'chown 1000:1000 /v && test -f /data/manifest.json' || {
      echo "docker_run: cannot prepare volume $1, or $DATA/manifest.json is not visible in the container" \
           "(is $DATA inside the Docker VM's shared folders?)" >&2
      exit 1
    }
}

# Can uid 1000 write to a bind mount of OUT, with the host seeing the file?
bind_output_works() {
  probe=".bind-probe-$TAG"
  docker run --rm --platform "$PLATFORM" --network none --user 1000:1000 \
    -v "$OUT":/output:z --entrypoint sh "$IMAGE" -c "touch /output/$probe" >/dev/null 2>&1 || return 1
  if [ -e "$OUT/$probe" ]; then
    rm -f "$OUT/$probe" 2>/dev/null || true
    return 0
  fi
  return 1
}

new_volume "$SCRATCH_VOL"
case "$OUTPUT_MODE" in
  bind) USE_BIND=1 ;;
  volume) USE_BIND=0 ;;
  auto) if bind_output_works; then USE_BIND=1; else USE_BIND=0; fi ;;
  *) echo "OUTPUT_MODE must be auto, bind or volume" >&2; exit 2 ;;
esac
if [ "$USE_BIND" = 1 ]; then
  OUT_MOUNT="$OUT:/output:z"
else
  new_volume "$OUT_VOL"
  OUT_MOUNT="$OUT_VOL:/output"
fi
echo "docker_run: scratch=volume output=$([ "$USE_BIND" = 1 ] && echo bind || echo volume)" >&2

# The pipeline itself: scorer flags, unchanged.
status=0
docker run --rm --platform "$PLATFORM" \
  --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges \
  --pids-limit 4096 --cpus "$CPUS" --memory "$MEM" --memory-swap "$MEM" \
  --user 1000:1000 \
  -v "$DATA":/data:ro,z -v "$OUT_MOUNT" -v "$SCRATCH_VOL":/scratch \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
  -e NCPU="${CPUS%.*}" -e MEM_GB="${MEM%g}" \
  "$IMAGE" || status=$?

# Volume mode: copy whatever the run wrote to the host (also after a failure, for debugging).
if [ "$USE_BIND" = 0 ]; then
  docker create --platform "$PLATFORM" --name "$COPY_CTR" -v "$OUT_VOL":/output "$IMAGE" >/dev/null
  docker cp "$COPY_CTR":/output/. "$OUT"
fi
exit "$status"
