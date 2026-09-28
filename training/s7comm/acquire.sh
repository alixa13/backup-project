#!/usr/bin/env bash
# The S7comm detector v2's training data (docs/superpowers/specs/2026-09-28-s7comm-detector-v2-design.md
# sections 4 and 5). Every capture pinned in sources.sha256 is fetched or copied and SHA-256-verified.
# It then runs through the deployed Zeek image offline (the sensor's own packages and JSON naming), and
# then through S7commFeatureExport, the platform's own parser, mapper and feature code.
# Output: DATA_DIR/features/<capture>.csv, the pipeline's only input.
# Runs on server3; the development machine never runs Zeek over these captures.
# Usage: acquire.sh DATA_DIR ONLINE_JAR [CAPTURE ...]   (no CAPTURE: every pinned one)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PINS="${S7_PINS:-$HERE/sources.sha256}"
ZEEK_IMAGE="${ZEEK_IMAGE:-netsec-ml/zeek:1}"
FLINK_IMAGE="${FLINK_IMAGE:-flink:2.2.1-java21}"
QUT_REPO=https://github.com/qut-infosec/2017QUT_S7comm.git
QUT_COMMIT=afac2b6f776bbe0f9f5bc58d0898e8516bc72561
ITI_REPO=https://github.com/ITI/ICS-Security-Tools.git
ITI_COMMIT=9b826091e7ba3fbdd5997d31e116f29e09cbbb48

die() { printf 'acquire: %s\n' "$*" >&2; exit 1; }
log() { printf 'acquire: %s\n' "$*"; }

data="${1:?usage: acquire.sh DATA_DIR ONLINE_JAR [CAPTURE ...]}"
jar="${2:?usage: acquire.sh DATA_DIR ONLINE_JAR [CAPTURE ...]}"
shift 2
[ -f "$jar" ] || die "the online job JAR $jar is missing: build it first (./mvnw package -pl modules/bootstrap-online-job -am -DskipTests)"
mkdir -p "$data"/pcap "$data"/repos "$data"/zeek "$data"/features "$data"/rejects
data="$(cd "$data" && pwd)"
jar_dir="$(cd "$(dirname "$jar")" && pwd)"
jar_name="$(basename "$jar")"

# pinned FILE SHA256: does FILE exist with exactly this SHA-256?
pinned() { [ -f "$1" ] && printf '%s  %s\n' "$2" "$1" | sha256sum -c --quiet - >/dev/null 2>&1; }

# clone_at DIR URL COMMIT: a shallow checkout of exactly COMMIT, reused when already there.
clone_at() {
  if [ "$(git -C "$1" rev-parse HEAD 2>/dev/null || true)" != "$3" ]; then
    rm -rf "$1"
    git init -q "$1"
    git -C "$1" fetch -q --depth 1 "$2" "$3"
    git -C "$1" checkout -q FETCH_HEAD
  fi
}

# fetch CAPTURE SHA KIND LOCATION: DATA/pcap/CAPTURE.pcap, verified (kept when already verified).
fetch() {
  local capture="$1" sha="$2" kind="$3" location="$4" out="$data/pcap/$1.pcap"
  pinned "$out" "$sha" && return 0
  case "$kind" in
    url) curl -fsSL --retry 3 -o "$out.part" "$location" && mv "$out.part" "$out" ;;
    local) cp "$location" "$out" ;;  # another project's file: read, never modified
    iti) clone_at "$data/repos/iti" "$ITI_REPO" "$ITI_COMMIT"; cp "$data/repos/iti/$location" "$out" ;;
    qut-zip)
      clone_at "$data/repos/qut" "$QUT_REPO" "$QUT_COMMIT"
      # The zip's single member (server3 has no unzip; Python's zipfile is enough).
      python3 - "$data/repos/qut/$location" "$out" <<'PY'
import sys, zipfile
z = zipfile.ZipFile(sys.argv[1])
(member,) = z.namelist()
with open(sys.argv[2], "wb") as f:
    f.write(z.read(member))
PY
      ;;
    *) die "unknown kind $kind for $capture" ;;
  esac
  pinned "$out" "$sha" || die "$out does not match its pinned SHA-256 $sha"
}

# run_zeek CAPTURE: the deployed image's offline JSON policy over the capture, no network.
run_zeek() {
  local out="$data/zeek/$1"
  [ -s "$out/s7comm.log" ] && return 0
  rm -rf "$out"
  mkdir -p "$out"
  docker run --rm --network none --user "$(id -u):$(id -g)" --entrypoint zeek \
    -v "$data/pcap:/pcap:ro" -v "$out:/work" -w /work \
    "$ZEEK_IMAGE" -C -r "/pcap/$1.pcap" /opt/netsec/offline-json.zeek
  [ -s "$out/s7comm.log" ] || die "Zeek wrote no s7comm.log for $1"
}

# run_export CAPTURE: S7commFeatureExport on the Flink image's Java 21, no network.
run_export() {
  docker run --rm --network none --user "$(id -u):$(id -g)" --entrypoint java \
    -v "$jar_dir:/jars:ro" -v "$data:/data" "$FLINK_IMAGE" \
    -cp "/jars/$jar_name" io.netsecml.platform.bootstrap.online.S7commFeatureExport \
    --out "/data/features/$1.csv" --rejects "/data/rejects/$1.jsonl" "$1=/data/zeek/$1/s7comm.log"
}

# The pins, filtered to the named captures when some are named.
selected=0
while read -r capture sha kind location; do
  case "$capture" in ''|'#'*) continue ;; esac
  if [ $# -gt 0 ] && ! printf '%s\n' "$@" | grep -qxF "$capture"; then continue; fi
  selected=$((selected + 1))
  log "$capture: fetch"
  fetch "$capture" "$sha" "$kind" "$location"
  log "$capture: zeek"
  run_zeek "$capture"
  log "$capture: export"
  run_export "$capture"
done < "$PINS"
[ "$selected" -gt 0 ] || die "no pinned capture matches: $*"

# The exports' own SHA-256s, which the run's evaluation summary records again.
(cd "$data/features" && sha256sum -- *.csv > SHA256SUMS)
log "done: $selected captures in $data/features"
