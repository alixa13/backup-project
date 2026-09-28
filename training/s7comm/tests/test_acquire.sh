#!/usr/bin/env bash
# acquire.sh with docker, curl and git stubbed on PATH: pins verified, one CSV
# per capture, cached captures not fetched again, and every failure named. The
# real fetch (git, the netresec share, Zeek, the JAR) runs on server3 (Task 10).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091  # deploy's assertion helpers, by a computed path
. "$HERE/../../../deploy/tests/lib.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Stubs: curl writes a pcap whose content is its URL; docker plays Zeek (an
# s7comm.log in /work) and the exporter (a CSV at --out, under /data); each
# records its call.
mkdir -p "$tmp/bin"
cat > "$tmp/bin/curl" <<'EOF'
#!/usr/bin/env bash
echo "curl $*" >> "$STUB_LOG"
while [ $# -gt 0 ]; do case "$1" in -o) out="$2"; shift 2 ;; *) url="$1"; shift ;; esac; done
printf 'pcap:%s' "$url" > "$out"
EOF
cat > "$tmp/bin/docker" <<'EOF'
#!/usr/bin/env bash
echo "docker $*" >> "$STUB_LOG"
work="" data="" out="" entry=""
while [ $# -gt 0 ]; do
  case "$1" in
    --entrypoint) entry="$2"; shift 2 ;;
    -v) case "$2" in *:/work) work="${2%:/work}" ;; *:/data) data="${2%:/data}" ;; esac; shift 2 ;;
    --out) out="$2"; shift 2 ;;
    *) shift ;;
  esac
done
if [ "$entry" = zeek ] && [ -z "${STUB_NO_S7:-}" ]; then echo '{"uid":"C1"}' > "$work/s7comm.log"; fi
if [ "$entry" = java ]; then printf 'capture,uid\nx,C1\n' > "$data/${out#/data/}"; fi
exit 0
EOF
cat > "$tmp/bin/git" <<'EOF'
#!/usr/bin/env bash
echo "git $*" >> "$STUB_LOG"
EOF
chmod +x "$tmp/bin/"*
export PATH="$tmp/bin:$PATH" STUB_LOG="$tmp/calls"
touch "$tmp/online.jar"

# Two pins: one URL, one local file (a read-only copy source).
printf 'local pcap' > "$tmp/local.pcap"
url_sha="$(printf 'pcap:https://example.invalid/a.pcap' | sha256sum | cut -c1-64)"
local_sha="$(sha256sum < "$tmp/local.pcap" | cut -c1-64)"
cat > "$tmp/pins" <<EOF
# capture  sha256  kind  location
cap-a  $url_sha  url  https://example.invalid/a.pcap
cap-b  $local_sha  local  $tmp/local.pcap
EOF
export S7_PINS="$tmp/pins"

# Every capture fetched, run through Zeek and exported; the sums written.
out="$(bash "$HERE/../acquire.sh" "$tmp/data" "$tmp/online.jar" 2>&1)"; status=$?
assert_eq 0 "$status" "a clean acquire succeeds: $out"
assert_eq "cap-a.csv cap-b.csv" "$(cd "$tmp/data/features" && printf '%s ' *.csv | sed 's/ $//')" "one CSV per capture"
assert_eq 2 "$(wc -l < "$tmp/data/features/SHA256SUMS")" "the exports' SHA-256s"
assert_eq 2 "$(grep -c -- '--entrypoint zeek' "$tmp/calls")" "Zeek ran per capture"
assert_eq 4 "$(grep -c -- '--network none' "$tmp/calls")" "Zeek and the exporter run without a network"
assert_eq 1 "$(grep -c '^curl' "$tmp/calls")" "only the URL pin is downloaded"
assert_eq "local pcap" "$(cat "$tmp/local.pcap")" "the local source is untouched"

# A second run fetches nothing again and reuses Zeek's output.
: > "$tmp/calls"
bash "$HERE/../acquire.sh" "$tmp/data" "$tmp/online.jar" >/dev/null 2>&1
assert_eq 0 "$(grep -c '^curl' "$tmp/calls")" "a verified capture is not fetched again"
assert_eq 0 "$(grep -c -- '--entrypoint zeek' "$tmp/calls")" "Zeek's output is reused"
assert_eq 2 "$(grep -c -- '--entrypoint java' "$tmp/calls")" "the exporter always runs (the code may have changed)"

# Only the named captures, when some are named.
: > "$tmp/calls"
bash "$HERE/../acquire.sh" "$tmp/data" "$tmp/online.jar" cap-b >/dev/null 2>&1
assert_eq 1 "$(grep -c -- '--entrypoint java' "$tmp/calls")" "one capture named, one exported"

# Final review M8: Zeek's output is reused only for the very capture it was made from. A
# half-written output left by a killed run, or a capture whose pin changed, runs Zeek again.
: > "$tmp/calls"
rm -f "$tmp/data/zeek/cap-a/.pcap-sha256"
bash "$HERE/../acquire.sh" "$tmp/data" "$tmp/online.jar" cap-a >/dev/null 2>&1
assert_eq 1 "$(grep -c -- '--entrypoint zeek' "$tmp/calls")" "an unstamped Zeek output is not trusted"
printf 'local pcap, recaptured' > "$tmp/local.pcap"
new_sha="$(sha256sum < "$tmp/local.pcap" | cut -c1-64)"
sed -i "s/$local_sha/$new_sha/" "$tmp/pins"
: > "$tmp/calls"
bash "$HERE/../acquire.sh" "$tmp/data" "$tmp/online.jar" cap-b >/dev/null 2>&1
assert_eq 1 "$(grep -c -- '--entrypoint zeek' "$tmp/calls")" "a re-pinned capture runs Zeek again"
assert_eq "$new_sha" "$(cat "$tmp/data/zeek/cap-b/.pcap-sha256")" "the output is stamped with its capture's pin"
sed -i "s/$new_sha/$local_sha/" "$tmp/pins"
printf 'local pcap' > "$tmp/local.pcap"

# A pin that does not match stops the run, naming the capture's file.
sed -i "s/$local_sha/$(printf '0%.0s' {1..64})/" "$tmp/pins"
out="$(bash "$HERE/../acquire.sh" "$tmp/data2" "$tmp/online.jar" 2>&1)"; status=$?
assert_eq 1 "$status" "a changed capture fails"
assert_eq 1 "$(grep -c 'cap-b.pcap does not match its pinned SHA-256' <<< "$out")" "naming the file"

# Zeek writing no s7comm.log stops the run, naming the capture.
sed -i "s/$(printf '0%.0s' {1..64})/$local_sha/" "$tmp/pins"
out="$(STUB_NO_S7=1 bash "$HERE/../acquire.sh" "$tmp/data3" "$tmp/online.jar" 2>&1)"; status=$?
assert_eq 1 "$status" "no S7 records fails"
assert_eq 1 "$(grep -c 'Zeek wrote no s7comm.log for cap-a' <<< "$out")" "naming the capture"

# A missing JAR and an unknown pin kind are named before anything is fetched.
out="$(bash "$HERE/../acquire.sh" "$tmp/data4" "$tmp/nowhere.jar" 2>&1)"; status=$?
assert_eq 1 "$(grep -c 'nowhere.jar is missing' <<< "$out")" "the missing JAR is named"
printf 'cap-c  %s  ftp  somewhere\n' "$local_sha" >> "$tmp/pins"
out="$(bash "$HERE/../acquire.sh" "$tmp/data5" "$tmp/online.jar" cap-c 2>&1)"; status=$?
assert_eq 1 "$(grep -c 'unknown kind ftp for cap-c' <<< "$out")" "the unknown kind is named"

finish
