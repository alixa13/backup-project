#!/usr/bin/env bash
# Packages an S7comm Stage 1 detector release into the SHA-pinned bundle the
# online job loads (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md
# section 7 and amendment A1): models/s7comm-stage1-detector/<version>/ with
# model.onnx, preprocessing.json, policy.json and calibration.npz byte for byte
# as released, plus bundle.json. The release's files are found through its own
# FROZEN_MANIFEST.json, so both the delivered layout
# (artifacts/v4_causal_final_model/) and v2's (artifacts/model/) package. Refuses
# any file that does not match that manifest, and never overwrites a bundle.
# Usage: package-s7comm-detector.sh <release-dir> <version> [out-root, default: models]
set -euo pipefail

die() { printf 'package-s7comm-detector: %s\n' "$*" >&2; exit 1; }

release="${1:?usage: package-s7comm-detector.sh <release-dir> <version> [out-root]}"
version="${2:?usage: package-s7comm-detector.sh <release-dir> <version> [out-root]}"
out_root="${3:-models}"
name=s7comm-stage1-detector
manifest="$release/FROZEN_MANIFEST.json"
[ -f "$manifest" ] || die "missing $manifest"

# The artifacts directory and the graph, as the release's own manifest lists them:
# artifacts/v4_causal_final_model/ in the delivery, artifacts/model/ in v2.
contract="$(jq -r '.files | keys[] | select(endswith("/preprocessor_contract.json"))' "$manifest")"
[ "$(printf '%s\n' "$contract" | grep -c .)" -eq 1 ] || die "$manifest lists no single preprocessor_contract.json"
artifacts="${contract%/preprocessor_contract.json}"
graph="$(jq -r --arg a "$artifacts/" '.files | keys[] | select(startswith($a) and endswith(".onnx"))' "$manifest")"
[ "$(printf '%s\n' "$graph" | grep -c .)" -eq 1 ] || die "$manifest lists no single ONNX graph under $artifacts"
graph="${graph#"$artifacts"/}"

# Each file must be the one the release's manifest records.
verify() {
  local file="$artifacts/$1" expected actual
  [ -f "$release/$file" ] || die "missing $release/$file"
  expected="$(jq -r --arg f "$file" '.files[$f].sha256 // empty' "$manifest")"
  actual="$(sha256sum "$release/$file" | cut -c1-64)"
  if [ -z "$expected" ] || [ "$expected" != "$actual" ]; then
    die "$release/$file does not match the release's own manifest (expected ${expected:-none}, got $actual)"
  fi
}
for f in "$graph" preprocessor_contract.json causal_online_shadow_policy.json \
         causal_online_conformal_calibration_scores.npz shadow_deployment_manifest.json; do
  verify "$f"
done

# Bundles are immutable: a new model is a new version, never an overwrite.
out="$out_root/$name/$version"
[ ! -e "$out" ] || die "$out already exists; bundles are immutable -- package a new version instead"

# The four runtime files, byte for byte, then the manifest that pins them.
mkdir -p "$out"
cp "$release/$artifacts/$graph" "$out/model.onnx"
cp "$release/$artifacts/preprocessor_contract.json" "$out/preprocessing.json"
cp "$release/$artifacts/causal_online_shadow_policy.json" "$out/policy.json"
cp "$release/$artifacts/causal_online_conformal_calibration_scores.npz" "$out/calibration.npz"
sha() { sha256sum "$1" | cut -c1-64; }
# The graph's input and output names are the release's (input, reconstruction);
# the ONNX scorer checks them against the graph itself when it opens it.
jq -n --arg name "$name" --arg version "$version" \
  --argjson length "$(jq '.score_semantics.sequence_length' "$out/policy.json")" \
  --argjson width "$(jq '.transformed_dimension' "$out/preprocessing.json")" \
  --argjson zero "$(jq -c '.identity_features_excluded_from_event_score' "$release/$artifacts/shadow_deployment_manifest.json")" \
  --arg model "$(sha "$out/model.onnx")" --arg prep "$(sha "$out/preprocessing.json")" \
  --arg policy "$(sha "$out/policy.json")" --arg cal "$(sha "$out/calibration.npz")" \
  '{name: $name, version: $version, schemaId: "s7comm-feature-v1", sequenceLength: $length, featureCount: $width,
    inputName: "input", outputName: "reconstruction", zeroWeightFeatures: $zero,
    modelSha: $model, preprocessingSha: $prep, policySha: $policy, calibrationSha: $cal}' > "$out/bundle.json"
# Plain data, whatever mode the release's files had.
chmod 644 "$out/model.onnx" "$out/preprocessing.json" "$out/policy.json" "$out/calibration.npz" "$out/bundle.json"
printf 'packaged %s\n' "$out"
