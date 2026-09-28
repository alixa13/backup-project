#!/usr/bin/env bash
# Packages the model team's Modbus Stage 1 delivery into the SHA-pinned bundle
# the online job loads (spec section 7): models/modbus-stage1-detector/v1/ with
# model.onnx, preprocessing.json and thresholds.json byte for byte as
# delivered, plus bundle.json. Refuses a model that does not match the
# delivery's own manifest, and never overwrites a bundle.
# Usage: package-modbus-detector.sh <delivery-dir> [out-root, default: models]
set -euo pipefail

die() { printf 'package-modbus-detector: %s\n' "$*" >&2; exit 1; }

delivery="${1:?usage: package-modbus-detector.sh <delivery-dir> [out-root]}"
out_root="${2:-models}"
name=modbus-stage1-detector
version=v1

# The delivery's four files, where the model team's layout puts them.
onnx="$delivery/models/dual_head_model_fp32.onnx"
manifest="$onnx.manifest.json"
preprocessing="$delivery/contracts/modbus_preprocessing_contract_v1/PREPROCESSING_CONTRACT_V1.json"
thresholds="$delivery/contracts/modbus_dual_head_temporal_dense_detector_v1/THRESHOLD_CONTRACT_V1.json"
for f in "$onnx" "$manifest" "$preprocessing" "$thresholds"; do
  [ -f "$f" ] || die "missing $f"
done

# The ONNX file must be the one the delivery's own manifest records.
expected="$(jq -r .onnx_sha256 "$manifest")"
actual="$(sha256sum "$onnx" | cut -c1-64)"
[ "$expected" = "$actual" ] || die "$onnx does not match the delivery's own manifest (expected $expected, got $actual)"

# Bundles are immutable: a new model is a new version, never an overwrite.
out="$out_root/$name/$version"
[ ! -e "$out" ] || die "$out already exists; bundles are immutable -- package a new version instead"

# The three files, byte for byte, then the manifest that pins them.
mkdir -p "$out"
cp "$onnx" "$out/model.onnx"
cp "$preprocessing" "$out/preprocessing.json"
cp "$thresholds" "$out/thresholds.json"
sha() { sha256sum "$1" | cut -c1-64; }
jq -n \
  --arg name "$name" --arg version "$version" \
  --arg input "$(jq -r '.parity.onnx_input_names[0]' "$manifest")" \
  --arg dense "$(jq -r '.parity.onnx_output_names[0]' "$manifest")" \
  --arg temporal "$(jq -r '.parity.onnx_output_names[1]' "$manifest")" \
  --arg model "$(sha "$out/model.onnx")" \
  --arg prep "$(sha "$out/preprocessing.json")" \
  --arg thr "$(sha "$out/thresholds.json")" \
  '{name: $name, version: $version, schemaId: "modbus-feature-v1", sequenceLength: 20, featureCount: 42,
    inputName: $input, denseOutputName: $dense, temporalOutputName: $temporal,
    modelSha: $model, preprocessingSha: $prep, thresholdsSha: $thr}' > "$out/bundle.json"
# Plain data, whatever mode the delivery's files had (the real ones are 775).
chmod 644 "$out/model.onnx" "$out/preprocessing.json" "$out/thresholds.json" "$out/bundle.json"
printf 'packaged %s\n' "$out"
