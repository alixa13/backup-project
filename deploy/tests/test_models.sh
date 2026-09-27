#!/usr/bin/env bash
# Pins deploy/models/package-modbus-detector.sh on a small fake delivery:
# the bundle's layout and bundle.json, the check of the ONNX file against the
# delivery's own manifest, and that bundles are never overwritten.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
script="$HERE/../models/package-modbus-detector.sh"

# A fake delivery in the real delivery's layout.
fake_delivery() {
  local d="$1"
  mkdir -p "$d/models" "$d/contracts/modbus_preprocessing_contract_v1" \
    "$d/contracts/modbus_dual_head_temporal_dense_detector_v1"
  printf 'not really onnx' > "$d/models/dual_head_model_fp32.onnx"
  # The real delivery's files are executable (mode 775); the bundle's must not be.
  chmod 775 "$d/models/dual_head_model_fp32.onnx"
  printf '{"feature_order":["a"]}' > "$d/contracts/modbus_preprocessing_contract_v1/PREPROCESSING_CONTRACT_V1.json"
  printf '{"dense":{"threshold":0.25}}' > "$d/contracts/modbus_dual_head_temporal_dense_detector_v1/THRESHOLD_CONTRACT_V1.json"
  jq -n --arg sha "$(sha256sum "$d/models/dual_head_model_fp32.onnx" | cut -c1-64)" \
    '{onnx_sha256: $sha, parity: {onnx_input_names: ["sequence_20x42"],
      onnx_output_names: ["modbus_dense_autoencoder", "modbus_causal_next_event_predictor"]}}' \
    > "$d/models/dual_head_model_fp32.onnx.manifest.json"
}

fake_delivery "$tmp/delivery"
out="$(bash "$script" "$tmp/delivery" "$tmp/out" 2>&1)"; status=$?
b="$tmp/out/modbus-stage1-detector/v1"
assert_eq 0 "$status" "packaging a sound delivery succeeds"
assert_eq "bundle.json model.onnx preprocessing.json thresholds.json" "$(cd "$b" && printf '%s\n' * | sort | tr '\n' ' ' | sed 's/ $//')" "the bundle's four files"
assert_eq modbus-stage1-detector "$(jq -r .name "$b/bundle.json")" "name"
assert_eq v1 "$(jq -r .version "$b/bundle.json")" "version"
assert_eq modbus-feature-v1 "$(jq -r .schemaId "$b/bundle.json")" "schemaId"
assert_eq 20 "$(jq -r .sequenceLength "$b/bundle.json")" "sequenceLength"
assert_eq 42 "$(jq -r .featureCount "$b/bundle.json")" "featureCount"
assert_eq sequence_20x42 "$(jq -r .inputName "$b/bundle.json")" "inputName from the delivery's manifest"
assert_eq modbus_causal_next_event_predictor "$(jq -r .temporalOutputName "$b/bundle.json")" "temporalOutputName"
assert_eq "$(sha256sum "$b/model.onnx" | cut -c1-64)" "$(jq -r .modelSha "$b/bundle.json")" "modelSha"
assert_eq "$(sha256sum "$b/preprocessing.json" | cut -c1-64)" "$(jq -r .preprocessingSha "$b/bundle.json")" "preprocessingSha"
assert_eq "$(sha256sum "$b/thresholds.json" | cut -c1-64)" "$(jq -r .thresholdsSha "$b/bundle.json")" "thresholdsSha"
assert_eq "$(sha256sum "$tmp/delivery/models/dual_head_model_fp32.onnx" | cut -c1-64)" \
  "$(sha256sum "$b/model.onnx" | cut -c1-64)" "the model is copied byte for byte"
assert_eq "644 644 644 644" "$(cd "$b" && stat -c %a bundle.json model.onnx preprocessing.json thresholds.json | tr '\n' ' ' | sed 's/ $//')" "bundle files are plain data, never executable"

# A bundle is immutable: packaging over an existing one is refused.
out="$(bash "$script" "$tmp/delivery" "$tmp/out" 2>&1)"; status=$?
assert_eq 1 "$status" "an existing bundle is never overwritten"
assert_eq 1 "$(grep -c 'already exists' <<< "$out")" "and says so"

# A corrupted model never gets packaged.
fake_delivery "$tmp/bad"
printf 'tampered' >> "$tmp/bad/models/dual_head_model_fp32.onnx"
out="$(bash "$script" "$tmp/bad" "$tmp/out2" 2>&1)"; status=$?
assert_eq 1 "$status" "a model that does not match the delivery's manifest is refused"
assert_eq 1 "$(grep -c "does not match the delivery's own manifest" <<< "$out")" "and says why"
assert_eq no "$([ -e "$tmp/out2/modbus-stage1-detector" ] && echo yes || echo no)" "and writes nothing"

finish
