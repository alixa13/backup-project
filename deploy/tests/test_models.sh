#!/usr/bin/env bash
# Pins deploy/models/package-modbus-detector.sh on a small fake delivery:
# the bundle's layout and bundle.json, the check of the ONNX file against the
# delivery's own manifest, and that bundles are never overwritten; and
# deploy/models/package-s7comm-detector.sh the same way, against the release's
# FROZEN_MANIFEST.json.
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

# --- package-s7comm-detector.sh (scoring spec section 7 and amendment A1) ---
s7script="$HERE/../models/package-s7comm-detector.sh"

# A fake release in DIR, its FROZEN_MANIFEST.json recording every file's SHA-256 as
# the release tooling does. ART and GRAPH give its layout: v2's (artifacts/model,
# s7comm_lstm_autoencoder.onnx) or the delivered model's (artifacts/v4_causal_final_model,
# s7comm_lstm_autoencoder_debiased.onnx).
fake_s7_release() {
  local rel="$1" art="$2" graph="$3" files='{}' f
  mkdir -p "$rel/$art"
  printf 'not really onnx: %s' "$graph" > "$rel/$art/$graph"
  chmod 775 "$rel/$art/$graph"
  printf '{"transformed_dimension":22}' > "$rel/$art/preprocessor_contract.json"
  printf '{"score_semantics":{"sequence_length":16}}' > "$rel/$art/causal_online_shadow_policy.json"
  printf 'not really npz' > "$rel/$art/causal_online_conformal_calibration_scores.npz"
  printf '{"identity_features_excluded_from_event_score":["s7_operation"]}' > "$rel/$art/shadow_deployment_manifest.json"
  for f in "$graph" preprocessor_contract.json causal_online_shadow_policy.json \
           causal_online_conformal_calibration_scores.npz shadow_deployment_manifest.json; do
    files="$(jq --arg k "$art/$f" --arg s "$(sha256sum "$rel/$art/$f" | cut -c1-64)" '. + {($k): {sha256: $s}}' <<< "$files")"
  done
  jq -n --argjson files "$files" '{files: $files}' > "$rel/FROZEN_MANIFEST.json"
}

fake_s7_release "$tmp/s7" artifacts/model s7comm_lstm_autoencoder.onnx
out="$(bash "$s7script" "$tmp/s7" v2 "$tmp/s7out" 2>&1)"; status=$?
s="$tmp/s7out/s7comm-stage1-detector/v2"
assert_eq 0 "$status" "packaging a sound S7 release succeeds: $out"
assert_eq "bundle.json calibration.npz model.onnx policy.json preprocessing.json" "$(cd "$s" && printf '%s\n' * | sort | tr '\n' ' ' | sed 's/ $//')" "the S7 bundle's five files"
assert_eq s7comm-stage1-detector "$(jq -r .name "$s/bundle.json")" "S7 name"
assert_eq v2 "$(jq -r .version "$s/bundle.json")" "S7 version from the argument"
assert_eq s7comm-feature-v1 "$(jq -r .schemaId "$s/bundle.json")" "S7 schemaId"
assert_eq 16 "$(jq -r .sequenceLength "$s/bundle.json")" "sequenceLength from the policy"
assert_eq 22 "$(jq -r .featureCount "$s/bundle.json")" "featureCount from the preprocessing contract"
assert_eq input "$(jq -r .inputName "$s/bundle.json")" "inputName"
assert_eq reconstruction "$(jq -r .outputName "$s/bundle.json")" "outputName"
assert_eq '["s7_operation"]' "$(jq -c .zeroWeightFeatures "$s/bundle.json")" "zeroWeightFeatures from the deployment manifest"
for pair in model.onnx:modelSha preprocessing.json:preprocessingSha policy.json:policySha calibration.npz:calibrationSha; do
  assert_eq "$(sha256sum "$s/${pair%%:*}" | cut -c1-64)" "$(jq -r ".${pair#*:}" "$s/bundle.json")" "${pair#*:}"
done
assert_eq "not really onnx: s7comm_lstm_autoencoder.onnx" "$(cat "$s/model.onnx")" "the graph the manifest lists is the model"
assert_eq "$(sha256sum "$tmp/s7/artifacts/model/causal_online_conformal_calibration_scores.npz" | cut -c1-64)" \
  "$(sha256sum "$s/calibration.npz" | cut -c1-64)" "the calibration scores are copied byte for byte"
assert_eq "644 644 644 644 644" "$(cd "$s" && stat -c %a bundle.json calibration.npz model.onnx policy.json preprocessing.json | tr '\n' ' ' | sed 's/ $//')" "S7 bundle files are plain data"

# The delivered model's layout packages too, found through its own manifest.
fake_s7_release "$tmp/s7old" artifacts/v4_causal_final_model s7comm_lstm_autoencoder_debiased.onnx
out="$(bash "$s7script" "$tmp/s7old" v1 "$tmp/s7out" 2>&1)"; status=$?
assert_eq 0 "$status" "the delivered layout packages too: $out"
assert_eq "not really onnx: s7comm_lstm_autoencoder_debiased.onnx" "$(cat "$tmp/s7out/s7comm-stage1-detector/v1/model.onnx")" "its graph is the model"

# Immutable, and a tampered file never gets packaged.
out="$(bash "$s7script" "$tmp/s7" v2 "$tmp/s7out" 2>&1)"; status=$?
assert_eq 1 "$status" "an existing S7 bundle is never overwritten"
fake_s7_release "$tmp/s7bad" artifacts/model s7comm_lstm_autoencoder.onnx
printf 'tampered' >> "$tmp/s7bad/artifacts/model/causal_online_shadow_policy.json"
out="$(bash "$s7script" "$tmp/s7bad" v2 "$tmp/s7out2" 2>&1)"; status=$?
assert_eq 1 "$status" "a file that does not match the release's manifest is refused"
assert_eq 1 "$(grep -c "causal_online_shadow_policy.json does not match the release's own manifest" <<< "$out")" "naming it"
assert_eq no "$([ -e "$tmp/s7out2/s7comm-stage1-detector" ] && echo yes || echo no)" "and writes nothing"

# A manifest that lists no ONNX graph is refused, naming the manifest.
fake_s7_release "$tmp/s7nograph" artifacts/model s7comm_lstm_autoencoder.onnx
jq 'del(.files["artifacts/model/s7comm_lstm_autoencoder.onnx"])' "$tmp/s7nograph/FROZEN_MANIFEST.json" > "$tmp/m.json" \
  && mv "$tmp/m.json" "$tmp/s7nograph/FROZEN_MANIFEST.json"
out="$(bash "$s7script" "$tmp/s7nograph" v2 "$tmp/s7out3" 2>&1)"; status=$?
assert_eq 1 "$status" "a release without a listed graph is refused"
assert_eq 1 "$(grep -c "lists no single ONNX graph" <<< "$out")" "saying so"

finish
