package io.netsecml.platform.adapter.registry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.netsecml.platform.domain.feature.FeatureDefinition;
import io.netsecml.platform.domain.feature.FeatureSchema;
import io.netsecml.platform.domain.feature.FeatureSchemaRegistry;
import io.netsecml.platform.domain.model.FeaturePreprocessing;
import io.netsecml.platform.domain.model.ModbusPreprocessing;
import io.netsecml.platform.domain.model.PreprocessingPolicy;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

// Loads a sequence-detector bundle (contracts/model/sequence-detector-bundle-v1.json)
// and refuses it unless every check of spec section 7 passes: each file's
// SHA-256, the feature schema, and the preprocessing's feature order. The
// graph's names and shapes are checked by the ONNX scorer that opens it.
public final class SequenceDetectorBundleLoader {

    // The delivered preprocessing contract writes NaN for unused means and
    // stds -- not strict JSON -- so the mapper must accept it.
    private static final JsonMapper MAPPER = JsonMapper.builder()
        .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
        .build();

    private SequenceDetectorBundleLoader() {
    }

    public static LoadedSequenceDetector load(Path bundleDir) throws IOException {
        BundleJson bundle = MAPPER.readValue(bundleDir.resolve("bundle.json").toFile(), BundleJson.class);
        byte[] model = Files.readAllBytes(bundleDir.resolve("model.onnx"));
        byte[] preprocessing = Files.readAllBytes(bundleDir.resolve("preprocessing.json"));
        byte[] thresholds = Files.readAllBytes(bundleDir.resolve("thresholds.json"));

        // Every file must be exactly the one bundle.json pins.
        requireSha("model.onnx", bundle.modelSha, model);
        requireSha("preprocessing.json", bundle.preprocessingSha, preprocessing);
        requireSha("thresholds.json", bundle.thresholdsSha, thresholds);

        // The schema must be one this platform builds, and the preprocessing
        // must cover its features in its order.
        FeatureSchema schema = FeatureSchemaRegistry.byId(bundle.schemaId);
        JsonNode prep = MAPPER.readTree(preprocessing);
        List<String> order = new ArrayList<>();
        prep.get("feature_order").forEach(n -> order.add(n.asText()));
        List<String> schemaOrder = schema.definitions().stream().map(FeatureDefinition::name).toList();
        if (!order.equals(schemaOrder)) {
            throw new IllegalStateException("preprocessing.json's feature order differs from " + schema.id()
                + "'s: " + order + " vs " + schemaOrder);
        }

        // Each feature's frozen policy, mask and parameters, in feature order.
        List<FeaturePreprocessing> features = new ArrayList<>();
        for (JsonNode p : prep.get("parameters")) {
            String mask = p.path("mask_feature").asText("");
            features.add(new FeaturePreprocessing(p.get("feature").asText(),
                PreprocessingPolicy.valueOf(p.get("policy").asText()),
                mask.isEmpty() ? -1 : order.indexOf(mask),
                p.get("mean").asDouble(), p.get("std").asDouble()));
        }
        if (!features.stream().map(FeaturePreprocessing::feature).toList().equals(order)) {
            throw new IllegalStateException("preprocessing.json's parameters are not in its feature order");
        }

        // Both thresholds come from the delivered threshold contract.
        JsonNode thr = MAPPER.readTree(thresholds);
        double dense = thr.get("dense").get("threshold").asDouble();
        double temporal = thr.get("temporal").get("threshold").asDouble();

        return new LoadedSequenceDetector(new SequenceDetectorBundle(bundle.name, bundle.version, bundle.modelSha,
            schema.id(), schema.contentHash(), bundle.sequenceLength, bundle.featureCount, bundle.inputName,
            bundle.denseOutputName, bundle.temporalOutputName, dense, temporal, new ModbusPreprocessing(features)),
            model);
    }

    private static void requireSha(String file, String expected, byte[] bytes) {
        String actual = sha256Hex(bytes);
        if (!actual.equals(expected)) {
            throw new IllegalStateException(file + " does not match bundle.json's recorded SHA-256: expected "
                + expected + " but computed " + actual);
        }
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every JVM", e);
        }
    }

    // bundle.json, every field required.
    @JsonIgnoreProperties(ignoreUnknown = false)
    private record BundleJson(
        @JsonProperty(value = "name", required = true) String name,
        @JsonProperty(value = "version", required = true) String version,
        @JsonProperty(value = "schemaId", required = true) String schemaId,
        @JsonProperty(value = "sequenceLength", required = true) int sequenceLength,
        @JsonProperty(value = "featureCount", required = true) int featureCount,
        @JsonProperty(value = "inputName", required = true) String inputName,
        @JsonProperty(value = "denseOutputName", required = true) String denseOutputName,
        @JsonProperty(value = "temporalOutputName", required = true) String temporalOutputName,
        @JsonProperty(value = "modelSha", required = true) String modelSha,
        @JsonProperty(value = "preprocessingSha", required = true) String preprocessingSha,
        @JsonProperty(value = "thresholdsSha", required = true) String thresholdsSha) {
    }
}
