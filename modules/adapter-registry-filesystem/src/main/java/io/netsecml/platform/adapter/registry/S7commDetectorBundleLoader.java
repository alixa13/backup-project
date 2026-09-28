package io.netsecml.platform.adapter.registry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.netsecml.platform.domain.feature.FeatureDefinition;
import io.netsecml.platform.domain.feature.FeatureSchema;
import io.netsecml.platform.domain.feature.FeatureSchemaRegistry;
import io.netsecml.platform.domain.feature.S7commFeatureSchemaV1;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import io.netsecml.platform.domain.model.S7commConformalPolicy;
import io.netsecml.platform.domain.model.S7commDetectorBundle;
import io.netsecml.platform.domain.model.S7commPreprocessing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

// Loads an S7comm detector bundle (contracts/model/s7comm-detector-bundle-v1.json)
// and refuses it unless every check of spec section 7 passes: each file's
// SHA-256, the feature schema, the preprocessing contract's features, order and
// transform, the policy's window, and the calibration file's four groups. The
// graph's names and shapes are checked by the ONNX scorer that opens it.
public final class S7commDetectorBundleLoader {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private S7commDetectorBundleLoader() {
    }

    public static LoadedS7commDetector load(Path bundleDir) throws IOException {
        BundleJson bundle = MAPPER.readValue(bundleDir.resolve("bundle.json").toFile(), BundleJson.class);
        byte[] model = Files.readAllBytes(bundleDir.resolve("model.onnx"));
        byte[] preprocessing = Files.readAllBytes(bundleDir.resolve("preprocessing.json"));
        byte[] policy = Files.readAllBytes(bundleDir.resolve("policy.json"));
        byte[] calibration = Files.readAllBytes(bundleDir.resolve("calibration.npz"));

        // Every file must be exactly the one bundle.json pins.
        requireSha("model.onnx", bundle.modelSha, model);
        requireSha("preprocessing.json", bundle.preprocessingSha, preprocessing);
        requireSha("policy.json", bundle.policySha, policy);
        requireSha("calibration.npz", bundle.calibrationSha, calibration);

        // The preprocessing hard-codes s7comm-feature-v1's layout, so the bundle
        // must be for exactly that schema.
        FeatureSchema schema = FeatureSchemaRegistry.byId(bundle.schemaId);
        require(schema.id().equals(S7commFeatureSchemaV1.SCHEMA.id()), "bundle.json's schemaId is " + schema.id()
            + ", but this detector reads " + S7commFeatureSchemaV1.SCHEMA.id());
        S7commPreprocessing prep = preprocessing(MAPPER.readTree(preprocessing), schema);
        require(prep.width() == bundle.featureCount, "bundle.json's featureCount " + bundle.featureCount
            + " but preprocessing.json produces " + prep.width());

        // The score's weights, from the raw features bundle.json names.
        float[] weights;
        try {
            weights = prep.scoreWeights(bundle.zeroWeightFeatures);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("bundle.json's zeroWeightFeatures: " + e.getMessage(), e);
        }

        return new LoadedS7commDetector(new S7commDetectorBundle(bundle.name, bundle.version, bundle.modelSha,
            schema.id(), schema.contentHash(), bundle.sequenceLength, bundle.featureCount, bundle.inputName,
            bundle.outputName, prep, weights,
            policy(MAPPER.readTree(policy), NpzReader.read(calibration), bundle.sequenceLength)), model);
    }

    // preprocessor_contract.json -> S7commPreprocessing (spec section 3.2),
    // refusing a contract whose features, order or transform differ.
    private static S7commPreprocessing preprocessing(JsonNode c, FeatureSchema schema) {
        List<String> order = schema.definitions().stream().map(FeatureDefinition::name).toList();
        require(texts(field(c, "raw_feature_order")).equals(order),
            "preprocessing.json's raw_feature_order differs from " + schema.id() + "'s feature order");
        JsonNode continuous = field(c, "continuous");
        List<String> names = texts(field(continuous, "features"));
        require(names.equals(order.subList(0, 12)), "preprocessing.json's continuous features are not "
            + schema.id() + "'s first 12");
        require("StableNumericTransformer".equals(field(continuous, "transformer").asText()),
            "preprocessing.json's continuous transformer is not StableNumericTransformer");
        JsonNode binary = field(c, "binary");
        require(texts(field(binary, "features")).equals(order.subList(12, 14)),
            "preprocessing.json's binary features are not " + schema.id() + "'s 13th and 14th");
        require(field(binary, "imputer_statistics").get(0).asDouble() == 0.0
            && field(binary, "imputer_statistics").get(1).asDouble() == 0.0,
            "preprocessing.json's binary imputer is not 0");
        JsonNode categorical = field(c, "categorical");
        require(texts(field(categorical, "features"))
                .equals(List.of(S7commPreprocessing.ROSCTR_FEATURE, S7commPreprocessing.OPERATION_FEATURE)),
            "preprocessing.json's categorical features are not s7_rosctr, s7_operation");

        // Each continuous feature: its median, and its range or its center and scale.
        JsonNode medians = field(continuous, "imputer_statistics");
        JsonNode centers = field(continuous, "robust_center");
        JsonNode scales = field(continuous, "robust_scale");
        JsonNode bounded = field(continuous, "bounded_ranges");
        List<S7commPreprocessing.Continuous> features = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            JsonNode range = bounded.get(names.get(i));
            features.add(range != null
                ? new S7commPreprocessing.Continuous(names.get(i), medians.get(i).asDouble(), 0.0, 1.0,
                    range.get(0).asDouble(), range.get(1).asDouble())
                : new S7commPreprocessing.Continuous(names.get(i), medians.get(i).asDouble(),
                    centers.get(i).asDouble(), scales.get(i).asDouble(), Double.NaN, Double.NaN));
        }
        JsonNode categories = field(categorical, "categories");
        S7commPreprocessing prep = new S7commPreprocessing(features, order.subList(12, 14),
            field(continuous, "transformed_clip").asDouble(),
            texts(field(categories, S7commPreprocessing.ROSCTR_FEATURE)),
            texts(field(categories, S7commPreprocessing.OPERATION_FEATURE)));

        // The contract's own column list must be the one this transform produces.
        require(prep.transformedFeatureOrder().equals(texts(field(c, "transformed_feature_order"))),
            "preprocessing.json's transformed_feature_order differs from the columns its transform produces: "
                + prep.transformedFeatureOrder());
        require(field(c, "transformed_dimension").asInt() == prep.width(),
            "preprocessing.json's transformed_dimension is not " + prep.width());
        return prep;
    }

    // causal_online_shadow_policy.json and the calibration scores ->
    // S7commConformalPolicy (spec section 3.4; alphas from alpha_by_group, D6).
    private static S7commConformalPolicy policy(JsonNode policy, Map<String, double[]> arrays, int sequenceLength) {
        JsonNode semantics = field(policy, "score_semantics");
        require(semantics.path("sequence_length").asInt() == sequenceLength, "policy.json's sequence_length "
            + semantics.path("sequence_length").asInt() + " is not bundle.json's " + sequenceLength);
        require("LAST_ONLY".equals(semantics.path("scored_timestep").asText()),
            "policy.json's scored_timestep is not LAST_ONLY");
        require(semantics.path("minimum_events_before_score").asInt() == sequenceLength,
            "policy.json's minimum_events_before_score is not the window length");
        Set<String> groups = new TreeSet<>(Arrays.stream(S7commScoreGroup.values()).map(Enum::name).toList());
        require(new TreeSet<>(arrays.keySet()).equals(groups),
            "calibration.npz holds " + arrays.keySet() + ", not the four groups " + groups);

        Map<S7commScoreGroup, double[]> scores = new EnumMap<>(S7commScoreGroup.class);
        arrays.forEach((group, values) -> scores.put(S7commScoreGroup.valueOf(group), values));
        Map<S7commScoreGroup, Double> alphas = new EnumMap<>(S7commScoreGroup.class);
        field(policy, "alpha_by_group").fields().forEachRemaining(e -> {
            require(groups.contains(e.getKey()), "policy.json's alpha_by_group names an unknown group " + e.getKey());
            alphas.put(S7commScoreGroup.valueOf(e.getKey()), e.getValue().asDouble());
        });
        return new S7commConformalPolicy(scores, alphas, field(policy, "fallback_alpha").asDouble());
    }

    private static JsonNode field(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || value.isNull()) {
            throw new IllegalStateException("the bundle's contract has no '" + name + "'");
        }
        return value;
    }

    private static List<String> texts(JsonNode array) {
        List<String> out = new ArrayList<>();
        array.forEach(n -> out.add(n.asText()));
        return out;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
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
        @JsonProperty(value = "outputName", required = true) String outputName,
        @JsonProperty(value = "zeroWeightFeatures", required = true) List<String> zeroWeightFeatures,
        @JsonProperty(value = "modelSha", required = true) String modelSha,
        @JsonProperty(value = "preprocessingSha", required = true) String preprocessingSha,
        @JsonProperty(value = "policySha", required = true) String policySha,
        @JsonProperty(value = "calibrationSha", required = true) String calibrationSha) {
    }
}
