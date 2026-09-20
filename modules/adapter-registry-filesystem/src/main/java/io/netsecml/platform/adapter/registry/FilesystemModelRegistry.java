package io.netsecml.platform.adapter.registry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.model.ModelRef;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

// Reads a model bundle directory off disk -- bundle.json plus model.onnx,
// matching contracts/model/model-bundle-v1.json -- and verifies the model
// file's SHA-256 against the bundle's own recorded modelSha before handing
// either to a caller. A truncated or swapped model file must never reach the
// scoring runtime: it would score silently and wrongly rather than failing.
//
// This adapter depends only on domain (for ModelRef) and Jackson, matching
// the "adapters do not import each other" rule: it never depends on
// adapter-kafka, adapter-onnx or any other adapter.
public final class FilesystemModelRegistry {

    // Jackson's @JsonProperty(required = true) only enforces that a key is
    // PRESENT in the JSON object -- it does not stop an explicit JSON `null`
    // from binding to a required primitive record component. Without this
    // feature enabled, "threshold": null silently becomes threshold = 0.0,
    // which then passes ModelRef's 0..1 range check and makes
    // `score >= threshold` true for every prediction: a detector that flags
    // all traffic with no error anywhere. Enabling
    // FAIL_ON_NULL_FOR_PRIMITIVES makes that same null throw
    // MismatchedInputException at load time instead, for every required
    // primitive field (threshold, positiveClassColumn) on this mapper.
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);

    private FilesystemModelRegistry() {
        // Static utility: load() is the only entry point. There is no
        // per-bundle state worth holding in an instance -- each call reads a
        // fresh directory and returns a fresh, independent LoadedModel.
    }

    // Reads bundleDir/bundle.json and bundleDir/model.onnx, verifies the
    // model's content hash, and returns the loaded bundle. Throws IOException
    // if either file is missing or unreadable, and IllegalStateException if
    // model.onnx's SHA-256 does not match the bundle's recorded modelSha.
    public static LoadedModel load(Path bundleDir) throws IOException {
        Path bundleFile = bundleDir.resolve("bundle.json");
        Path onnxFile = bundleDir.resolve("model.onnx");

        // Jackson's File-based readValue throws FileNotFoundException (an
        // IOException) when bundleFile does not exist, and Files.readAllBytes
        // throws NoSuchFileException (also an IOException) for onnxFile --
        // between the two, a missing bundle directory is rejected with no
        // extra existence check needed here.
        BundleJson bundle = MAPPER.readValue(bundleFile.toFile(), BundleJson.class);
        byte[] onnx = Files.readAllBytes(onnxFile);

        String actualSha = sha256Hex(onnx);
        if (!actualSha.equals(bundle.modelSha)) {
            // The message names "modelSha" on purpose -- an operator staring at
            // a failed job needs to know which field of bundle.json to check
            // first, not just that "something" didn't match.
            throw new IllegalStateException(
                "model.onnx does not match bundle's recorded modelSha: expected " + bundle.modelSha
                    + " but computed " + actualSha);
        }

        // ModelRef's own compact constructor re-validates every one of these
        // fields (blank checks, hex-digest shape, threshold range, non-empty
        // classes); this call is where a malformed bundle.json that passed
        // Jackson but not domain validation gets caught.
        ModelRef ref = new ModelRef(bundle.name, bundle.version, bundle.schemaId, bundle.schemaHash,
                bundle.modelSha, (float) bundle.threshold, bundle.classes, bundle.outputName,
                bundle.positiveClassColumn);

        List<SampleVector> samples = toSampleVectors(bundle.sampleVectors);

        return new LoadedModel(ref, onnx, samples);
    }

    // @JsonProperty(required = true) only rejects an ABSENT sampleVectors key;
    // Jackson still binds an explicit `"sampleVectors": null` for a required
    // List (FAIL_ON_NULL_FOR_PRIMITIVES only covers primitives, not
    // reference types), so this null check is load()'s own responsibility.
    // The message names the field so an operator does not have to guess which
    // part of a large bundle.json is wrong.
    private static List<SampleVector> toSampleVectors(List<SampleVectorJson> sampleVectors) {
        if (sampleVectors == null) {
            throw new IllegalStateException(
                "bundle.json's sampleVectors must not be null; use an empty array for a bundle with no golden vectors");
        }
        List<SampleVector> result = new ArrayList<>(sampleVectors.size());
        for (int i = 0; i < sampleVectors.size(); i++) {
            result.add(new SampleVector(toFloatArray(sampleVectors.get(i).values, i), sampleVectors.get(i).expectedScore));
        }
        return result;
    }

    // bundle.json carries sample values as JSON numbers, which Jackson binds
    // to List<Double>; the domain and ONNX side both want float32. Both the
    // list itself and any of its elements can arrive as an explicit JSON
    // null (the same required-but-nullable gap as sampleVectors above), so
    // both are checked here rather than left to throw a bare, unhelpful
    // NullPointerException with no indication of which sample or index is
    // bad -- sampleIndex is only for that error message.
    private static float[] toFloatArray(List<Double> values, int sampleIndex) {
        if (values == null) {
            throw new IllegalStateException(
                "bundle.json's sampleVectors[" + sampleIndex + "].values must not be null");
        }
        float[] result = new float[values.size()];
        for (int i = 0; i < values.size(); i++) {
            Double value = values.get(i);
            if (value == null) {
                throw new IllegalStateException(
                    "bundle.json's sampleVectors[" + sampleIndex + "].values[" + i + "] must not be null");
            }
            result[i] = value.floatValue();
        }
        return result;
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every JVM", e);
        }
    }

    // Jackson binding target for bundle.json. Its shape mirrors
    // contracts/model/model-bundle-v1.json field-for-field. featureOrder,
    // metrics, trainedAt and provenance are parsed -- so a bundle.json
    // missing one of those keys fails loudly (Jackson's `required = true`
    // throws MismatchedInputException for an absent key) instead of silently
    // passing -- but that guarantee does NOT extend to an explicit JSON
    // `null` for these four: none of them is primitive and none is
    // null-checked here, so "trainedAt": null binds silently as a null
    // String and simply flows through unused. That is tolerable because all
    // four are documentation/audit-only past parsing: FilesystemModelRegistry
    // never uses them to build a ModelRef or to decide a feature vector's
    // runtime order, which always comes from the registered feature schema,
    // never a bundle file. The fields that DO drive a scoring decision are
    // held to the stricter standard: threshold and positiveClassColumn
    // reject an explicit null too, because MAPPER above enables
    // FAIL_ON_NULL_FOR_PRIMITIVES, and sampleVectors (plus each entry's
    // values) is checked by hand in toSampleVectors/toFloatArray for the
    // same reason -- both are reference types that flag does not cover.
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record BundleJson(
            @JsonProperty(value = "name", required = true) String name,
            @JsonProperty(value = "version", required = true) String version,
            @JsonProperty(value = "schemaId", required = true) String schemaId,
            @JsonProperty(value = "schemaHash", required = true) String schemaHash,
            @JsonProperty(value = "featureOrder", required = true) List<String> featureOrder,
            @JsonProperty(value = "classes", required = true) List<String> classes,
            @JsonProperty(value = "threshold", required = true) double threshold,
            @JsonProperty(value = "modelSha", required = true) String modelSha,
            @JsonProperty(value = "outputName", required = true) String outputName,
            @JsonProperty(value = "positiveClassColumn", required = true) int positiveClassColumn,
            @JsonProperty(value = "metrics", required = true) Map<String, Object> metrics,
            @JsonProperty(value = "trainedAt", required = true) String trainedAt,
            @JsonProperty(value = "provenance", required = true) String provenance,
            @JsonProperty(value = "sampleVectors", required = true) List<SampleVectorJson> sampleVectors) {
    }

    // Jackson binding target for one entry of bundle.json's sampleVectors.
    private record SampleVectorJson(
            @JsonProperty(value = "values", required = true) List<Double> values,
            @JsonProperty(value = "expectedScore", required = true) double expectedScore) {
    }
}
