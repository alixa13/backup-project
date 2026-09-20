package io.netsecml.platform.adapter.registry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.model.ModelRef;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

    private static final ObjectMapper MAPPER = new ObjectMapper();

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

        List<SampleVector> samples = bundle.sampleVectors.stream()
                .map(sample -> new SampleVector(toFloatArray(sample.values), sample.expectedScore))
                .toList();

        return new LoadedModel(ref, onnx, samples);
    }

    // bundle.json carries sample values as JSON numbers, which Jackson binds
    // to List<Double>; the domain and ONNX side both want float32.
    private static float[] toFloatArray(List<Double> values) {
        float[] result = new float[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = values.get(i).floatValue();
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
    // metrics, trainedAt and provenance are parsed -- so a bundle.json missing
    // one of them fails loudly instead of silently passing -- but are
    // documentation/audit-only past that: FilesystemModelRegistry never uses
    // them to build a ModelRef or to decide a feature vector's runtime order,
    // which always comes from the registered feature schema, never a bundle file.
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
