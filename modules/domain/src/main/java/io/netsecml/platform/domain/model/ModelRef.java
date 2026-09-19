package io.netsecml.platform.domain.model;

import java.io.Serializable;
import java.util.List;

// The identity and shape of one scoring model bundle: which model, which
// version, which feature schema it was trained against, and how to read its
// output tensor. This travels in Flink job configuration (hence Serializable)
// and is the value every scoring component -- the registry, the ONNX adapter,
// the inference use case -- agrees on.
//
// schemaHash and modelSha are content hashes (schema drift and model-file
// integrity, respectively), so both are validated as 64 lowercase hex
// characters here. That is the same REPRESENTATION the rest of the platform
// uses for a SHA-256 hex digest (see FeatureVector.schemaHash and
// Prediction.deriveId) -- it is not a claim that those other sites validate
// the format too; FeatureVector.schemaHash in particular carries no such check.
public record ModelRef(String name, String version, String schemaId, String schemaHash, String modelSha,
                        float threshold, List<String> classes, String outputName, int positiveClassColumn)
        implements Serializable {

    public ModelRef {
        // Identity fields: a blank value here means the bundle cannot be looked
        // up, logged, or matched against a feature schema.
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (version == null || version.isBlank()) {
            throw new IllegalArgumentException("version must not be blank");
        }
        if (schemaId == null || schemaId.isBlank()) {
            throw new IllegalArgumentException("schemaId must not be blank");
        }
        if (outputName == null || outputName.isBlank()) {
            throw new IllegalArgumentException("outputName must not be blank");
        }

        // Content hashes: exactly 64 lowercase hex characters (a SHA-256 digest),
        // never bounded to a shorter prefix or accepted uppercase.
        if (schemaHash == null || !schemaHash.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("schemaHash must be 64 lowercase hex characters");
        }
        if (modelSha == null || !modelSha.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("modelSha must be 64 lowercase hex characters");
        }

        // A decision threshold is a probability; anything outside 0..1 cannot be
        // compared against a score and is a configuration error, not a runtime
        // one. NaN and infinity both pass a naive `< 0 || > 1` range check (every
        // NaN comparison is false), so finiteness is checked explicitly -- a NaN
        // threshold would make every `score >= threshold` decision false, so the
        // job would emit a silent, confident stream of "not attack" predictions
        // rather than error, which is exactly the failure this unit exists to
        // prevent (see Prediction.score, which guards the same shape of value).
        if (!Float.isFinite(threshold) || threshold < 0.0f || threshold > 1.0f) {
            throw new IllegalArgumentException("threshold must be finite and within 0.0..1.0, got " + threshold);
        }

        // classes is the model's human-readable label set -- what a prediction's
        // consumer reads back for score and decision. A model that cannot name
        // its classes is not described well enough to serve. This is required on
        // its own terms: positiveClassColumn does NOT index into this list (see
        // below), so classes being non-empty is not what makes that column valid.
        if (classes == null || classes.isEmpty()) {
            throw new IllegalArgumentException("classes must not be empty");
        }
        // Defensive copy in: the caller keeps no handle on our internal list.
        classes = List.copyOf(classes);

        // positiveClassColumn selects a column of the model's OUTPUT TENSOR, not an
        // index into classes -- a single-column probability output (this platform's
        // fixture) and a two-column zipmap=False sklearn output both stay valid
        // regardless of how many labels classes carries, so this is bounded only
        // below, never by classes.size().
        if (positiveClassColumn < 0) {
            throw new IllegalArgumentException("positiveClassColumn must not be negative");
        }
    }
}
