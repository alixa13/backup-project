package io.netsecml.platform.domain.model;

import java.util.Objects;

// A loaded, verified sequence detector: its identity, the feature schema it
// was trained on, its graph's input and output names and shapes, its two
// frozen thresholds, and its frozen preprocessing. Built only by the bundle
// loader, after every SHA-256 and the feature order have been checked.
public record SequenceDetectorBundle(String name, String version, String modelSha, String schemaId,
                                     String schemaHash, int sequenceLength, int featureCount, String inputName,
                                     String denseOutputName, String temporalOutputName, double denseThreshold,
                                     double temporalThreshold, ModbusPreprocessing preprocessing) {

    public SequenceDetectorBundle {
        // Identity and graph names are looked up by string; none may be missing.
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(modelSha, "modelSha");
        Objects.requireNonNull(schemaId, "schemaId");
        Objects.requireNonNull(schemaHash, "schemaHash");
        Objects.requireNonNull(inputName, "inputName");
        Objects.requireNonNull(denseOutputName, "denseOutputName");
        Objects.requireNonNull(temporalOutputName, "temporalOutputName");
        Objects.requireNonNull(preprocessing, "preprocessing");
        // The temporal head predicts event L from events 1..L-1, so L >= 2.
        if (sequenceLength < 2) {
            throw new IllegalArgumentException("sequenceLength must be at least 2, was " + sequenceLength);
        }
        // The preprocessing transforms exactly the vector the graph reads.
        if (featureCount != preprocessing.width()) {
            throw new IllegalArgumentException("featureCount " + featureCount + " but preprocessing covers "
                + preprocessing.width());
        }
        // A threshold compares a mean absolute error, so it is a positive number.
        if (!(denseThreshold > 0 && Double.isFinite(denseThreshold)
                && temporalThreshold > 0 && Double.isFinite(temporalThreshold))) {
            throw new IllegalArgumentException("thresholds must be positive and finite");
        }
    }

    // How a window records which bundle filled it: name/version.
    public String bundleId() {
        return name + "/" + version;
    }
}
