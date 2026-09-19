/**
 * Domain-level prediction and inference result types (score, decision, threshold), independent of ONNX Runtime.
 *
 * <p>Holds {@link io.netsecml.platform.domain.inference.Prediction}: one scored event's score, decision,
 * and the model/schema lineage needed to trace it back to the bundle and feature vector that produced it.
 */
package io.netsecml.platform.domain.inference;
