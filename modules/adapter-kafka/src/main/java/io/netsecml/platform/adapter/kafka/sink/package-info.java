/**
 * Kafka producer wiring for feature, prediction, DLQ, and invalid-event topics.
 *
 * <p>Implemented: serializers ({@code FeatureVectorSerializer},
 * {@code RejectedRecordSerializer}) for the online job's producers, and their
 * exact-inverse deserializers ({@code FeatureVectorDeserializer},
 * {@code RejectedEventDeserializer}) for the archive job's consumers, plus the
 * neutral {@code RejectedRecordPayload} wire shape shared between them.
 */
package io.netsecml.platform.adapter.kafka.sink;
