/**
 * Composition root for the independent Kafka-to-ClickHouse archive job.
 *
 * <p>Implemented: {@code ArchiveJob} wires two Kafka-source-to-ClickHouse-sink
 * chains (feature vectors and rejected records), each behind its own
 * {@code RichMapFunction} ({@code FeatureVectorRowMapFunction},
 * {@code InvalidEventRowMapFunction}) that is the only place allowed to know
 * about both the Kafka wire format and the ClickHouse row shape.
 */
package io.netsecml.platform.bootstrap.archive;
