/**
 * Mapping from domain values ({@code FeatureVector}, {@code RejectedEvent}) to
 * ClickHouse row representations ({@code FeatureVectorRow}, {@code InvalidEventRow}).
 *
 * <p>Consumes domain types only. This package imports no Kafka or Flink
 * connector types — the mappers know nothing about how a value arrived, only
 * how to turn it into a row, which keeps the module's domain-to-row boundary
 * enforced by its own classpath.
 */
package io.netsecml.platform.adapter.clickhouse.mapper;
