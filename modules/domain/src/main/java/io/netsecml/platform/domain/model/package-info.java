/**
 * Domain-level model identity and bundle metadata types (name, version, SHA, schema hash), independent of the registry adapter.
 *
 * <p>Holds {@link io.netsecml.platform.domain.model.ModelRef}: one scoring model bundle's identity, its
 * pinned feature schema, its decision threshold, and which column of its output tensor is the positive class.
 */
package io.netsecml.platform.domain.model;
