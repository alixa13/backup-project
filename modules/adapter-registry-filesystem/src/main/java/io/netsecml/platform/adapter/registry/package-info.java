/**
 * Read-only filesystem model registry adapter: {@link io.netsecml.platform.adapter.registry.FilesystemModelRegistry}
 * reads a model bundle directory (bundle.json + model.onnx, matching
 * contracts/model/model-bundle-v1.json) and verifies model.onnx's SHA-256
 * against the bundle's recorded modelSha before returning a
 * {@link io.netsecml.platform.adapter.registry.LoadedModel}.
 */
package io.netsecml.platform.adapter.registry;
