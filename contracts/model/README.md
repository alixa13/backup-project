# contracts/model/

Owns `model-bundle-v1.json`: the frozen shape of a model bundle directory's
`bundle.json` -- name/version, feature schema id and content hash, feature
order, class labels, decision threshold, the ONNX model file's SHA-256, output
tensor name and positive-class column, training metrics/provenance, and the
golden `sampleVectors` used to guard against converter drift between training
and serving. Both engineers own this contract. Frozen Days 7-8 (Roadmap.md
Section 5).

These files are immutable. A change to a frozen contract creates a new version
(`-v2`); it never edits the committed file. `FilesystemModelRegistryTest` in
`modules/adapter-registry-filesystem` reads a bundle matching this shape and
verifies it against a fixture at `tests/fixtures/models/conn-demo-v1/`.
