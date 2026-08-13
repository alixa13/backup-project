# models/

Read-only runtime mount for immutable, content-addressed model bundles
(`adapter-registry-filesystem`). Model artifact bytes are versioned by the training
release process, not by Git — see `contracts/model/model-bundle-manifest-v1.json`
(once committed) for the manifest that pins artifact SHA-256, schema hash, and
evaluation summary per bundle.

Expected shape once populated (Roadmap.md Day 7-8 onward):

```
models/<model-name>/<version>/
  model.onnx
  manifest.json
```

Nothing under this directory except this file and `.gitignore` is tracked by Git.
