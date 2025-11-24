# Metadata fixtures

Sample local metadata JSON files used for demos or ad‑hoc testing.

How to use
- Point the code to a specific file by setting the env var `METADATA_LOCAL_FILE`.
  - Example (Bash/WSL): `METADATA_LOCAL_FILE=fixtures/metadata/local_metadata_test.json PYTHONPATH=src python -m local_pipeline`
- The default (`local_metadata.json` at the repo root) remains the active store when the env var is not set.

Notes
- Root-level `local_metadata*.json` are ignored by Git (see `.gitignore`).
- Files in this folder are committed intentionally as fixtures.
