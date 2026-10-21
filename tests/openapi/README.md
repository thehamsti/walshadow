# Vendored OpenAPI 3.1 schemas

Used only by the API docs tests (`src/ops/api/docs_tests.rs`) to validate the
OpenAPI document the HTTP API serves, offline. Nothing here ships in the
binary.

| File | `$id` (source) |
|---|---|
| `schema-2022-10-07.json` | `https://spec.openapis.org/oas/3.1/schema/2022-10-07` |
| `dialect-base.json` | `https://spec.openapis.org/oas/3.1/dialect/base` |
| `meta-base.json` | `https://spec.openapis.org/oas/3.1/meta/base` |

Fetched unmodified from those URLs on 2026-10-21. They are published by the
OpenAPI Initiative in [OAI/OpenAPI-Specification](https://github.com/OAI/OpenAPI-Specification)
under the Apache License 2.0. JSON Schema 2020-12 meta-schemas, which they
reference, are built into the `jsonschema` crate. The tests resolve every
reference from these files or that built-in set, and fail if anything else
would be fetched.
