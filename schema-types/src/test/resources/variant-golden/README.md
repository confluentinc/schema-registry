# Variant golden vectors

Binary test vectors copied verbatim from
[apache/parquet-testing](https://github.com/apache/parquet-testing) at commit
`4b1ce4502afff8d20c9b4bb08d07e04e21cdeff3`, path `variant/`. Apache License 2.0.

Each case is a `<name>.metadata` / `<name>.value` pair: the two binary buffers of
one Variant value. 29 cases.

`manifest.json` in this directory is ours, not upstream's. Upstream ships a
`data_dictionary.json` which is deliberately **not** copied here and must not be
used as an assertion target: it is invalid JSON (trailing comma), omits
`long_string`, and renders decimals through a binary double so low digits are
lost. It was used only as an independent cross-check while authoring the
manifest; every place our expected output differs from it is listed in
`manifest.json` under `dictionaryNote`.

The same corpus and manifest are vendored byte-identically into the six
non-Java clients, which assert the manifest's SHA-256 so a drifted copy fails
loudly rather than grading itself against its own expectations.
