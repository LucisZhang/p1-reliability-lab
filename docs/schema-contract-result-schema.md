# Phase B2 schema-contract result schema

`showcase/results/schema_contract_drill.json` is added only by a passing remote run. It extends
the immutable top-level provenance envelope with:

- `environment`: the remote Linux hostname, CPU, RAM, OS/kernel, architecture, Docker version,
  and load average captured during the real broker run.
- `contracts`: the registered Debezium topic key/value Avro schemas, subject names, Registry
  IDs and versions, canonical SHA-256 hashes, effective `BACKWARD` modes, and the committed
  consumer-projection contract fixture hashes.
- `producer` and `consumer`: the filtered, non-secret Avro converter settings, Debezium state,
  Flink job/deserializer class, Registry URL, and job ID proving both sides used Schema Registry.
- `incompatible_schema_attempt`: the exact `event_id long -> string` mutation, compatibility
  response, registration HTTP response, and before/after subject versions. A pass requires HTTP
  `409` and an unchanged latest schema.
- `flow_continuity`: source/Iceberg state before and after rejection, the deterministic
  post-rejection event, Kafka offsets/lag, Flink checkpoints, and Iceberg snapshot IDs.
- `checks` and `summary`: machine-checkable proof that the incompatible schema was rejected,
  the old schema remained active, both producer and consumer stayed running, Kafka lag returned
  to zero, and the post-rejection source/Iceberg row-level diff was zero.

The dashboard sync validator rejects the artifact unless every B2 check is true. The generator
refuses to overwrite an existing result path, and `sync-down` refuses byte-different content at
an existing local path.
