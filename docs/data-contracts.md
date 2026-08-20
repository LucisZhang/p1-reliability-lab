# Path B Avro data contracts

Phase B2 keeps the Phase B1 topology and changes the Path B wire format from Kafka Connect JSON
to Confluent Avro. Debezium Connect 3.2.4.Final produces the keyed CDC envelope; Confluent's
7.9.8 `AvroConverter` registers and serializes both `<topic>-key` and `<topic>-value`. The Flink
Path B job uses the same converter in deserialization and resolves each wire schema ID through
Schema Registry 7.9.8. Path A is unchanged.

`make broker-up` runs the pinned Maven 3.9 toolchain after the broker preflight and resolves the
converter's 7.9.8 runtime closure with `maven-dependency-plugin` 3.8.1. The generated directory is
excluded from Git and remote sync; the Debezium Dockerfile copies it into an isolated Connect
plugin directory. This avoids relying on an additional builder image while keeping a fresh clone
reproducible from the pinned POM.

The registry is globally `BACKWARD`, and the drill also pins `BACKWARD` explicitly on the two
order-topic subjects once Debezium has created them. The authoritative wire schemas are generated
from the MySQL table and Debezium envelope. The passing remote result preserves their full JSON,
subject, ID, version, and canonical SHA-256 so the evidence survives the ephemeral registry.

The files under `contracts/avro/` are the stable Flink consumer projection and three CI evolution
fixtures:

- `order-change-v1.avsc`: the required projection fields read from the Debezium envelope.
- `order-change-compatible-add-v2.avsc`: adds an optional `trace_id` with default `null` and must
  pass.
- `order-change-incompatible-remove-v2.avsc`: removes required `event_id` and must fail the lab's
  required-field policy.
- `order-change-incompatible-type-v2.avsc`: changes `event_id` from `long` to `string` and must
  fail both Avro backward compatibility and the lab policy.

Pure Avro `BACKWARD` reader compatibility permits a new reader to ignore a field that existed in
older data. The plan also requires field removal to fail, so CI deliberately adds a stricter
required-field-preservation rule without changing the locked Registry mode. The live registry
drill uses the type change, which Schema Registry itself must reject with HTTP `409`.

Run the light contract tests in CI or locally:

```bash
make test
```

Run the real drill only on the guarded remote Linux workstation:

```bash
make sync-up P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
make remote-broker-up P1_REMOTE_ROOT="$P1_REMOTE_ROOT" \
  ARGS="--phase contracts --fresh"
make remote-broker-verify P1_REMOTE_ROOT="$P1_REMOTE_ROOT" \
  ARGS="--phase contracts --baseline-events 2 --seed 211"
make sync-down P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
```

The fresh flag removes only the remote lab's Compose containers and named volumes. It does not
delete or overwrite any committed result; `sync-down` compares existing artifacts and refuses a
different file at the same append-only path.

Certified remote run `20260820T120104Z-7e40acd6` is committed in
[`showcase/results/schema_contract_drill.json`](../showcase/results/schema_contract_drill.json).
The live value subject stayed at version `1` after the incompatible registration returned HTTP
`409`; the connector and Flink job stayed running, checkpoint ID advanced from `5` to `7`, the
post-rejection event reached Iceberg, all partition lag was `0`, and the final source/Iceberg
row-level diff was `0`. The claim stops at Registry rejection and old-schema continuity; it does
not cover the broker-failure drills in later phases.
