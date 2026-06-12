# Draft: Kafka output for the Akvorado outlet

**Status:** proof-of-concept on branch `feat/kafka-outlet-draft`. Not upstreamed,
not production-ready. Built to gauge the real size and shape of the change
before we decide how to proceed. Internal context: work item #812 (alert CERT on
SUNET C2 matches in live NetFlow).

## What it does

Adds a *second output* to the outlet: produce the already-decoded-and-enriched
flow to a Kafka topic, in parallel with the existing ClickHouse write. A
downstream consumer (Flink, or a small matcher) can then do real-time detection
without bulk-exporting from ClickHouse.

This is the architecturally clean tap point: the outlet is exactly where a flow
is fully decoded and enriched, so fanning out to an extra destination there
avoids (a) re-parsing raw NetFlow/IPFIX off the inlet topic and (b) hammering
ClickHouse with an export query.

```
inlet → Kafka (raw) → outlet ─┬→ ClickHouse        (existing)
                              └→ Kafka (enriched)   (this draft)  → Flink / matcher
```

## What was added

| File | ~Lines | Role |
|------|-------|------|
| `outlet/kafkaout/{root,config,metrics}.go` | 203 | New producer component — mirrors `inlet/kafka` |
| `common/schema/flatten.go` | 121 | `MarshalFlowJSON` — the only genuinely new logic |
| `outlet/core/root.go`, `outlet/core/worker.go` | 9 | Optional dependency + produce hook in the worker `finalize` |
| `cmd/akvorado/outlet.go` | 11 | Config field + component wiring |

**~344 lines total.** Builds and vets clean (the fresh clone needs `make`'s
codegen first: protobuf, enumers, mocks).

## The one non-trivial piece: serialization

`json.Marshal(FlowMessage)` only sees the *fixed struct fields* — it does **not**
include `SrcPort`/`DstPort`/`Bytes`/`Proto`, which are dynamic ClickHouse columns
stored in the unexported `bf.batch`. Detection needs ports, so
`common/schema/flatten.go` reads those columns back out by type-switch (mirroring
`Undo`/`appendDefaultValues`) and emits a flat JSON object. This is why the
serializer must live inside `common/schema`, and it's the bulk of the new code.
Using JSON deliberately sidesteps the dynamic-schema protobuf machinery.

## How to try it

Disabled by default. In the outlet config:

```yaml
kafka-out:
  enabled: true
  topic: flows-enriched          # actual topic gets a -v<N> suffix
  brokers: [kafka:9092]
  queue-size: 4096
```

Then consume `flows-enriched-v<N>`; each message is one flow as JSON.

## Caveats (deliberate, for a draft)

- **Best-effort delivery** (async produce, like the inlet and the HTTP flow tee).
  Detection probably wants at-least-once — a deliberate add, not a rewrite.
- **Scalar columns only.** Covers the 5-tuple + bytes/packets/proto; arrays
  (AS paths, communities) are skipped — fine for `(IP, port)` matching.
- **Reaches into `common/schema` internals** (`bf.batch`). Cheap to maintain
  in-tree; brittle as an out-of-tree carried patch across Akvorado upgrades —
  a point in favour of upstreaming the idea rather than forking.

## Decision context

- **If the C2 set is small / slow-changing:** a ClickHouse dictionary + SQL match
  likely meets #812's DoD first; this Kafka output is then the phase-2 platform.
- **If streaming is warranted:** this patch, or a standalone goflow2 proxy that
  consumes the inlet raw topic (no upstream dependency).
- Maintainer keeps detection out of scope but is making the outlet more
  extensible — favour an upstream discussion first, carry this patch as fallback.
