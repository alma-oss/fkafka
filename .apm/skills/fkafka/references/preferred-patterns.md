# Preferred Patterns — Alma.Kafka

## Core Principles

- Build configurations through the smart constructors (`ConsumerConfiguration.createWithConnection`, `ProducerConfiguration.createWithConnection`) and override individual fields with record-update syntax rather than constructing the records by hand. New optional fields get sane defaults this way.
- Treat every wrapper type through its companion module (`BrokerList.value`, `StreamName.value`, `GroupId.value`, `PartitionLag.lag`). Do not pattern-match the DU inline outside the module that owns it.
- Both `Producer` and `Consumer` own native resources and implement `IDisposable`. Bind them with `use` (or `Producer.close` / consume the sequence inside a scope) so the producer flushes and the consumer closes deterministically.
- `Consumer.consume` returns a lazy, effectively infinite sequence. Drive it with `Seq.iter` / `Seq.map` and let the surrounding scope control lifetime; do not force it with `Seq.toList`.

## Recommended API Usage

- Consuming: pass a projection from `TracedMessage<string>` to your payload as the second argument of `Consumer.consume`. The simplest projection is `TracedMessage.message >> parseMessage`. See `examples.md` → Basic Consume.
- Producing: build the value with `MessageToProduce.create (key, value)` or `MessageToProduce.createWithHeaders headers (key, value)`, then `Producer.produce` (batch) or `Producer.produceSingle` (produce + flush). See `examples.md` → Produce With Key And Headers.
- Choose `MessageKey.Simple` for a single-field key and `MessageKey.Delimited` when the key is a composite that must stay KSQL-compatible.
- Lag monitoring: `Admin.lags` returns `PartitionLag list`; sum with `List.sumBy PartitionLag.lag`. See `examples.md` → Consumer Lag.

## Error Handling

- The API is railway-oriented: consume/commit results are `Result` / `AsyncResult` carrying DU error types (`ConsumeError`, `ManualCommitError`, `MetaDataParseError`). Match on the specific cases rather than catching exceptions.
- `ConsumeError.PreviousMessageWasNotCommited` only appears under manual commit with `FailOnNotCommittedMessage.WithException`; it signals the previous message was never committed.
- Under manual commit, always inspect the `Result` returned by `ManualCommit.execute` and surface `ManualCommitError` instead of ignoring it.

## Composition

- Transform payloads while preserving trace and commit handles with `TracedMessage.map`. This keeps the `Commit` handle attached after parsing.
- When you start child spans for processing, finish them — `TracedMessage.finish` finishes the message's own span; spans you start from `Alma.Tracing` you finish yourself.

## Integration with Other Libraries

- Tracing is automatic only when a tracer is active in the host process; the library checks tracer availability and otherwise produces an inactive span. The trace context is propagated as B3 Kafka headers — the consumer extracts it, the producer injects it. No manual header plumbing is required for propagation.
- External checkpoints integrate via `GetCheckpoint = Some f` on the consumer configuration, where `f: GroupId -> TopicPartition -> AsyncResult<TopicPartitionOffset, exn>`. See `examples.md` → External Checkpoint.
- Health checks come from `Checker.defaultChecker` / `IntervalChecker.defaultChecker`; attach them to the configuration's `Checker` / `IntervalChecker` fields to gate consuming/producing on cluster and topic availability with retry and `Alma.Metrics` status marking.

## Naming Conventions

- Single-case DU wrappers (`BrokerList of string`, `Offset of int64`, `EventId of Guid`, …) each have a `[<RequireQualifiedAccess>]` companion module exposing `value` and constructors. Follow this module-per-type convention for any helper you add.
- Namespace is `Alma.Kafka`; qualify modules (`Consumer.`, `Producer.`, `Admin.`) rather than opening everything.

## Testing Recommendations

- Use `GroupId.Random` in tests to force reading a topic from the beginning with an isolated group.
- The repository's own automated coverage is limited to trace-propagation tests; consumer/producer behavior against a real broker is verified manually (the `example/` project ships a `docker-compose.yaml` with Kafka for local runs). Write integration tests against a disposable broker when changing consume/produce logic.
