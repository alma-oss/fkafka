---
name: fkafka
description: Use whenever generating or reviewing F# code that produces to or consumes from Apache Kafka via the Alma.Kafka library — calls to Consumer.consume, ConsumerConfiguration.createWithConnection, Producer.create / Producer.produce, MessageToProduce, Admin.lags, Checker, or composes TracedMessage handling, manual/auto commit (CommitMessage), external offset checkpoints (GetCheckpoint), or B3 trace propagation through Kafka headers. Trigger also on mentions of BrokerList, StreamName, GroupId, consumer lag, KSQL message keys, MessageKey.Delimited, or "read/write a Kafka topic in F#".
---

# F-Kafka

Library: [alma-oss/fkafka](https://github.com/alma-oss/fkafka)
NuGet: `Alma.Kafka`

## Purpose

`Alma.Kafka` is an F# wrapper over `Confluent.Kafka` for producing and consuming messages to/from Kafka topics. It exposes a typed, railway-oriented (`AsyncResult`) API with consumer-lag monitoring, cluster/topic health checking, manual or automatic offset commit, external offset checkpoints, and automatic distributed-trace propagation through Kafka message headers.

## When to Use

- Consuming a Kafka topic as a lazy F# sequence of typed messages.
- Producing messages with a key (simple or KSQL-style delimited) and custom headers.
- Monitoring consumer lag per partition for a given group.
- Adding health checks (cluster/topic availability with retry) around producing/consuming.
- Storing offsets in external storage instead of Kafka's built-in commit.

## When NOT to Use

- Non-Kafka message brokers.
- Low-level partition assignment or admin operations beyond topic listing and lag — drop to `Confluent.Kafka` directly.
- Schema-registry / Avro / Protobuf serialization — this library works with `string` payloads.

## Main Concepts

- `BrokerList` — single-case DU wrapping the comma-separated bootstrap-server string.
- `StreamName` — topic name; either an explicit `StreamName` or derived from an `Instance`.
- `GroupId` — `Id of string` for a stable group, or `Random` for a unique throwaway group (always reads from the beginning).
- `ConnectionConfiguration` — record of `{ BrokerList; Topic }`.
- `ConsumerConfiguration` — full consumer setup; build with `ConsumerConfiguration.createWithConnection` or `createWithDefaults`.
- `Consumer.consume` — turns a configuration into a lazy, effectively infinite `seq` of `TracedMessage`.
- `TracedMessage<'Message>` — `{ Commit; Message; Trace }`; map the payload via `TracedMessage.map`, finish the span via `TracedMessage.finish`.
- `CommitMessage` — `Automatically` (Kafka autocommit) or `Manually of FailOnNotCommittedMessage`.
- `ManualCommit` — handle whose `ManualCommit.execute` commits the current offset under manual mode.
- `GetCheckpoint` — optional function to resolve a starting offset per partition from external storage.
- `ProducerConfiguration` / `Producer` — producer setup and the disposable producer handle.
- `MessageToProduce` — `{ Key; Headers; Value }`; build with `MessageToProduce.create` / `createWithHeaders`.
- `MessageKey` — `Simple of string` or `Delimited of string list` (joined with `,`, spaces stripped, KSQL-compatible).
- `Admin` — cluster inspection: `createAdmin`, `getAllTopics`, `topicExists`, `isUp`, `lags`.
- `PartitionLag` — `{ Partition; Lag }` produced by `Admin.lags`.
- `Checker` / `IntervalChecker` — health-check records with `defaultChecker` presets.
- `Event<'KeyData,'MetaData,'DomainData>` / `CommonEvent` — typed event-envelope schema with `EventId`, `CorrelationId`, `CausationId`, `Resource`.
- `MetaData` — parsed message metadata (`OnlyCreatedAt` or `CreatedAndProcessed`) via `MetaData.parse`.

## Related Libraries

- `Confluent.Kafka` — underlying client; its types surface in errors and handles.
- `Feather.ErrorHandling` — `AsyncResult` / `asyncResult` CE used across the API.
- `Alma.Tracing` — span types and B3 header inject/extract used for trace propagation.
- `Alma.ServiceIdentification` — `Instance`, `Domain`, `Context` used by `StreamName` and event envelopes.
- `Alma.Metrics` — service status (`MarkAsEnabled` / `MarkAsDisabled`) wired into health checks.

## Keywords for Search

Kafka, Alma.Kafka, fkafka, F# Kafka, Confluent.Kafka, consumer, producer, BrokerList, StreamName, GroupId, ConsumerConfiguration, Consumer.consume, TracedMessage, CommitMessage, ManualCommit, GetCheckpoint, checkpoint, offset, consumer lag, Admin.lags, PartitionLag, Producer, MessageToProduce, MessageKey, Delimited, KSQL, Header, Checker, IntervalChecker, health check, trace propagation, B3 headers, Event, MetaData

## Reference Files

- For composition principles and recommended API usage, read `references/preferred-patterns.md`.
- For known pitfalls and incorrect assumptions, read `references/anti-patterns.md`.
- For worked code examples, read `references/examples.md`.
