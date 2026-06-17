# AGENTS.md — Alma.Kafka (fkafka)

This repo ships Agent Skill for the `Alma.Kafka` library. Compatible agents discover it automatically; see `.agents/skills/fkafka/SKILL.md`.

## Project Purpose

F# library (`Alma.Kafka`) for producing and consuming messages to/from Apache Kafka streams. Provides a typed, traced API with consumer lag monitoring, health checking, manual/auto commit modes, external checkpoint support, event schema definitions, metadata parsing, and trace propagation via Kafka headers. Published as a NuGet package.

## Tech Stack

| Component | Detail |
|---|---|
| Language | F# on .NET 10.0 |
| SDK | `global.json` pins .NET SDK 10.0.x (`rollForward: latestMinor`) |
| Kafka client | `Confluent.Kafka ~> 2.12` + `librdkafka.redist ~> 2.12` |
| Async sequences | `FSharp.Control.AsyncSeq ~> 3.1` |
| JSON parsing | `FSharp.Data ~> 6.0` (JSON Type Provider for schemas) |
| Logging | `Microsoft.Extensions.Logging ~> 10.0` |
| Error handling | `Feather.ErrorHandling` (`asyncResult` CE) |
| Metrics | `Alma.Metrics` (service status, mark enabled/disabled) |
| Serialization | `Alma.Serializer` (JSON serialization) |
| Service identification | `Alma.ServiceIdentification` — `Instance`, `Domain`, `Context`, etc. |
| State | `Alma.State` |
| Tracing | `Alma.Tracing` — OpenTracing-style spans, B3 header propagation |
| Test framework | Expecto |
| Build system | FAKE (F# Make) v1.3.0 via `build/` project |
| Package manager | Paket |
| Lint | fsharplint |

## Commands

```bash
# Restore dependencies
dotnet paket install

# Build
./build.sh build

# Run tests
./build.sh -t tests

# Publish to NuGet (CI only — requires NUGET_API_KEY)
./build.sh -t publish
```

Build options:
- `no-clean` — skip cleaning output dirs (required on CI)
- `no-lint` — run lint but ignore failures

## Project Structure

```
fkafka/
├── Kafka.fsproj                # Library project (PackageId: Alma.Kafka, v30.0.0)
├── AssemblyInfo.fs             # Auto-generated assembly metadata
├── src/
│   ├── Types.fs                # Core types: BrokerList, StreamName, GroupId, Header, Offset, TopicPartition, GetCheckpoint, connection types, service status helpers
│   ├── Admin.fs                # Admin client: createAdmin, getAllTopics, topicExists, isUp, lags (consumer lag per partition)
│   ├── Checker.fs              # Health checker: cluster + topic availability checks with retry/interval
│   ├── Trace.fs                # Trace propagation: inject/extract B3 headers to/from Kafka message headers
│   ├── Consumer.fs             # Consumer: consume as sequence, TracedMessage, manual/auto commit, external checkpoint support
│   ├── Events.fs               # Event schema: generic Event<'Key,'Meta,'Domain>, CommonEvent, EventId, CorrelationId, CausationId, Resource
│   ├── Producer.fs             # Producer: create, produce messages with key + headers, flush/close
│   ├── MetaData.fs             # MetaData parsing: CreatedAt, ProcessedBy, MetaData DU
│   └── schema/
│       ├── events.json         # JSON Type Provider schema for event parsing
│       └── metaData.json       # JSON Type Provider schema for metadata parsing
├── tests/
│   ├── tests.fsproj            # Test project
│   ├── Tests.fs                # Expecto test runner entry point
│   └── Propagation.fs          # Trace propagation tests (inject/extract B3 headers)
├── example/                    # Example console app demonstrating usage
│   ├── Example.fs              # Entry point
│   ├── Consumer.fs             # Consumer example
│   ├── Produce.fs              # Producer example
│   ├── Admin.fs                # Admin example
│   ├── ConsumerLag.fs          # Lag monitoring example
│   ├── Headers.fs              # Custom headers example
│   ├── Offset.fs               # Offset management example
│   ├── docker-compose.yaml     # Kafka + Zookeeper for local example
│   └── run-console.sh          # Script to run the example
├── build/
│   ├── Build.fs                # FAKE build entry point
│   ├── Targets.fs              # FAKE target definitions
│   └── ...
├── paket.dependencies
├── paket.references
├── fsharplint.json
├── global.json
├── CHANGELOG.md
├── authentication.md           # Kafka authentication notes
├── todo.md                     # Development todo list
└── .github/workflows/
    ├── tests.yaml
    ├── pr-check.yaml
    └── publish.yaml
```

## Architecture & Key Concepts

### Consumer (`Alma.Kafka.Consumer`)

- `ConsumerConfiguration.createWithConnection`: Creates config with connection (BrokerList + Topic) and GroupId.
- `Consumer.consume`: Returns a lazy sequence of `TracedMessage<'Message>`. Each message includes trace context extracted from B3 headers.
- **Commit modes**: `CommitMessage.Automatically` (Kafka auto-commit) or `CommitMessage.Manually` (caller commits via `ManualCommit.execute`).
- **External checkpoint**: Set `GetCheckpoint` on config to resume from external storage (DynamoDB, Redis, etc.). When no checkpoint exists, consumer starts from earliest available offset.
- `TracedMessage`: `{ Commit; Message; Trace }` — provides access to the parsed message, trace span, and manual commit handle.
- **Retry with exponential backoff**: Connection failures use `MarkAsDisabled.executeAndWait` with exponential wait (up to 30s).

### Producer (`Alma.Kafka.Producer`)

- `Producer.create`: Creates a producer with optional health checker.
- `MessageToProduce`: `{ Key: MessageKey; Headers: Header list; Value: string }`.
- `MessageKey`: `Simple of string | Delimited of string list` — delimited keys are comma-separated (KSQL compatible).
- Producer is `IDisposable` — flushes and closes on dispose.

### Admin (`Alma.Kafka.Admin`)

- `Admin.createAdmin`: Creates an admin client from a BrokerList.
- `Admin.lags`: Computes per-partition consumer lag for a given group.
- `Admin.getAllTopics`, `Admin.topicExists`, `Admin.isUp`: Cluster inspection.

### Events (`Alma.Kafka.Events`)

- Generic event type: `Event<'KeyData, 'MetaData, 'DomainData>` with full schema (Id, CorrelationId, CausationId, Timestamp, Event name, Domain/Context/Purpose/Version/Zone/Bucket, Resource, etc.).
- `CommonEvent`: Non-generic subset without data fields.
- All IDs use single-case DU wrappers: `EventId of Guid`, `CorrelationId of Guid`, `CausationId of Guid`.

### MetaData (`Alma.Kafka.MetaData`)

- `MetaData`: `OnlyCreatedAt | CreatedAndProcessed` — parsed from JSON using Type Provider with `src/schema/metaData.json`.
- `ProcessedBy`: `{ Instance; Commit: GitCommit; ImageVersion: DockerImageVersion }`.

### Trace Propagation (`Alma.Kafka.Trace`)

- `Trace.inject`: Injects active trace as B3 headers (`X-B3-TraceId`, `X-B3-SpanId`, `X-B3-ParentSpanId`, etc.) into Kafka message headers.
- `Trace.extractFromHeaders` / `Trace.extractFromKafkaHeaders`: Extracts trace context from consumed message headers.

### Health Checking (`Alma.Kafka.Checker`)

- `Checker.defaultChecker`: 10 retries, 1s initial wait, checks cluster and topic availability.
- `IntervalChecker`: Periodic async checks (every 60s by default) for cluster/topic health.

## Key Dependencies

| Package | Role |
|---|---|
| `Confluent.Kafka` | .NET Kafka client (consumer, producer, admin) |
| `librdkafka.redist` | Native Kafka library |
| `FSharp.Control.AsyncSeq` | Async sequences for interval-based health checks |
| `FSharp.Data` | JSON Type Provider for event/metadata schema parsing |
| `Microsoft.Extensions.Logging` | Structured logging abstraction |
| `Feather.ErrorHandling` | `asyncResult` CE and combinators |
| `Alma.Metrics` | Service status (enabled/disabled marking) |
| `Alma.Serializer` | JSON serialization |
| `Alma.ServiceIdentification` | `Instance`, `Domain`, `Context`, etc. for topic/event naming |
| `Alma.State` | State management |
| `Alma.Tracing` | Distributed tracing with B3 header propagation |

## Conventions

- **Namespace**: `Alma.Kafka` for all modules.
- **Single-case DU wrappers**: `BrokerList of string`, `StreamName of string | Instance of Instance`, `GroupId.Id of string | GroupId.Random`, `HeaderKey of string`, `Offset of int64`, `EventId of Guid`, etc.
- **Module-per-type**: Each DU has a companion `[<RequireQualifiedAccess>] module` with `value`, `parse`, etc.
- **Railway-oriented error handling**: `AsyncResult<'T, 'Error>` everywhere. Error types are DUs (`ConsumeError`, `ManualCommitError`, `MetaDataParseError`).
- **Trace via B3 headers**: Traces are propagated as `X-B3-*` Kafka headers. Consumer extracts, producer injects.
- **Message key format**: Delimited keys use comma separation for KSQL compatibility.
- **IDisposable pattern**: Both `Producer` and `Consumer` implement `IDisposable` — always use `use` bindings.
- **Internal visibility**: Types/functions prefixed with `internal` or `private` are not part of the public API.
- **Units of measure**: `Second` and `Attempt` are F# units of measure for retry/wait configuration.

## CI/CD

| Workflow | Trigger | What it does |
|---|---|---|
| `tests.yaml` | PRs + nightly cron | Runs `./build.sh -t tests` on ubuntu-latest with .NET 10.x |
| `pr-check.yaml` | PRs | Blocks fixup commits + ShellCheck |
| `publish.yaml` | Git tags `[0-9]+.[0-9]+.[0-9]+` | Publishes to NuGet.org |

## Release Process

1. Increment `<Version>` in `Kafka.fsproj`
2. Update `CHANGELOG.md`
3. Commit and create a git tag matching the version
4. Push tag — CI publishes automatically

## Local Development with Kafka

The `example/` directory contains a standalone console app with `docker-compose.yaml` for running Kafka + Zookeeper locally:

```bash
cd example
docker compose up -d    # Start Kafka + Zookeeper
./run-console.sh        # Run the example app
```

The main library itself has no docker-compose — it's a library, not a service.

## Pitfalls

- **Tests are limited**: Only trace propagation tests exist (`tests/Propagation.fs`). No integration tests against a real Kafka broker. Changes to consumer/producer logic require manual verification.
- **No external checkpoint tests**: The `GetCheckpoint` feature (external offset storage) has no automated tests.
- **`example/` is separate**: The example project has its own `docker-compose.yaml` and `example.fsproj` — it's not built as part of the main build.
- **JSON Type Provider schemas**: `src/schema/events.json` and `src/schema/metaData.json` are used at compile time. Changing these files affects type inference.
- **`authentication.md` and `todo.md`**: Notes files at the root — not code, but may contain useful context about Kafka auth patterns and planned work.
- **High version number (v30)**: This is a mature library with many breaking changes in its history. Check `CHANGELOG.md` for migration notes.
- **`build/` is shared boilerplate**: FAKE build files are shared across Alma libraries. Do not modify `Targets.fs` or `SafeBuildHelpers.fs` without understanding cross-project impact.
- **`GroupId.Random`**: Creates a unique group ID per consumer instance (uses `DateTime.Now.Ticks`). Useful for testing but means consumer always reads from the beginning.
