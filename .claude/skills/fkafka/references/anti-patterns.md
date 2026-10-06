# Anti-Patterns — Alma.Kafka

Each entry is **mistake → why → fix**.

## Lifecycle

- **Mistake:** Creating a `Producer` or `Consumer` without `use` (or never calling `Producer.close`).
  **Why:** Both hold native `librdkafka` resources and implement `IDisposable`; a leaked producer may never flush buffered messages, and a leaked consumer never closes its group session.
  **Fix:** Bind with `use producer = Producer.create config`, or scope the `Consumer.consume` sequence so disposal happens when the scope ends.

- **Mistake:** Forcing the consume sequence with `Seq.toList` / `List.ofSeq`.
  **Why:** `Consumer.consume` is effectively infinite; materializing it blocks forever.
  **Fix:** Stream it lazily with `Seq.iter` / `Seq.map` / `Seq.truncate`.

## Group IDs

- **Mistake:** Using `GroupId.Random` in production.
  **Why:** It generates a unique group id per process start, so the consumer always re-reads the topic from the beginning and never shares progress across instances.
  **Fix:** Use `GroupId.Id "<stable-name>"` for any long-running consumer; reserve `GroupId.Random` for tests and one-off reads.

## Commit Handling

- **Mistake:** Selecting `CommitMessage.Manually` but never calling `ManualCommit.execute`.
  **Why:** Offsets are never committed, so the consumer reprocesses from the last committed position on restart; with `FailOnNotCommittedMessage.WithException` the next consume fails with `ConsumeError.PreviousMessageWasNotCommited`.
  **Fix:** After successfully processing a message, call `ManualCommit.execute message.Commit` and handle the `ManualCommitError` result.

- **Mistake:** Discarding the `Result` from `ManualCommit.execute`.
  **Why:** A failed commit (`ManualCommitError.KafkaException` / `RuntimeException`) is silently lost, masking duplicate-processing risk.
  **Fix:** Pattern-match the result and log/propagate the error.

## External Checkpoints

- **Mistake:** Throwing from a `GetCheckpoint` function when no stored offset exists.
  **Why:** The library treats a successful result with `Offset = None` as "no checkpoint" and falls back to the earliest offset; an exception instead aborts partition assignment.
  **Fix:** Return `Ok { TopicPartition = tp; Offset = None }` when nothing is stored.

- **Mistake:** Saving the external checkpoint and committing the Kafka offset as independent, non-atomic steps.
  **Why:** A crash between the two leaves stored and committed offsets out of sync, causing message loss or duplication.
  **Fix:** Persist the external checkpoint and the Kafka commit together (transactionally or with idempotent processing) after each message or batch.

## Message Keys

- **Mistake:** Embedding spaces or your own separators in a `MessageKey.Delimited` list.
  **Why:** Delimited keys are joined with `,` and have spaces stripped to stay KSQL-compatible; manual separators or spaces produce keys that don't match downstream consumers.
  **Fix:** Pass the raw field values as a `string list` and let `MessageKey.Delimited` build the comma-joined key.

## Wrong Abstractions

- **Mistake:** Reaching into the `Trace` module or hand-injecting B3 headers.
  **Why:** Trace inject/extract is internal and runs automatically inside `Producer.produce` and `Consumer.consume`; duplicating it produces conflicting spans.
  **Fix:** Rely on the built-in propagation; only create application-level child spans through `Alma.Tracing`.

- **Mistake:** Pattern-matching wrapper DUs (`BrokerList`, `Offset`, `GroupId`) inline across modules.
  **Why:** It bypasses the companion-module API and breaks if the representation changes.
  **Fix:** Use the module accessors (`BrokerList.value`, `GroupId.value`, …).
