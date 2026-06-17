# Examples — Alma.Kafka

This file is the single source of truth for all example code in this skill. Each example is self-contained and ordered by increasing complexity. Payloads are `string`; `parseMessage` is a placeholder for your own deserialization.

## Basic Consume

```fsharp
open Alma.Kafka

let connection = {
    BrokerList = BrokerList "127.0.0.1:9092"
    Topic = StreamName "demo-topic"
}

let configuration = ConsumerConfiguration.createWithConnection connection (GroupId.Id "demo-group")

let parseMessage (raw: string) = raw  // replace with real deserialization

Consumer.consume configuration (TracedMessage.message >> parseMessage)
|> Seq.iter (fun message -> printfn "Message: %s" message)
```

## Produce With Key And Headers

```fsharp
open Alma.Kafka

let connection = {
    BrokerList = BrokerList "127.0.0.1:9092"
    Topic = StreamName "demo-topic"
}

use producer = Producer.create (ProducerConfiguration.createWithConnection connection)

let headers = [
    Header.ofString (HeaderKey "source") "web-api"
]

// Simple key
MessageToProduce.createWithHeaders headers (MessageKey.Simple "entity-42", "payload")
|> Producer.produce producer

// Composite, KSQL-compatible key (joined with ",")
MessageToProduce.create (MessageKey.Delimited [ "tenant-1"; "entity-42" ], "payload")
|> Producer.produceSingle producer   // produce + flush
```

## Consume With Manual Commit

```fsharp
open Alma.Kafka

let connection = {
    BrokerList = BrokerList "127.0.0.1:9092"
    Topic = StreamName "demo-topic"
}

let configuration =
    { ConsumerConfiguration.createWithConnection connection (GroupId.Id "demo-group") with
        CommitMessage = CommitMessage.Manually FailOnNotCommittedMessage.WithException
    }

Consumer.consume configuration id
|> Seq.iter (fun tracedMessage ->
    // process tracedMessage.Message ...
    match ManualCommit.execute tracedMessage.Commit with
    | Ok () -> ()
    | Error (ManualCommitError.KafkaException e) -> eprintfn "commit failed: %A" e
    | Error (ManualCommitError.RuntimeException e) -> eprintfn "commit failed: %A" e
)
```

## Consumer Lag

```fsharp
open Microsoft.Extensions.Logging
open Alma.Kafka
open Alma.Kafka.Admin

let runLag (logger: ILogger) =
    let connection = {
        BrokerList = BrokerList "127.0.0.1:9092"
        Topic = StreamName "demo-topic"
    }

    let totalLag =
        Admin.lags logger connection (GroupId.Id "demo-group")
        |> Async.RunSynchronously
        |> List.sumBy PartitionLag.lag

    printfn "total lag: %d" totalLag
```

## External Checkpoint

```fsharp
open Alma.Kafka
open Feather.ErrorHandling

// Resolve a starting offset from external storage; return Offset = None when nothing is stored.
let getCheckpoint (groupId: GroupId) (topicPartition: TopicPartition): AsyncResult<TopicPartitionOffset, exn> = asyncResult {
    let! storedOffset = ExternalStore.tryGetOffset groupId topicPartition  // your code: returns Offset option
    return { TopicPartition = topicPartition; Offset = storedOffset }
}

let connection = {
    BrokerList = BrokerList "127.0.0.1:9092"
    Topic = StreamName "demo-topic"
}

let configuration =
    { ConsumerConfiguration.createWithConnection connection (GroupId.Id "demo-group") with
        CommitMessage = CommitMessage.Manually FailOnNotCommittedMessage.WithException
        GetCheckpoint = Some getCheckpoint
    }

Consumer.consume configuration id
|> Seq.iter (fun tracedMessage ->
    // process tracedMessage.Message, then persist the external checkpoint and commit atomically
    match ManualCommit.execute tracedMessage.Commit with
    | Ok () -> ExternalStore.saveOffset (GroupId.Id "demo-group") tracedMessage  // your code
    | Error e -> eprintfn "commit failed: %A" e
)
```

## Consume With Application Tracing

```fsharp
open Alma.Kafka
open Alma.Tracing

let connection = {
    BrokerList = BrokerList "127.0.0.1:9092"
    Topic = StreamName "demo-topic"
}

let configuration = ConsumerConfiguration.createWithConnection connection (GroupId.Id "demo-group")

// The consumer's own span is created automatically from the message's B3 headers.
// Here we start an application child span for processing and finish it ourselves.
Consumer.consume configuration (fun tracedMessage ->
    tracedMessage.Message,
    "Process message" |> Trace.ChildOf.start tracedMessage.Trace
)
|> Seq.iter (fun (message, processTrace) ->
    // process message ...
    processTrace |> Trace.finish
)
```

## Health Checked Producer

```fsharp
open Alma.Kafka

let connection = {
    BrokerList = BrokerList "127.0.0.1:9092"
    Topic = StreamName "demo-topic"
}

// Blocks with retry until the cluster and topic are available, then yields a connected producer.
let configuration =
    { ProducerConfiguration.createWithConnection connection with
        Checker = Some Checker.defaultChecker
    }

use producer = Producer.create configuration

MessageToProduce.create (MessageKey.Simple "entity-42", "payload")
|> Producer.produceSingle producer
```
