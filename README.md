F-Kafka
=======

Library for reading messages or producing messages to/from stream.

## Install

Add following into `paket.dependencies`
```
source https://nuget.pkg.github.com/almacareer/index.json username: "%PRIVATE_FEED_USER%" password: "%PRIVATE_FEED_PASS%"
# LMC Nuget dependencies:
nuget Alma.Kafka
```

NOTE: For local development, you have to create ENV variables with your github personal access token.
```sh
export PRIVATE_FEED_USER='{GITHUB USERNANME}'
export PRIVATE_FEED_PASS='{TOKEN}'	# with permissions: read:packages
```

Add following into `paket.references`
```
Alma.Kafka
```

## Use

### Consume Event sequence
```fs
open Alma.Kafka

let connection = {
    BrokerList = BrokerList "127.0.0.1:9092,"  // list of all brokers
    Topic = StreamName "my-topic"              // topic name
}

let configuration = ConsumerConfiguration.createWithConnection connection GroupId.Random

Consumer.consume configuration (TracedMessage.message >> RawEvent.Parse)
|> Seq.iter (fun event ->
    printfn "Event: %A" event
)

// Or with tracing
Consumer.consume configuration (fun tracedMessage ->
    tracedMessage.Message |> parseEvent,
    "Parse event" |> Trace.ChildOf.start tracedMessage.Trace
)
|> Seq.iter (fun (event, parseTrace) ->
    printfn "Event: %A" event
    printfn "Trace %A" (parseTrace |> Trace.id)
    parseTrace |> Trace.finish
)
```

### Getting consumer lags for a groupId
> https://stackoverflow.com/questions/60506580/how-can-i-list-the-kafka-consumer-lag-and-latest-offset-per-partition-and-consum

```fs
open System
open Microsoft.Extensions.Logging
open Alma.Kafka
open Alma.Kafka.Admin
open Alma.Logging
open Alma.ErrorHandling

let brokerList = BrokerList "127.0.0.1:9092"
let topic = StreamName "my-topic"
let groupId = GroupId.Id "group-id-to-check"

use loggerFactory = LoggerFactory.create [
    UseLevel LogLevel.Trace
    LogToConsole
]
let logger = loggerFactory.CreateLogger("Logger")

let partitionLags =
    Admin.lags logger { BrokerList = brokerList; Topic = topic } groupId
    |> Async.RunSynchronously

let totalLag =
    partitionLags
    |> List.sumBy PartitionLag.lag

printfn "total lag: %A" totalLag
```

## Using external checkpoint for storing offsets

You can use external storage (like a database, Redis, etc.) to store Kafka consumer offsets instead of relying on Kafka's built-in offset management. This is useful for ensuring exactly-once processing or when you need more control over offset management.

### Configuration

Add a `GetCheckpoint` function to your consumer configuration:

```fs
open Alma.Kafka

// Define your checkpoint retrieval function
let getCheckpoint (topicPartition: TopicPartition): AsyncResult<TopicPartitionOffset, exn> = asyncResult {
    let! offset = yourExternalStorage.GetOffset(topicPartition)

    return {
        TopicPartition = topicPartition
        Offset = offset
    }
}

let connection = {
    BrokerList = BrokerList "127.0.0.1:9092"
    Topic = StreamName "my-topic"
}

let configuration =
    { ConsumerConfiguration.createWithConnection connection (GroupId.Id "my-group") with
        GetCheckpoint = Some getCheckpoint  // Add your checkpoint function
    }

// Use the consumer as normal
Consumer.consume configuration (TracedMessage.message >> RawEvent.Parse)
|> Seq.iter (fun event ->
    // Process your event
    printfn "Event: %A" event

    // Save the offset to your external storage after processing
    // yourExternalStorage.SaveOffset(event.TopicPartition, event.Offset)
)
```

### How it works

1. **When partitions are assigned**: The consumer calls your `GetCheckpoint` function for each partition
2. **If checkpoint exists**: Consumer resumes from the stored offset
3. **If no checkpoint**: Consumer starts from the beginning (due to `AutoOffsetReset.Earliest`)
4. **Offset management**: You're responsible for saving offsets to your external storage after successfully processing messages

### Important notes

- The `GetCheckpoint` function should return `Offset = None` when no checkpoint exists, not throw an exception
- Offsets should be saved to external storage **after** successfully processing each message, or after batch
- The consumer uses `AutoOffsetReset.Earliest` by default, so missing checkpoints start from the beginning
- Make sure your external storage operations are robust and handle failures appropriately


---

## Release
1. Increment version in `Kafka.fsproj`
2. Update `CHANGELOG.md`
3. Commit new version and tag it

## Development
### Requirements
- [dotnet core](https://dotnet.microsoft.com/learn/dotnet/hello-world-tutorial)

### Build
```bash
./build.sh build
```

### Tests
```bash
./build.sh -t tests
```
