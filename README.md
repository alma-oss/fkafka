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
