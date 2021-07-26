F-Kafka
=======

Library for reading messages or producing messages to/from stream.

## Install

Add following into `paket.dependencies`
```
git ssh://git@bitbucket.lmc.cz:7999/archi/nuget-server.git master Packages: /nuget/
# LMC Nuget dependencies:
nuget Lmc.Kafka
```

Add following into `paket.references`
```
Lmc.Kafka
```

## Use

### Consume Event sequence
```fs
open Lmc.Kafka

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
4. Run `$ fake build target release`
5. Go to `nuget-server` repo, run `faket build target copyAll` and push new versions

## Development
### Requirements
- [dotnet core](https://dotnet.microsoft.com/learn/dotnet/hello-world-tutorial)
- [FAKE](https://fake.build/fake-gettingstarted.html)

### Build
```bash
./build.sh
```

### Watch
```bash
./build.sh -t watch
```
