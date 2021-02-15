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

### Consume RawEvent sequence
```fs
open Lmc.Kafka

let connection = {
    BrokerList = BrokerList "127.0.0.1:9092,"  // list of all brokers
    Topic = StreamName "my-topic"              // topic name
}

let configuration = ConsumerConfiguration.createWithConnection connection GroupId.Random

Consumer.consume configuration RawEvent.Parse
|> Seq.iter (fun event ->
    printfn "Event: %A" event
)
```

### Handle raw event
```fs
open Lmc.Kafka

let logMessage = printfn "%s"   // this function will be used for logging, it gets a simple message of what kafka lib is doing
let incrementMessageCount = id  // this function will be used for incrementing a message count, it gets raw event content (string) for each consumed event

let connection = {
    BrokerList = BrokerList "127.0.0.1:9092,"  // list of all brokers
    Topic = StreamName "my-topic"              // topic name
}

let configuration = ConsumerConfiguration.createWithConnection connection GroupId.Random

let onRawContent rawEvent = printfn "%A" rawEvent

onRawContent                        // on RawEvent handler
|> RawEvent.messageReader           // there are more available readers (see Kafka.{...}Reader)
|> Consumer.read logMessage configuration incrementMessageCount
```

### Handle raw event with
```fs
open Lmc.Kafka

type DomainEvent =
    // + concrete domain events
    | Raw of RawEvent

type DomainHandler = {
    // + concrete domain event handlers
    OnRawEvent: RawEvent -> unit
}

let defaultDomain = {
    // + concrete domain event handlers
    OnRawEvent = ignore
}

type DomainEventReader = MessageReader<DomainEvent>

let DomainEventReader handler: DomainEventReader =
    {
        ParseEvent = RawEvent.parse >> DomainEvent.Raw  // parsing a message - if DomainEvent has more types, you have to parse message by your own
        OnEvent = function
            // + concrete domain event handlers
            | Raw event -> event |> handler.OnRawEvent
    }
    |> ParsedMessageReader

let runDomainWithHandler kafkaConfiguration =
    { defaultDomain with
        // + concrete domain event handlers

        OnRawEvent = fun event ->
            event.Event |> incrementCount
    }
    |> DomainEventReader
    |> Consumer.read ignore kafkaConfiguration id
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
fake build
```

### Watch
```bash
fake build target watch
```
