F-Kafka
=======

Library for reading messages or producing messages to/from stream.

## Install
```
dotnet add package -s $NUGET_SERVER_PATH Lmc.Kafka
```
Where `$NUGET_SERVER_PATH` is the URL of nuget server
- it should be http://development-nugetserver-common-stable.service.devel1-services.consul:31794 (_make sure you have a correct port, since it changes with deployment_)
- see http://consul-1.infra.pprod/ui/devel1-services/services/development-nugetServer-common-stable for detailed information (and port)

## Use
```fs
open Kafka

let logMessage = printfn "%s"   // this function will be used for logging, it gets a simple message of what kafka lib is doing
let incrementMessageCount = id  // this functin will be used for incrementing a message count, it gets raw event content (string) for each consumed event

let configuration = {
    BrokerList = "127.0.0.1:9092,"  // list of all brokers
    Topic = "my-topic"              // topic name
}

(fun baseEvent -> printfn "%A" baseEvent)   // on BaseEvent handler
|> BaseEvent.messageReader                  // there are more available readers (see Kafka.{...}Reader)
|> Consumer.consumeStream logMessage configuration incrementMessageCount
```

## Release
1. Increment version in `src/Kafka.fsproj`
2. Update `CHANGELOG.md`
3. Commit new version and tag it
4. Run `$ fake build target release`

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
