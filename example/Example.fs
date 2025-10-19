open System
open Microsoft.Extensions.Logging
open MF.ConsoleApplication
open Alma.ErrorHandling
open Alma.Kafka
open Alma.Logging
open Alma.Tracing

module ProduceMultipleMessagesInOneTrace =
    let produceMessages (output: MF.ConsoleApplication.Output) configuration trace messages =
        output.Message "Start producing ..."
        use producer = Producer.create configuration

        let produce (message: MessageToProduce) =
            use trace =
                "Produce message"
                |> Trace.ChildOf.startActive trace
                |> Trace.addTags [ "event.key", (message.Key |> MessageKey.value) ]

            output.Message("produce message: %A", message)
            message
            |> Producer.produceWithTrace producer trace

        messages
        |> List.iter produce

    let run (output: MF.ConsoleApplication.Output) now (configuration: ProducerConfiguration) (loggerFactory: ILoggerFactory) =
        let exampleTrace = Trace.Active.start "Example producer"

        let logger = loggerFactory.CreateLogger("Example - producer")
        logger.LogInformation("Trace {trace}", exampleTrace |> Trace.id)
        let configuration = { configuration with Logger = Some logger }

        let produceWithKeys () =
            [
                MessageToProduce.create (MessageKey.Simple "one", $"event-one-{now()}")
                MessageToProduce.create (MessageKey.Simple "two", $"event-two-{now()}")
                MessageToProduce.create (MessageKey.Simple "three", $"event-three-{now()}")
                //MessageToProduce.create (MessageKey.Simple "four", $"event-four-{now()}")
                //MessageToProduce.create (MessageKey.Simple "five", $"event-five-{now()}")
            ]
            |> produceMessages output configuration exampleTrace

        produceWithKeys()
        exampleTrace |> Trace.finish

module ProduceMultipleMessagesWithOwnTraceForEachMessage =
    let produce (output: MF.ConsoleApplication.Output) producer trace (message: MessageToProduce) =
        use trace =
            "Produce message"
            |> Trace.ChildOf.startActive trace
            |> Trace.addTags [ "event.key", (message.Key |> MessageKey.value) ]
        output.Message("produce message: %A", message)
        message
        |> Producer.produceWithTrace producer trace

    let run output now (configuration: ProducerConfiguration) (loggerFactory: ILoggerFactory) =
        let logger = loggerFactory.CreateLogger("Example - producer")
        let configuration = { configuration with Logger = Some logger }
        use producer = Producer.create configuration

        let produce = produce output producer

        for i in 1 .. 105 do
            let id = sprintf "%05i" i
            use eventTrace = Trace.Active.start $"Event {i}"
            let key = i % 10

            MessageToProduce.create (MessageKey.Simple $"key_{key}", $"event[P:{key}]-{id}-{now()}")
            |> produce eventTrace

[<EntryPoint>]
let main argv =
    consoleApplication {
        name "Example"
        version "1.0.0"
        info ApplicationInfo.NameAndVersion

        command "consume" {
            Description = "Example of consuming events."
            Help = None
            Arguments = []
            Options = [
                Option.optionalArray "broker" (Some "b") "Broker list" (Some [ "127.0.0.1:19092" ])
                Option.optional "topic" None "Topic to consume from" (Some "development-local-experimental-v1")
                Option.optional "group-id" None "Consumer group ID" None
                Option.noValue "no-autocommit" None "Disable auto-commit"
                Option.noValue "allow-skip" None "Allow skipping commits on errors"
                Option.optional "limit" None "Limit of consumed messages" (Some "0")
            ]
            Initialize = None
            Interact = None
            Execute = Execute <| fun (input, output) ->
                let brokerList =
                    match input |> Input.Option.asList "broker" with
                    | [] -> Environment.GetEnvironmentVariable("RPK_BROKERS") |> string
                    | brokers -> brokers |> String.concat ","

                let stream =
                    input
                    |> Input.Option.asString "topic"
                    |> Option.defaultValue "development-local-experimental-v1"

                let groupId =
                    match input with
                    | Input.Option.IsSet "group-id" (OptionValue.ValueOptional (Some value)) when value |> String.IsNullOrWhiteSpace |> not ->  GroupId.Id value
                    | _ -> GroupId.Random

                /// default: true
                let enableAutocommit =
                    match input with
                    | Input.Option.IsSet "no-autocommit" _ -> false
                    | _ -> true

                /// If the value is true and autocommit is disabled, it will end with errors (to simulate the problem)
                let allowSkip =
                    match input with
                    | Input.Option.IsSet "allow-skip" _ -> true
                    | _ -> false

                let limit = input |> Input.Option.asInt "limit" |> Option.defaultValue 0

                if output.IsVerbose() then
                    output.Table ["Option"; "Value"] [
                        [ "brokerList"; try brokerList with _ -> "N/A" ]
                        [ "topic"; try stream with _ -> "N/A" ]
                        [ "groupId"; try groupId |> sprintf "%A" with _ -> "N/A" ]
                        [ "enableAutocommit"; try enableAutocommit |> sprintf "%A" with _ -> "N/A" ]
                        [ "allowSkip"; try allowSkip |> sprintf "%A" with _ -> "N/A" ]
                        [ "limit"; try limit |> sprintf "%A" with _ -> "N/A" ]
                    ]

                use loggerFactory = LoggerFactory.create [
                    UseLevel (if output.IsVerbose() then LogLevel.Trace else LogLevel.Information)
                    LogToConsole
                ]

                let logger = loggerFactory.CreateLogger("Example - consumer")

                output.Message "Start consuming ..."
                let connection = {
                    BrokerList = BrokerList brokerList
                    Topic = StreamName stream
                }
                let configuration =
                    { ConsumerConfiguration.createWithConnection connection groupId with
                        Logger = Some <| loggerFactory.CreateLogger("Kafka")
                        Checker = Some Checker.defaultChecker
                        CommitMessage =
                            if not enableAutocommit then CommitMessage.Manually FailOnNotCommittedMessage.WithException
                            else CommitMessage.Automatically
                    }

                let mutable i = 0

                let execute () =
                    Consumer.consumeMessages configuration id
                    |> Seq.map (fun m -> i <- i + 1; m)
                    |> fun stream -> if limit > 0 then stream |> Seq.take limit else stream
                    |> Seq.iter (function
                        | Ok { Message = m } ->
                            output.Message (
                                "[<c:magenta>%02i</c>] Message[<c:yellow>P:%A|O:%A</c>]: %s[%i]",
                                i,
                                m.Message.Partition,
                                m.Message.Offset,
                                m.Message.Value,
                                m.Message.Value.Length
                            )

                            //System.Threading.Thread.Sleep 1000

                            if not enableAutocommit then
                                if allowSkip && System.Random().Next(0, 6) >= 4 then
                                    // simulation of error, which leads to skip the commit
                                    output.Message (
                                        "[<c:magenta>%02i</c>] Message<<c:yellow>O:%A</c>> --> SKIP commit",
                                        i,
                                        m.Message.Offset
                                    )
                                else
                                    match m.Commit |> ManualCommit.execute with
                                    | Ok () ->
                                        output.Message (
                                            "[<c:magenta>%02i</c>] Message<<c:yellow>O:%A</c>> --> is committed",
                                            i,
                                            m.Message.Offset
                                        )
                                    | Error e ->
                                        output.Error (
                                            "[<c:magenta>%02i</c>] Error: %A",
                                            i,
                                            e
                                        )
                                        failwithf "%A" e

                        | Error (ConsumeError.PreviousMessageWasNotCommited as e) ->
                            logger.LogError (sprintf "[%02i] Error: {error}" i, e)
                            failwithf "Commit skipped!"

                        | Error e -> logger.LogError (sprintf "[%02i] Error: {error}" i, e)
                    )

                execute()

                ExitCode.Success
            }

        command "produce" {
            Description = "Example of producing events."
            Help = None
            Arguments = []
            Options = [
                Option.optionalArray "broker" (Some "b") "Broker list" (Some [ "127.0.0.1:19092" ])
                Option.optional "topic" None "Topic to consume from" (Some "development-local-experimental-v1")
                Option.noValue "one-trace" None "Produce all messages in one trace"
                Option.noValue "require-tracer" None "Check tracer availability and fail, if not available"
            ]
            Initialize = None
            Interact = None
            Execute = Execute <| fun (input, output) ->
                let now () = DateTime.Now

                let brokerList =
                    match input |> Input.Option.asList "broker" with
                    | [] -> Environment.GetEnvironmentVariable("RPK_BROKERS") |> string
                    | brokers -> brokers |> String.concat ","

                let stream =
                    input
                    |> Input.Option.asString "topic"
                    |> Option.defaultValue "development-local-experimental-v1"

                let inOneTrace =
                    match input with
                    | Input.Option.IsSet "one-trace" _ -> true
                    | _ -> false

                let isTracerRequired =
                    match input with
                    | Input.Option.IsSet "require-tracer" _ -> true
                    | _ -> false

                if isTracerRequired && Tracer.Check.isTracerAvailable() |> not then
                    failwithf "Tracer is not available\n%A" (Tracer.Check.environment())

                output.Message "Configuration"
                if output.IsVerbose() then
                    output.Table ["Option"; "Value"] [
                        [ "brokerList"; try brokerList with _ -> "N/A" ]
                        [ "topic"; try stream with _ -> "N/A" ]
                        [ "inOneTrace"; try inOneTrace |> sprintf "%A" with _ -> "N/A" ]
                        [ "isTracerRequired"; try isTracerRequired |> sprintf "%A" with _ -> "N/A" ]
                    ]

                let configuration: ProducerConfiguration = ProducerConfiguration.createWithConnection {
                    BrokerList = BrokerList brokerList
                    Topic = StreamName stream
                }

                use loggerFactory = LoggerFactory.create [
                    UseLevel LogLevel.Trace
                    LogToConsole
                ]

                if inOneTrace then
                    ProduceMultipleMessagesInOneTrace.run output now configuration loggerFactory
                else
                    ProduceMultipleMessagesWithOwnTraceForEachMessage.run output now configuration loggerFactory

                Tracer.finishTracerProvider()

                output.Message "waiting ..."
                System.Threading.Thread.Sleep 2000

                ExitCode.Success
        }

        command "admin" {
            Description = "Example of admin access for kafka."
            Help = None
            Arguments = []
            Options = [
                Option.optionalArray "broker" (Some "b") "Broker list" (Some [ "127.0.0.1:19092" ])
                Option.optional "topic" None "Topic to consume from" (Some "development-local-experimental-v1")
                Option.optional "group-id" None "Consumer group ID" None
            ]
            Initialize = None
            Interact = None
            Execute = ExecuteAsync <| fun (input, output) -> async {
                let brokerList =
                    match input |> Input.Option.asList "broker" with
                    | [] -> Environment.GetEnvironmentVariable("RPK_BROKERS") |> string
                    | brokers -> brokers |> String.concat ","

                let stream =
                    input
                    |> Input.Option.asString "topic"
                    |> Option.defaultValue "development-local-experimental-v1"

                let groupId =
                    match input with
                    | Input.Option.IsSet "group-id" (OptionValue.ValueOptional (Some value)) when value |> String.IsNullOrWhiteSpace |> not ->  GroupId.Id value
                    | _ -> GroupId.Random

                if output.IsVerbose() then
                    output.Table ["Option"; "Value"] [
                        [ "brokerList"; try brokerList with _ -> "N/A" ]
                        [ "topic"; try stream with _ -> "N/A" ]
                        [ "groupId"; try groupId |> sprintf "%A" with _ -> "N/A" ]
                    ]

                use loggerFactory = LoggerFactory.create [
                    UseLevel (if output.IsVerbose() then LogLevel.Trace else LogLevel.Information)
                    LogToConsole
                ]

                let logger = loggerFactory.CreateLogger("Example - admin")
                let brokerList = BrokerList brokerList

                output.Section "List of all topics"
                use admin = Admin.createAdmin brokerList
                Admin.getAllTopics admin
                |> List.sortBy StreamName.value
                |> List.map (StreamName.value >> List.singleton)
                |> output.Options "topics:"

                output.Section "Partition lags"
                output.Message (" - for stream: <c:cyan>%s</c>", stream)
                output.Message (" - for group: <c:yellow>%s</c>", groupId |> GroupId.value)
                let! partitionLags = Admin.lags logger { BrokerList = brokerList; Topic = StreamName stream } groupId

                let totalLag =
                    partitionLags
                    |> List.sumBy Admin.PartitionLag.lag

                partitionLags
                |> List.map (fun partitionLag ->
                    [ sprintf " - partition[%d]" partitionLag.Partition; sprintf "%A" partitionLag.Lag ]
                )
                |> output.Table ["Partition"; "Lag"]

                output.Message ("Total lag: <c:magenta>%A</c>", totalLag)

                return ExitCode.Success
            }
        }
    }
    |> run argv
