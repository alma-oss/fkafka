// Learn more about F# at http://fsharp.org

open System
open Alma.Kafka
open Alma.Logging
open Microsoft.Extensions.Logging

[<EntryPoint>]
let main argv =
    printfn "Example\n=======\n"

    //let brokerList = "kfall-2.dev1.services.lmc:9092"
    //let brokerList = "kafka.service.dev1-services.consul:9092"
    let brokerList = Environment.GetEnvironmentVariable("RPK_BROKERS")
    let topic = "development-local-experimental-v1"
    let topicWithPartitions = "development-local-experimentalWithPartition-v1"
    let organizationStream = "consents-intentOrganizationStream-development-v1v2"

    let stream = topic
    //let groupId = "consumer-group-id-v2.0.2+2.1.1-2"
    let groupId = "consumer-group-id-v2.3.0.A1"
    let groupId = "consumer-2024-04-10--01" // with lag
    let groupId = "consumer-2024-04-10--02" // without lag

    /// default: true
    let enableAutocommit = true

    /// If the value is true and autocommit is disabled, it will end with errors (to simulate the problem)
    let allowSkip = false

    (* printfn "Configuration: %A" [
        //("groupId", groupId)
        ("brokerList", brokerList)
        ("topic", topic)
    ] *)

    use loggerFactory = LoggerFactory.create [
        UseLevel LogLevel.Trace
        LogToConsole
    ]

    let logger = loggerFactory.CreateLogger("Example - consumer")

    logger.LogInformation "Start consuming ..."
    let connection = {
        BrokerList = BrokerList brokerList
        Topic = StreamName stream
    }
    let configuration =
        { ConsumerConfiguration.createWithConnection connection (GroupId.Id groupId) with
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
        //|> Seq.take 5
        |> Seq.iter (function
            | Ok { Message = m } ->
                logger.LogTrace (
                    sprintf "[%02i] Message[P:{partition}|O:{offset}]: {value}[{length}]" i,
                    m.Message.Partition,
                    m.Message.Offset,
                    m.Message.Value,
                    m.Message.Value.Length
                )

                //System.Threading.Thread.Sleep 1000

                if not enableAutocommit then
                    if allowSkip && System.Random().Next(0, 6) >= 4 then
                        // simulation of error, which leads to skip the commit
                        logger.LogTrace (sprintf "[%02i] Message<O:{offset}> --> SKIP commit" i, m.Message.Offset)

                    else
                        match m.Commit |> ManualCommit.execute with
                        | Ok () ->
                            logger.LogTrace (sprintf "[%02i] Message<O:{offset}> --> is commited" i, m.Message.Offset)
                        | Error e ->
                            logger.LogError (sprintf "[%02i] Error: {error}" i, e)
                            failwithf "%A" e

            | Error (ConsumeError.PreviousMessageWasNotCommited as e) ->
                logger.LogError (sprintf "[%02i] Error: {error}" i, e)
                failwithf "Commit skipped!"

            | Error e -> logger.LogError (sprintf "[%02i] Error: {error}" i, e)
        )

    execute()

    logger.LogInformation "====\nDone\n===="
    0 // return an integer exit code
