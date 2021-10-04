// Learn more about F# at http://fsharp.org

open System
open Lmc.Kafka
open Lmc.Logging
open Microsoft.Extensions.Logging

[<EntryPoint>]
let main argv =
    printfn "Example\n=======\n"

    let brokerList = "kfall-1.dev1.services.lmc:9092"
    let topic = "consents-interactionCollectorStream-local-v1"
    //let groupId = "consumer-group-id-v8"

    (* printfn "Configuration: %A" [
        //("groupId", groupId)
        ("brokerList", brokerList)
        ("topic", topic)
    ] *)

    use loggerFactory = LoggerFactory.create [
        UseLevel LogLevel.Trace
        LogToConsole
    ]

    let logger = loggerFactory.CreateLogger("Example")

    logger.LogInformation "Start consuming ..."
    let connection = {
        BrokerList = BrokerList brokerList
        Topic = StreamName topic
    }
    let configuration =
        { ConsumerConfiguration.createWithConnection connection GroupId.Random with
            Logger = Some <| loggerFactory.CreateLogger("Kafka")
            Checker = Some Checker.defaultChecker
        }

    let mutable i = 0

    Consumer.consume configuration id
    |> Seq.map (fun m -> i <- i + 1; m)
    |> Seq.take 50
    |> Seq.iter (function
        | Ok { Message = m } -> logger.LogTrace (sprintf "[%02i]Message: string[{length}]" i, m.Length)
        | Error e -> logger.LogError (sprintf "[%02i]Error: {error}" i, e)
    )

    logger.LogInformation "====\nDone\n===="
    0 // return an integer exit code
