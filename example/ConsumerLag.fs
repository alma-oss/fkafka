// Learn more about F# at http://fsharp.org

open System
open Alma.Kafka
open Alma.Logging
open Alma.ErrorHandling
open Microsoft.Extensions.Logging

(* type Message = {
    Value: string
    Offset: int
    Lag: int
}

let data =
    [
        { Value = "one"; Offset = 1; Lag = 5 }
        { Value = "two"; Offset = 2; Lag = 4 }
        { Value = "three"; Offset = 3; Lag = 3 }
        { Value = "four"; Offset = 4; Lag = 2 }
        { Value = "five"; Offset = 5; Lag = 1 }
        { Value = "six"; Offset = 6; Lag = 0 }
        { Value = "seven"; Offset = 7; Lag = 0 }
    ]
    |> Seq.ofList

data
//|> Seq.choose (fun m -> if m.Lag = 0 then None else Some m)
|> Seq.takeWhile (fun m -> m.Lag > 0)
|> Seq.iter (fun m -> m.Value |> printfn "- %s") *)

let runConsume configuration (logger: ILogger) =
    let mutable i = 0

    Consumer.consumeMessages configuration id
    |> Seq.map (fun m -> i <- i + 1; m)
    (* |> Seq.takeWhile (function
        | Ok { Message = { Message = { Lag = Some lag }}} -> lag > (int64 0)
        | _ -> false
    ) *)
    |> Seq.pick (function
        | Ok { Message = m } ->
            logger.LogTrace (
                sprintf "[%02i] Message[P:{partition}|O:{offset}]: {value}[{length}] - Lag:{lag}" i,
                m.Message.Partition,
                m.Message.Offset,
                "m.Message.Value",
                m.Message.Value.Length,
                m.Message.Lag
            )

            // System.Threading.Thread.Sleep 500

            if m.Message.Lag > Some (int64 0)
            then
                logger.LogInformation("Message with lag {lag} -> continue reading ...", m.Message.Lag)
                None
            else
                logger.LogInformation("Message with lag {lag} -> stop reading!", m.Message.Lag)
                Some ()

        | Error (ConsumeError.PreviousMessageWasNotCommited as e) ->
            logger.LogError (sprintf "[%02i] Error: {error}" i, e)
            failwithf "Commit skipped!"
            Some()

        | Error e ->
            logger.LogError (sprintf "[%02i] Error: {error}" i, e)
            Some()
    )

[<EntryPoint>]
let main argv =
    printfn "Example\n=======\n"

    let brokerList = "kfall-2.dev1.services.lmc:9092"
    let topicWithPartitions = "development-local-experimentalWithPartition-v1"
    //let topicWithPartitions = "consents-contractAggregateStateInterpreterStream-development-v1"

    let groupId = "consumer-lag-group-id-v010"

    use loggerFactory = LoggerFactory.create [
        UseLevel LogLevel.Trace
        LogToConsole
    ]

    let logger = loggerFactory.CreateLogger("Example - consumer.lag")

    logger.LogInformation "Start consuming ..."
    let connection = {
        BrokerList = BrokerList brokerList
        Topic = StreamName topicWithPartitions
    }
    let configuration =
        { ConsumerConfiguration.createWithConnection connection (GroupId.Id groupId) with
            Logger = Some <| loggerFactory.CreateLogger("Kafka")
            Checker = Some Checker.defaultChecker
            CountLag = true
        }

    let workers = 1

    let stopWatch = System.Diagnostics.Stopwatch.StartNew()
    [ 1 .. workers ]
    |> List.map (fun i -> async {
        let logger = loggerFactory.CreateLogger($"Consumer[{i}]")
        runConsume configuration logger
    })
    |> Async.Parallel
    |> Async.Ignore
    |> Async.RunSynchronously
    stopWatch.Stop()

    logger.LogInformation("====\nDone in {duration}ms\n====", stopWatch.Elapsed.TotalMilliseconds)
    0 // return an integer exit code
