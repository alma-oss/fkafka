// Learn more about F# at http://fsharp.org

open System
open Microsoft.Extensions.Logging
open Alma.Kafka
open Alma.Kafka.Admin
open Alma.Logging
open Alma.ErrorHandling

[<EntryPoint>]
let main argv =
    printfn "Admin - kafka"
    let brokerList = BrokerList "kafka.service.dev1-services.consul:9092"
    let topic = StreamName "consents-consentorStream-development-v1"
    let groupId = GroupId.Id "consents-eventStreamCBRouter-common-stable_v1v1"

    use loggerFactory = LoggerFactory.create [
        UseLevel LogLevel.Trace
        LogToConsole
    ]
    let logger = loggerFactory.CreateLogger("Example - admin")

    use admin = Admin.createAdmin brokerList

    (* Admin.getAllTopics admin
    |> List.filter (fun stream -> (stream |> StreamName.value).StartsWith "consents")
    |> List.sortBy StreamName.value
    |> printfn "topics:\n%A" *)

    topic
    |> Admin.topicExists admin
    |> printfn "topic %A exists: %A\n" topic

    let partitionLags =
        Admin.lags logger { BrokerList = brokerList; Topic = topic } groupId
        |> Async.RunSynchronously

    let totalLag =
        partitionLags
        |> List.sumBy PartitionLag.lag

    printfn "total lag: %A" totalLag

    printfn "Done"
    0 // return an integer exit code
