// Learn more about F# at http://fsharp.org

open System
open Lmc.Kafka
open Lmc.Tracing
open Lmc.Logging
open Microsoft.Extensions.Logging

let produceMessages configuration trace messages =
    printfn "Start producing ..."
    use producer = Producer.create configuration

    let produce message =
        printfn "produce message: %A" message
        message
        |> Producer.produceWithTrace producer trace

    messages
    |> List.iter produce

[<EntryPoint>]
let main argv =
    printfn "Produce message"
    printfn "==============="

    let exampleTrace = Trace.Active.start "Example producer"

    let brokerList = "kfall-1.dev1.services.lmc:9092"
    let topic = "development-local-experimental-v1"
    let topicWithPartitions = "development-local-experimentalWithPartition-v1"

    printfn "Configuration"
    printfn "%A" [
        ("brokerList", brokerList)
        ("topic", topic)
    ]

    let configuration: ProducerConfiguration = ProducerConfiguration.createWithConnection {
        BrokerList = BrokerList brokerList
        Topic = StreamName topicWithPartitions
    }

    use loggerFactory = LoggerFactory.create [
        UseLevel LogLevel.Trace
        LogToConsole
    ]

    let logger = loggerFactory.CreateLogger("Example - producer")
    logger.LogInformation("Trace {trace}", exampleTrace |> Trace.id)
    let configuration = { configuration with Logger = Some logger }

    let produceWithKeys () =
        [
            MessageToProduce.create (MessageKey.Delimited ["all";"common";"one"], "one")
            MessageToProduce.create (MessageKey.Delimited ["all";"common";"two"], "two")
            MessageToProduce.create (MessageKey.Delimited ["all";"common";"three"], "three")
            MessageToProduce.create (MessageKey.Delimited ["all";"common";"two"], "two(2)")
            MessageToProduce.create (MessageKey.Delimited ["all";"common";"four"], "four")
            MessageToProduce.create (MessageKey.Delimited ["all";"common";"five"], "five")
            MessageToProduce.create (MessageKey.Delimited ["all";"common";"five"], "five(2)")
        ]
        |> List.take 1
        |> produceMessages configuration exampleTrace

    produceWithKeys()
    exampleTrace |> Trace.finish

    printfn "waiting ..."
    System.Threading.Thread.Sleep 2000

    printfn "\nDone\n"
    0 // return an integer exit code
