// Learn more about F# at http://fsharp.org

open System
open Alma.Kafka
open Alma.Tracing
open Alma.Logging
open Microsoft.Extensions.Logging

let now () = DateTime.Now

module ProduceMultipleMessagesInOneTrace =
    let produceMessages configuration trace messages =
        printfn "Start producing ..."
        use producer = Producer.create configuration

        let produce (message: MessageToProduce) =
            use trace =
                "Produce message"
                |> Trace.ChildOf.startActive trace
                |> Trace.addTags [ "event.key", (message.Key |> MessageKey.value) ]
            printfn "produce message: %A" message
            message
            |> Producer.produceWithTrace producer trace

        messages
        |> List.iter produce

    let run (configuration: ProducerConfiguration) (loggerFactory: ILoggerFactory) =
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
            |> produceMessages configuration exampleTrace

        produceWithKeys()
        exampleTrace |> Trace.finish

module ProduceMultipleMessagesWithOwnTraceForEachMessage =
    let produce producer trace (message: MessageToProduce) =
        use trace =
            "Produce message"
            |> Trace.ChildOf.startActive trace
            |> Trace.addTags [ "event.key", (message.Key |> MessageKey.value) ]
        printfn "produce message: %A" message
        message
        |> Producer.produceWithTrace producer trace

    let run (configuration: ProducerConfiguration) (loggerFactory: ILoggerFactory) =
        let logger = loggerFactory.CreateLogger("Example - producer")
        let configuration = { configuration with Logger = Some logger }
        use producer = Producer.create configuration

        let produce = produce producer

        for i in 1 .. 69 do
            let id = sprintf "%05i" i
            use eventTrace = Trace.Active.start $"Event {i}"

            MessageToProduce.create (MessageKey.Simple $"id_{id}", $"event-{id}-{now()}")
            |> produce eventTrace

[<EntryPoint>]
let main argv =
    printfn "Produce message"
    printfn "==============="

    //let brokerList = "kafka.service.dev1-services.consul:9092"
    let brokerList = Environment.GetEnvironmentVariable("RPK_BROKERS")
    let topicWithASinglePartition = "development-local-experimental-v1"
    let topicWithPartitions = "development-local-experimentalWithPartition-v1"

    let topic = topicWithASinglePartition

    if Tracer.Check.isTracerAvailable() |> not then
        failwithf "Tracer is not available\n%A" (Tracer.Check.environment())

    printfn "Configuration"
    printfn "%A" [
        ("brokerList", brokerList)
        ("topic", topic)
    ]
    let configuration: ProducerConfiguration = ProducerConfiguration.createWithConnection {
        BrokerList = BrokerList brokerList
        Topic = StreamName topic
    }

    use loggerFactory = LoggerFactory.create [
        UseLevel LogLevel.Trace
        LogToConsole
    ]

    // ProduceMultipleMessagesInOneTrace.run configuration loggerFactory
    ProduceMultipleMessagesWithOwnTraceForEachMessage.run configuration loggerFactory

    Tracer.finishTracerProvider()

    printfn "waiting ..."
    System.Threading.Thread.Sleep 2000

    printfn "\nDone\n"
    0 // return an integer exit code
