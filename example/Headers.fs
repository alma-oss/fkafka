open System
open Alma.Kafka

let produceEvents connection events = async {
    printfn "Produce events"
    printfn "--------------\n"

    printfn "Connect producer ..."

    let producer =
        connection
        |> ProducerConfiguration.createWithConnection
        |> Producer.prepareProducer
        |> Producer.connect

    printfn "Produce events ..."

    do!
        events
        |> List.mapi (fun i (headers, message) ->
            async {
                let wait = (i + 1) * 2000
                printfn "Produce event and wait for %A ms" wait
                message |> Producer.produceWithHeaders producer headers

                printf "Waiting ... "
                do! Async.Sleep wait
                printfn "Done"
            }
        )
        |> Async.Sequential
        |> Async.map ignore
}

let consumeEvents connection f = async {
    printfn "Consume events"
    printfn "--------------\n"

    printfn "Connect consumer ..."

    GroupId.Random
    |> ConsumerConfiguration.createWithConnection connection
    |> Consumer.consumeMessagesWithHeaders
    |> Seq.iter f
}

[<EntryPoint>]
let main argv =
    printfn "Headers example"
    printfn "===============\n"

    let connection = {
        BrokerList = BrokerList "kfall-1.dev1.services.lmc:9092"
        Topic = StreamName "consumer-offset-test-v8"
    }

    let header key value = Header.ofString (HeaderKey key) value

    let events =
        [
            [ header "TraceId" "1" ], "event1"
            [ header "TraceId" "2"; header "Span" "span2" ], "event2"
            [], "event3"
            [ header "TraceId" "4" ], "event4"
            [ header "TraceId" "5" ], "event5"
        ]

    [
        produceEvents connection events
        consumeEvents connection (fun message ->
            printfn "Consumed event[%A] %A with [ %s ]"
                message.Offset
                message.Value
                (
                    message.Headers
                    |> List.map (fun header -> sprintf "%s: %s" (header.Key |> HeaderKey.value) (header |> Header.valueAsString))
                    |> String.concat ", "
                )
        )
    ]
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    printfn "Done"
    0
