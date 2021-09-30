// Learn more about F# at http://fsharp.org

open System
open Lmc.Kafka

[<EntryPoint>]
let main argv =
    printfn "Consume messages"
    let brokerList = "kfall-1.dev1.services.lmc:9092"
    let topic = "consents-interactionCollectorStream-local-v1"
    //let groupId = "consumer-group-id-v8"

    (* printfn "Configuration: %A" [
        //("groupId", groupId)
        ("brokerList", brokerList)
        ("topic", topic)
    ] *)

    printfn "Start consuming ..."
    let connection = {
        BrokerList = BrokerList brokerList
        Topic = StreamName topic
    }
    let configuration =
        { ConsumerConfiguration.createWithConnection connection GroupId.Random with
            Logger = Some {
                Log = printfn "[Kafka] %s"
            }
            Checker = Some Checker.defaultChecker
        }

    let mutable i = 0

    Consumer.consume configuration id
    |> Seq.iter (function
        | Ok { Message = m } -> printfn "Message[%A]: string[%A]" i m.Length
        | Error e -> printfn "Error: %A" e
    )

    printfn "====\nDone\n===="
    0 // return an integer exit code
