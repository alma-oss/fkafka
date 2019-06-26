// Learn more about F# at http://fsharp.org

open System
open Kafka
open MF.ConsoleStyle

[<EntryPoint>]
let main argv =
    Console.title "Consume messages"
    let brokerList = "kfall-1.dev1.services.lmc:9092"
    let topic = "consents-intentStream-development-v1"
    //let groupId = "consumer-group-id-v8"

    Console.options "Configuration" [
        //("groupId", groupId)
        ("brokerList", brokerList)
        ("topic", topic)
    ]

    Console.message "Start consuming ..."
    let connection = {
        BrokerList = BrokerList brokerList
        Topic = StreamName topic
    }
    let configuration =
        { ConsumerConfiguration.createWithConnection connection GroupId.Random with
            Logger = Some {
                Log = Console.messagef "[Kafka] %s"
            }
            Checker = Some Checker.defaultChecker
        }

    Consumer.consumeLastMessage configuration
    |> printfn "%A"

    printfn "Expected: 0e4405ca-9866-43a9-a005-0e44c11b904a"

    //DecodedMessageReader { ReadMessage = (printfn " - Replay Event: %A") }
    //|> Consumer.consumeStreamToOffset configuration (int64 14)

    //DecodedMessageReader { ReadMessage = (printfn " - Event: %A") }
    //|> Consumer.consumeStreamWithGroupId Console.message configuration groupId

    Console.success "Done"
    0 // return an integer exit code
