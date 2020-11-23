// Learn more about F# at http://fsharp.org

open System
open MF.ConsoleStyle
open Lmc.Kafka

[<EntryPoint>]
let main argv =
    Console.title "Produce message"
    let brokerList = "kfall-1.dev1.services.lmc:9092"
    let topic = "consumer-offset-test-v8"

    Console.options "Configuration" [
        ("brokerList", brokerList)
        ("topic", topic)
    ]
    let configuration = {
        BrokerList = brokerList
        Topic = topic
    }

    Console.message "Start producing ..."
    //use producer = Producer.createProducer brokerList
    //let produce message = 
    //    printfn "produce message: %A" message
    //    message
    //    |> Producer.produceMessage producer topic

    [
        //"one"
        //"two"
        //"three"
        //"four"
        //"five"

        //"six"
        //"seven"
        //"eight"
        //"nine"
        //"ten"
        
        "eleven"
        "twelve"
    ]
    |> Producer.produce configuration

    Console.success "Done"
    0 // return an integer exit code
