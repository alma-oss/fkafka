// Learn more about F# at http://fsharp.org

open System
open Lmc.Kafka
open MF.ConsoleStyle

[<EntryPoint>]
let main argv =
    Console.title "Admin - kafka"
    let brokerList = BrokerList "kfall-1.dev1.services.lmc:9092"
    let topic = StreamName "consents-interactionStream-development-v1"
    let groupId = GroupId.Id "consents-intentStreamAggregator-common-stable"

    use admin = Admin.createAdmin brokerList

    Admin.getAllTopics admin
    |> printfn "topics:\n%A"

    topic
    |> Admin.topicExists admin
    |> printfn "topic %s exists: %A" topic

    Console.success "Done"
    0 // return an integer exit code
