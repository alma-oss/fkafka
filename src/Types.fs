namespace Kafka

//
// Common
//

[<Measure>] type second

type BrokerList = BrokerList of string
type StreamName = StreamName of string

module StreamName =
    let value (StreamName streamName) = streamName

type GroupId =
    | Random
    | Id of string

module GroupId =
    let map f = function
        | Id groupId -> groupId |> f |> Id
        | Random -> Random

    let value = function
        | Id groupId -> groupId
        | Random -> sprintf "random-%d" System.DateTime.Now.Ticks    // random (unique) group id means, it will always starts from the beginning

type ServiceStatus = {
    MarkAsEnabled: unit -> unit
    MarkAsDisabled: unit -> unit
}

type Logger = {
    Log: string -> unit
}

type ConnectionConfiguration = {
    BrokerList: BrokerList
    Topic: StreamName
}
