namespace Kafka

//
// Common
//

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

type Configuration = {
    BrokerList: BrokerList
    Topic: StreamName
}

//
// Kafka readers
//

type DecodedMessageReader = {
    ReadMessage: string -> unit
}

type ParsedMessageReader<'Event> = {
    ParseEvent : string -> 'Event
    OnEvent: 'Event -> unit
}

type MessageReader<'Event> =
    | DecodedMessageReader of DecodedMessageReader
    | ParsedMessageReader of ParsedMessageReader<'Event>
