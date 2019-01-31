namespace Kafka

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

type Configuration = {
    BrokerList: string
    Topic: string
}
