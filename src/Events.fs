namespace Kafka

open System

//
// Events
//

type Resource = {
    Name: string
    Href: string
}

type Event<'KeyData, 'MetaData, 'DomainData> = {
    Schema: int
    Id: Guid
    CorrelationId: Guid
    CausationId: Guid
    Timestamp: string
    Event: string
    Domain: string
    Context: string
    Purpose: string
    Version: string
    Zone: string
    Bucket: string
    Resource: Resource option
    MetaData: 'MetaData
    KeyData: 'KeyData
    DomainData: 'DomainData
}

type RawData = RawData of FSharp.Data.JsonValue

type RawEvent = Event<RawData, RawData option, RawData option>

[<RequireQualifiedAccessAttribute>]
module RawEvent =
    open FSharp.Data

    type private Schema1 = JsonProvider<"schema/events.json", SampleIsList=true>

    let private formatDateTime (dateTimeOffset: DateTimeOffset) =
        dateTimeOffset.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'")

    let parse message: RawEvent =
        let event = message |> Schema1.Parse

        {
            Schema = 1
            Id = event.Id
            CorrelationId = event.CorrelationId
            CausationId = event.CausationId
            Timestamp = event.Timestamp |> formatDateTime
            Event = event.Event
            Domain = event.Domain
            Context = event.Context
            Purpose = event.Purpose
            Version = event.Version
            Zone = event.Zone
            Bucket = event.Bucket
            KeyData = RawData event.KeyData.JsonValue
            Resource = Some {
                Name = event.Resource.Name
                Href = event.Resource.Href
            }
            MetaData = Some (RawData event.MetaData.JsonValue)
            DomainData = Some (RawData event.DomainData.JsonValue)
        }

    let messageReader onEvent =
        {
            ParseEvent = parse
            OnEvent = onEvent
        }
        |> ParsedMessageReader
