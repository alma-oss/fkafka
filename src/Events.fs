namespace Kafka

open System

//
// Events
//

type Metadata = {
    CreatedAt: string
}

type Resource = {
    Name: string
    Href: string
}

type Event<'KeyData, 'DomainData> = {
    Schema: int
    Id: Guid
    CorrelationId: Guid
    Timestamp: string
    Event: string
    Domain: string
    Context: string
    Purpose: string
    Version: string
    Zone: string
    Bucket: string
    MetaData: Metadata
    Resource: Resource
    KeyData: 'KeyData
    DomainData: 'DomainData
}

type RawData = RawData of FSharp.Data.JsonValue

type BaseEvent = Event<RawData, RawData>

[<RequireQualifiedAccessAttribute>]
module BaseEvent =
    open FSharp.Data

    type private Schema1 = JsonProvider<"schema/events.json", SampleIsList=true>

    let private formatDateTime (dateTimeOffset: DateTimeOffset) =
        dateTimeOffset.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'")

    let private parse message: BaseEvent =
        let event = message |> Schema1.Parse

        {
            Schema = 1
            Id = event.Id
            CorrelationId = event.CorrelationId
            Timestamp = event.Timestamp |> formatDateTime
            Event = event.Event
            Domain = event.Domain
            Context = event.Context
            Purpose = event.Purpose
            Version = event.Version
            Zone = event.Zone
            Bucket = event.Bucket
            MetaData = {
                CreatedAt = event.MetaData.CreatedAt |> formatDateTime
            }
            Resource = {
                Name = event.Resource.Name
                Href = event.Resource.Href
            }
            KeyData = RawData event.KeyData.JsonValue
            DomainData = RawData event.DomainData.JsonValue
        }

    let messageReader onEvent =
        {
            ParseEvent = parse
            OnEvent = onEvent
        }
        |> ParsedMessageReader
