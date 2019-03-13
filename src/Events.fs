namespace Kafka

open System
open ServiceIdentification

//
// Events
//

// Simple types

type EventId = EventId of Guid
module EventId =
    let value (EventId eventId) = eventId

type CorrelationId = CorrelationId of Guid
module CorrelationId =
    let value (CorrelationId correlationId) = correlationId

type CausationId = CausationId of Guid
module CausationId =
    let value (CausationId causationId) = causationId

type EventName = EventName of string
module EventName =
    let value (EventName eventName) = eventName

type Resource = {
    Name: string
    Href: string
}

// Generic event

type Event<'KeyData, 'MetaData, 'DomainData> = {
    Schema: int
    Id: EventId
    CorrelationId: CorrelationId
    CausationId: CausationId
    Timestamp: string
    Event: EventName
    Domain: Domain
    Context: Context
    Purpose: Purpose
    Version: Version
    Zone: Zone
    Bucket: Bucket
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
            Id = event.Id |> EventId
            CorrelationId = event.CorrelationId |> CorrelationId
            CausationId = event.CausationId |> CausationId
            Timestamp = event.Timestamp |> formatDateTime
            Event = event.Event |> EventName
            Domain = event.Domain |> Domain
            Context = event.Context |> Context
            Purpose = event.Purpose |> Purpose
            Version = event.Version |> Version
            Zone = event.Zone |> Zone
            Bucket = event.Bucket |> Bucket
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
