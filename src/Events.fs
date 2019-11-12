namespace Kafka

open System
open ServiceIdentification

//
// Events
//

// Simple types

type EventId = EventId of Guid

[<RequireQualifiedAccess>]
module EventId =
    let value (EventId eventId) = eventId

type CorrelationId = CorrelationId of Guid

[<RequireQualifiedAccess>]
module CorrelationId =
    let value (CorrelationId correlationId) = correlationId

type CausationId = CausationId of Guid

[<RequireQualifiedAccess>]
module CausationId =
    let value (CausationId causationId) = causationId
    let fromEventId = EventId.value >> CausationId

type EventName = EventName of string

[<RequireQualifiedAccess>]
module EventName =
    let value (EventName eventName) = eventName

type Resource = {
    Name: string
    Href: string
}

type ResourceDto = {
    name: string
    href: string
}

[<RequireQualifiedAccess>]
module Resource =
    let toDto (resource: Resource) =
        {
            name = resource.Name
            href = resource.Href
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

type CommonEvent = {
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
}

type NoData = unit

type EventDto<'ResourceDto, 'KeyDataDto, 'MetaDataDto, 'DomainDataDto> = {
    schema: int
    id: Guid
    correlation_id: Guid
    causation_id: Guid
    timestamp: string
    event: string
    domain: string
    context: string
    purpose: string
    version: string
    zone: string
    bucket: string
    resource: 'ResourceDto
    meta_data: 'MetaDataDto
    key_data: 'KeyDataDto
    domain_data: 'DomainDataDto
}

type SerializeEvent<'KeyData, 'MetaData, 'DomainData, 'ResourceDto, 'KeyDataDto, 'MetaDataDto, 'DomainDataDto, 'Error> =
    (Event<'KeyData, 'MetaData, 'DomainData> -> Result<unit, 'Error>)
        -> (Resource option -> Result<'ResourceDto, 'Error>)
        -> ('MetaData -> Result<'MetaDataDto, 'Error>)
        -> ('KeyData -> Result<'KeyDataDto, 'Error>)
        -> ('DomainData -> Result<'DomainDataDto, 'Error>)
        -> Event<'KeyData, 'MetaData, 'DomainData>
        -> Result<EventDto<'ResourceDto, 'KeyDataDto, 'MetaDataDto, 'DomainDataDto>, 'Error>

[<RequireQualifiedAccess>]
module Event =
    let toCommon (event: Event<'KeyData, 'MetaData, 'DomainData>) =
        {
            Schema = event.Schema
            Id = event.Id
            CorrelationId = event.CorrelationId
            CausationId = event.CausationId
            Timestamp = event.Timestamp
            Event = event.Event
            Domain = event.Domain
            Context = event.Context
            Purpose = event.Purpose
            Version = event.Version
            Zone = event.Zone
            Bucket = event.Bucket
            Resource = event.Resource
        }

    let toDto: SerializeEvent<'KeyData, 'MetaData, 'DomainData, 'ResourceDto, 'KeyDataDto, 'MetaDataDto, 'DomainDataDto, 'Error> =
        fun assertEventType serializeResource serializeMetaData serializeKeyData serializeDomainData event ->
            result {
                do! assertEventType event

                let! resourceDto = event.Resource |> serializeResource
                let! metaDataDto = event.MetaData |> serializeMetaData
                let! keyDataDto = event.KeyData |> serializeKeyData
                let! domainDataDto = event.DomainData |> serializeDomainData

                return {
                    schema = event.Schema
                    id = event.Id |> EventId.value
                    correlation_id = event.CorrelationId |> CorrelationId.value
                    causation_id = event.CausationId |> CausationId.value
                    timestamp = event.Timestamp
                    event = event.Event |> EventName.value
                    domain = event.Domain |> Domain.value
                    context = event.Context |> Context.value
                    purpose = event.Purpose |> Purpose.value
                    version = event.Version |> Version.value
                    zone = event.Zone |> Zone.value
                    bucket = event.Bucket |> Bucket.value
                    meta_data = metaDataDto
                    resource = resourceDto
                    key_data = keyDataDto
                    domain_data = domainDataDto
                }
            }

[<RequireQualifiedAccess>]
module CommonEvent =
    let box (event: CommonEvent) =
        Box.createFromValues
            event.Domain
            event.Context
            event.Purpose
            event.Version
            event.Zone
            event.Bucket

type RawData = RawData of FSharp.Data.JsonValue

[<RequireQualifiedAccess>]
module RawData =
    open FSharp.Data

    let toJson (RawData data) =
        data.ToString(JsonSaveOptions.DisableFormatting)

type RawEvent = Event<RawData, RawData option, RawData option>

[<RequireQualifiedAccess>]
module RawEvent =
    open FSharp.Data

    type private Schema1 = JsonProvider<"src/schema/events.json", SampleIsList=true>

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
            Resource =
                event.Resource
                |> Option.map (fun resource ->
                    {
                        Name = resource.Name
                        Href = resource.Href
                    }
                )
            MetaData =
                event.MetaData
                |> Option.map (fun metaData ->
                    RawData metaData.JsonValue
                )
            DomainData =
                event.DomainData
                |> Option.map (fun domainData ->
                    RawData domainData.JsonValue
                )
        }

    let messageReader onEvent =
        {
            ParseEvent = parse
            OnEvent = onEvent
        }
        |> ParsedMessageReader

    let toCommon (event: RawEvent) =
        event
        |> Event.toCommon
