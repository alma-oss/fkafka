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
    Name: string
    Href: string
}

[<RequireQualifiedAccess>]
module Resource =
    let toDto: Resource -> ResourceDto = fun resource ->
        {
            Name = resource.Name
            Href = resource.Href
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

type EventWithResourceDto<'ResourceDto, 'KeyDataDto, 'MetaDataDto, 'DomainDataDto> = {
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
    Resource: 'ResourceDto
    MetaData: 'MetaDataDto
    KeyData: 'KeyDataDto
    DomainData: 'DomainDataDto
}

type EventWithoutResourceDto<'KeyDataDto, 'MetaDataDto, 'DomainDataDto> = {
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
    MetaData: 'MetaDataDto
    KeyData: 'KeyDataDto
    DomainData: 'DomainDataDto
}

[<RequireQualifiedAccess>]
type EventDto<'ResourceDto, 'KeyDataDto, 'MetaDataDto, 'DomainDataDto> =
    | WithResource of EventWithResourceDto<'ResourceDto, 'KeyDataDto, 'MetaDataDto, 'DomainDataDto>
    | WithoutResource of EventWithoutResourceDto<'KeyDataDto, 'MetaDataDto, 'DomainDataDto>

type SerializeEventWithResource<'KeyData, 'MetaData, 'DomainData, 'ResourceDto, 'KeyDataDto, 'MetaDataDto, 'DomainDataDto, 'Error> =
    (Event<'KeyData, 'MetaData, 'DomainData> -> Result<unit, 'Error>)
        -> (Resource option -> Result<'ResourceDto, 'Error>)
        -> ('MetaData -> Result<'MetaDataDto, 'Error>)
        -> ('KeyData -> Result<'KeyDataDto, 'Error>)
        -> ('DomainData -> Result<'DomainDataDto, 'Error>)
        -> Event<'KeyData, 'MetaData, 'DomainData>
        -> Result<EventWithResourceDto<'ResourceDto, 'KeyDataDto, 'MetaDataDto, 'DomainDataDto>, 'Error>

type SerializeEventWithoutResource<'KeyData, 'MetaData, 'DomainData, 'KeyDataDto, 'MetaDataDto, 'DomainDataDto, 'Error> =
    (Event<'KeyData, 'MetaData, 'DomainData> -> Result<unit, 'Error>)
        -> ('MetaData -> Result<'MetaDataDto, 'Error>)
        -> ('KeyData -> Result<'KeyDataDto, 'Error>)
        -> ('DomainData -> Result<'DomainDataDto, 'Error>)
        -> Event<'KeyData, 'MetaData, 'DomainData>
        -> Result<EventWithoutResourceDto<'KeyDataDto, 'MetaDataDto, 'DomainDataDto>, 'Error>

type SerializeEvent<'KeyData, 'MetaData, 'DomainData, 'ResourceDto, 'KeyDataDto, 'MetaDataDto, 'DomainDataDto, 'Error> =
    (Event<'KeyData, 'MetaData, 'DomainData> -> Result<unit, 'Error>)
        -> (Resource -> Result<'ResourceDto, 'Error>)
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

                let! metaDataDto = event.MetaData |> serializeMetaData
                let! keyDataDto = event.KeyData |> serializeKeyData
                let! domainDataDto = event.DomainData |> serializeDomainData

                match event.Resource with
                | Some resource ->
                    let! resourceDto = resource |> serializeResource

                    return EventDto.WithResource {
                        Schema = event.Schema
                        Id = event.Id |> EventId.value
                        CorrelationId = event.CorrelationId |> CorrelationId.value
                        CausationId = event.CausationId |> CausationId.value
                        Timestamp = event.Timestamp
                        Event = event.Event |> EventName.value
                        Domain = event.Domain |> Domain.value
                        Context = event.Context |> Context.value
                        Purpose = event.Purpose |> Purpose.value
                        Version = event.Version |> Version.value
                        Zone = event.Zone |> Zone.value
                        Bucket = event.Bucket |> Bucket.value
                        MetaData = metaDataDto
                        Resource = resourceDto
                        KeyData = keyDataDto
                        DomainData = domainDataDto
                    }
                | _ ->
                    return EventDto.WithoutResource {
                        Schema = event.Schema
                        Id = event.Id |> EventId.value
                        CorrelationId = event.CorrelationId |> CorrelationId.value
                        CausationId = event.CausationId |> CausationId.value
                        Timestamp = event.Timestamp
                        Event = event.Event |> EventName.value
                        Domain = event.Domain |> Domain.value
                        Context = event.Context |> Context.value
                        Purpose = event.Purpose |> Purpose.value
                        Version = event.Version |> Version.value
                        Zone = event.Zone |> Zone.value
                        Bucket = event.Bucket |> Bucket.value
                        MetaData = metaDataDto
                        KeyData = keyDataDto
                        DomainData = domainDataDto
                    }
            }

    let withResourceToDto: SerializeEventWithResource<'KeyData, 'MetaData, 'DomainData, 'ResourceDto, 'KeyDataDto, 'MetaDataDto, 'DomainDataDto, 'Error> =
        fun assertEventType serializeResource serializeMetaData serializeKeyData serializeDomainData event ->
            result {
                do! assertEventType event

                let! resourceDto = event.Resource |> serializeResource
                let! metaDataDto = event.MetaData |> serializeMetaData
                let! keyDataDto = event.KeyData |> serializeKeyData
                let! domainDataDto = event.DomainData |> serializeDomainData

                return {
                    Schema = event.Schema
                    Id = event.Id |> EventId.value
                    CorrelationId = event.CorrelationId |> CorrelationId.value
                    CausationId = event.CausationId |> CausationId.value
                    Timestamp = event.Timestamp
                    Event = event.Event |> EventName.value
                    Domain = event.Domain |> Domain.value
                    Context = event.Context |> Context.value
                    Purpose = event.Purpose |> Purpose.value
                    Version = event.Version |> Version.value
                    Zone = event.Zone |> Zone.value
                    Bucket = event.Bucket |> Bucket.value
                    MetaData = metaDataDto
                    Resource = resourceDto
                    KeyData = keyDataDto
                    DomainData = domainDataDto
                }
            }

    let withoutResourceToDto: SerializeEventWithoutResource<'KeyData, 'MetaData, 'DomainData, 'KeyDataDto, 'MetaDataDto, 'DomainDataDto, 'Error> =
        fun assertEventType serializeMetaData serializeKeyData serializeDomainData event ->
            result {
                do! assertEventType event

                let! metaDataDto = event.MetaData |> serializeMetaData
                let! keyDataDto = event.KeyData |> serializeKeyData
                let! domainDataDto = event.DomainData |> serializeDomainData

                return {
                    Schema = event.Schema
                    Id = event.Id |> EventId.value
                    CorrelationId = event.CorrelationId |> CorrelationId.value
                    CausationId = event.CausationId |> CausationId.value
                    Timestamp = event.Timestamp
                    Event = event.Event |> EventName.value
                    Domain = event.Domain |> Domain.value
                    Context = event.Context |> Context.value
                    Purpose = event.Purpose |> Purpose.value
                    Version = event.Version |> Version.value
                    Zone = event.Zone |> Zone.value
                    Bucket = event.Bucket |> Bucket.value
                    MetaData = metaDataDto
                    KeyData = keyDataDto
                    DomainData = domainDataDto
                }
            }

[<RequireQualifiedAccess>]
module CommonEvent =
    let schema ({ Schema = schema }: CommonEvent) = schema
    let id ({ Id = id }: CommonEvent) = id
    let correlationId ({ CorrelationId = correlationId }: CommonEvent) = correlationId
    let causationId ({ CausationId = causationId }: CommonEvent) = causationId

    let timestamp ({ Timestamp = timestamp }: CommonEvent) = timestamp
    let eventType ({ Event = event }: CommonEvent) = event

    let box (event: CommonEvent) =
        Box.createFromValues
            event.Domain
            event.Context
            event.Purpose
            event.Version
            event.Zone
            event.Bucket

    let resource ({ Resource = resource }: CommonEvent) = resource

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
