namespace Lmc.Kafka

open Lmc.ServiceIdentification
open Lmc.Metrics.ServiceStatus

//
// Common
//

[<Measure>] type Second
[<Measure>] type Attempt

type BrokerList = BrokerList of string

[<RequireQualifiedAccess>]
module BrokerList =
    let value (BrokerList brokerList) = brokerList

type StreamName =
    | StreamName of string
    | Instance of Instance

[<RequireQualifiedAccess>]
module StreamName =
    let value = function
        | (StreamName streamName) -> streamName
        | Instance instance -> instance |> Instance.concat "-"

type GroupId =
    | Random
    | Id of string

[<RequireQualifiedAccess>]
module GroupId =
    let map f = function
        | Id groupId -> groupId |> f |> Id
        | Random -> Random

    let value = function
        | Id groupId -> groupId
        | Random -> sprintf "random-%d" System.DateTime.Now.Ticks    // random (unique) group id means, it will always starts from the beginning

//
// Headers
//

type HeaderKey = HeaderKey of string

[<RequireQualifiedAccess>]
module HeaderKey =
    let value (HeaderKey key) = key

type Header = {
    Key: HeaderKey
    Value: byte array
}

[<RequireQualifiedAccess>]
module Header =
    open System.Text

    let key { Key = key } = key

    let value { Value = value } = value
    let valueAsString = value >> Encoding.ASCII.GetString

    let ofString key (value: string) =
        {
            Key = key
            Value = value |> Encoding.ASCII.GetBytes
        }

    let internal toKafkaHeader (header: Header) =
        Confluent.Kafka.Header(
            header.Key |> HeaderKey.value,
            header.Value
        )

    let internal fromKafkaHeader (header: Confluent.Kafka.IHeader): Header =
        {
            Key = HeaderKey header.Key
            Value = header.GetValueBytes()
        }

//
// Service Status
//

[<RequireQualifiedAccess>]
module internal MarkAsDisabled =
    open System

    let executeAndWait log (attempt: int<Attempt>) (maxRetries: int<Attempt>) markAsDisabled (waitFor: int<Second>) =
        markAsDisabled |> MarkAsDisabled.execute
        let waitForSeconds = int waitFor

        log <| sprintf "[Attempt: %i/%i] Waiting for resource %s" attempt maxRetries (String.replicate waitForSeconds ".")
        Threading.Thread.Sleep(TimeSpan.FromSeconds (float waitForSeconds))

        let nextTimeWaitFor = Math.Min(waitForSeconds * 2, 30) |> LanguagePrimitives.Int32WithMeasure<Second>
        let currentAttempt = attempt + 1<Attempt>

        (currentAttempt, nextTimeWaitFor)

[<RequireQualifiedAccess>]
module internal ServiceStatus =
    let resolve = function
        | Some { MarkAsEnabled = markAsEnabled; MarkAsDisabled = markAsDisabled } -> (markAsEnabled, markAsDisabled)
        | _ -> (MarkAsEnabled ignore, MarkAsDisabled ignore)

    let resolveMarkAsDisabled = function
        | Some markAsDisabled -> markAsDisabled
        | _ -> MarkAsDisabled ignore

//
// Connection
//

type ConnectionConfiguration = {
    BrokerList: BrokerList
    Topic: StreamName
}

//
// Utilities
//

[<AutoOpen>]
module internal Utils =
    let tee f a =
        f a
        a
