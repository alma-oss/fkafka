namespace Alma.Kafka

open Alma.ServiceIdentification
open Alma.ErrorHandling
open Alma.Metrics.ServiceStatus

//
// Common
//

[<Measure>] type Second
[<Measure>] type Attempt

type BrokerList = BrokerList of string

[<RequireQualifiedAccess>]
module BrokerList =
    let value (BrokerList brokerList) = brokerList

[<CustomEquality; NoComparison>]
type StreamName =
    | StreamName of string
    | Instance of Instance

    member internal this.Value =
        match this with
        | (StreamName streamName) -> streamName
        | Instance instance -> instance |> Instance.concat "-"

    override this.GetHashCode() =
        this.Value |> hash

    override this.Equals (b) =
        match b with
        | :? StreamName as streamB -> this.Value = streamB.Value
        | _ -> false

[<RequireQualifiedAccess>]
module StreamName =
    let value: StreamName -> string =
        fun stream -> stream.Value

[<RequireQualifiedAccess>]
[<CustomEquality; NoComparison>]
type GroupId =
    | Random
    | Id of string

    member internal this.Value =
        match this with
        | Id groupId -> groupId
        | Random -> sprintf "random-%d" System.DateTime.Now.Ticks    // random (unique) group id means, it will always starts from the beginning

    override this.GetHashCode() =
        this.Value |> hash

    override this.Equals (b) =
        match b with
        | :? GroupId as groupId ->
            match this, groupId with
            | Random, Random -> true
            | Id thisId, Id bValue -> thisId = bValue
            | _ -> false
        | _ -> false

[<RequireQualifiedAccess>]
module GroupId =
    let map f = function
        | GroupId.Id groupId -> groupId |> f |> GroupId.Id
        | GroupId.Random -> GroupId.Random

    let value: GroupId -> string =
        fun groupId -> groupId.Value

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
// Checkpoint
//

type TopicPartition = {
    Topic: StreamName
    Partition: int
}

type TopicPartitionOffset = {
    TopicPartition: TopicPartition
    Offset: Offset option
}

and Offset = Offset of int64

[<RequireQualifiedAccess>]
module internal Offset =
    let toKafka (Offset offset) = Confluent.Kafka.Offset offset

[<RequireQualifiedAccess>]
module internal TopicPartition =
    let ofKafka (tp: Confluent.Kafka.TopicPartition) =
        if tp.Partition.IsSpecial then None
        else
            Some {
                Topic = tp.Topic |> StreamName
                Partition = tp.Partition.Value
            }

[<RequireQualifiedAccess>]
module internal TopicPartitionOffset =
    let toKafka (tp: TopicPartitionOffset) =
        Confluent.Kafka.TopicPartitionOffset(
            tp.TopicPartition.Topic |> StreamName.value,
            tp.TopicPartition.Partition,
            tp.Offset |> Option.map Offset.toKafka |> Option.defaultValue Confluent.Kafka.Offset.Unset
        )

type GetCheckpoint = GroupId -> TopicPartition -> AsyncResult<TopicPartitionOffset, exn>

//
// Utilities
//

[<AutoOpen>]
module internal Utils =
    let tee f a =
        f a
        a
