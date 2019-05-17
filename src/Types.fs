namespace Kafka
open Metrics.ServiceStatus

//
// Common
//

[<Measure>] type second
[<Measure>] type attempt

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

//
// Service Status
//

module internal MarkAsDisabled =
    open System

    let executeAndWait log (attempt: int<attempt>) (maxRetries: int<attempt>) markAsDisabled (waitFor: int<second>) =
        markAsDisabled |> MarkAsDisabled.execute
        let waitForSeconds = int waitFor

        log <| sprintf "[Attempt: %i/%i] Waiting for resource %s" attempt maxRetries (String.replicate waitForSeconds ".")
        Threading.Thread.Sleep(TimeSpan.FromSeconds (float waitForSeconds))

        let nextTimeWaitFor = Math.Min(waitForSeconds * 2, 30) |> LanguagePrimitives.Int32WithMeasure<second>
        let currentAttempt = attempt + 1<attempt>

        (currentAttempt, nextTimeWaitFor)

module internal ServiceStatus =
    let resolve = function
        | Some { MarkAsEnabled = markAsEnabled; MarkAsDisabled = markAsDisabled } -> (markAsEnabled, markAsDisabled)
        | _ -> (MarkAsEnabled ignore, MarkAsDisabled ignore)

    let resolveMarkAsDisabled = function
        | Some markAsDisabled -> markAsDisabled
        | _ -> MarkAsDisabled ignore

//
// Logger
//

type Logger = {
    Log: string -> unit
}

module internal Logger =
    let resolve = function
        | Some { Log = log } -> log
        | _ -> ignore

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

[<AutoOpenAttribute>]
module internal GenericHelpers =
    let tee f a =
        f a
        a
