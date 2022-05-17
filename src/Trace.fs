namespace Lmc.Kafka

[<RequireQualifiedAccess>]
module internal Trace =
    open Lmc.Tracing
    open Lmc.Tracing.Extension

    let private kafkaHeadersToList headers =
        headers
        |> List.map (fun header -> header |> Header.key |> HeaderKey.value, header |> Header.valueAsString)

    let extractFromHeaders (headers: Header list) =
        headers
        |> kafkaHeadersToList
        |> Http.extractFromHeaders

    let extractFromKafkaHeaders (headers: Confluent.Kafka.Headers) () =
        match headers with
        | null -> None
        | headers ->
            headers
            |> Seq.map Header.fromKafkaHeader
            |> List.ofSeq
            |> extractFromHeaders

    let inject trace (headers: Header list) =
        match trace with
        | Inactive -> headers
        | _ ->
            headers
            |> kafkaHeadersToList
            |> Http.inject trace
            |> List.map (fun (key, value) -> value |> Header.ofString (HeaderKey key))
