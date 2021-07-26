namespace Lmc.Kafka

[<RequireQualifiedAccess>]
module internal Trace =
    open OpenTracing
    open OpenTracing.Propagation
    open OpenTracing.Tag

    open Lmc.Tracing

    [<AutoOpen>]
    module private Headers =
        open System.Collections.Generic

        type Headers = Dictionary<string, string>
        type IHeaders = IDictionary<string, string>

        type HeaderSeq = (string * string) seq

        let headersToDictionary (headerList: HeaderSeq): IHeaders =
            headerList
            |> Seq.fold
                (fun (headers: Headers) (key, value) ->
                    headers.Add(key, value)
                    headers
                )
                (Headers())
            :> IHeaders

    let private kafkaHeadersToDictionary headers =
        headers
        |> List.map (fun header -> header |> Header.key |> HeaderKey.value, header |> Header.valueAsString)
        |> headersToDictionary

    let extractFromHeaders (headers: Header list) =
        let httpHeadersCarrier = TextMapExtractAdapter(headers |> kafkaHeadersToDictionary) :> ITextMap

        match Tracer.tracer().Extract(BuiltinFormats.HttpHeaders, httpHeadersCarrier) with
        | null -> Inactive
        | context -> Context context

    let extractFromKafkaHeaders (headers: Confluent.Kafka.Headers) () =
        match headers with
        | null -> Trace.Inactive
        | headers ->
            headers
            |> Seq.map Header.fromKafkaHeader
            |> List.ofSeq
            |> extractFromHeaders

    let inject trace (headers: Header list) =
        match trace |> Trace.context with
        | Some context ->
            let headersDict = headers |> kafkaHeadersToDictionary
            let kafkaHeadersCarrier = TextMapInjectAdapter(headersDict) :> ITextMap

            Tracer.tracer().Inject(context, BuiltinFormats.HttpHeaders, kafkaHeadersCarrier)

            headersDict
            |> Seq.map (fun kv -> kv.Value |> Header.ofString (HeaderKey kv.Key))
            |> Seq.toList

        | _ -> headers
