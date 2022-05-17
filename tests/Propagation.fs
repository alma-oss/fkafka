module Propagation

open Expecto
open Lmc.Tracing
open Lmc.Kafka

[<Tests>]
let checkTracePropagation =
    testList "Kafka - trace propagation" [
        testCase "should inject trace to headers" <| fun _ ->
            let span = Trace.Span.start "span"
            let headers = Trace.inject span []

            Expect.isNonEmpty headers "Injected headers should not be empty"
            Expect.hasLength headers 3 "There should be 3 injected headers"

            headers
            |> List.iter (fun header -> Expect.stringStarts (header.Key |> HeaderKey.value) "X-B3-" "Injected header should start with X-B3-")

            let map = headers |> List.map (fun header -> (header.Key |> HeaderKey.value), (header |> Header.valueAsString)) |> Map.ofList
            Expect.equal (span |> Trace.traceId) (map |> Map.tryFind "X-B3-TraceId") "Headers should have traceId header."
            Expect.equal (span |> Trace.spanId) (map |> Map.tryFind "X-B3-SpanId") "Headers should have spanId header."

        testCase "should inject child trace to headers" <| fun _ ->
            let span = "main" |> Trace.Span.start
            let child = "child" |> Trace.ChildOf.start span

            let headers = Trace.inject child []

            Expect.isNonEmpty headers "Injected headers should not be empty"
            Expect.hasLength headers 4 "There should be 4 injected headers"

            headers
            |> List.iter (fun header -> Expect.stringStarts (header.Key |> HeaderKey.value) "X-B3-" "Injected header should start with X-B3-")

            let map = headers |> List.map (fun header -> (header.Key |> HeaderKey.value), (header |> Header.valueAsString)) |> Map.ofList
            Expect.equal (child |> Trace.traceId) (map |> Map.tryFind "X-B3-TraceId") "Headers should have traceId header."
            Expect.equal (child |> Trace.spanId) (map |> Map.tryFind "X-B3-SpanId") "Headers should have spanId header."
            Expect.equal (child |> Trace.parentId) (map |> Map.tryFind "X-B3-ParentSpanId") "Headers should have parentSpanId header."

        testCase "should inject trace to headers with old trace information" <| fun _ ->
            let old = "old" |> Trace.Span.start
            let headers = Trace.inject old []

            let span = "main" |> Trace.Span.start
            let child = "child" |> Trace.ChildOf.start span

            let headers = Trace.inject child headers

            Expect.isNonEmpty headers "Injected headers should not be empty"
            Expect.hasLength headers 4 "There should be 4 injected headers"

            headers
            |> List.iter (fun header -> Expect.stringStarts (header.Key |> HeaderKey.value) "X-B3-" "Injected header should start with X-B3-")

            let map = headers |> List.map (fun header -> (header.Key |> HeaderKey.value), (header |> Header.valueAsString)) |> Map.ofList
            Expect.equal (child |> Trace.traceId) (map |> Map.tryFind "X-B3-TraceId") "Headers should have traceId header."
            Expect.equal (child |> Trace.spanId) (map |> Map.tryFind "X-B3-SpanId") "Headers should have spanId header."
            Expect.equal (child |> Trace.parentId) (map |> Map.tryFind "X-B3-ParentSpanId") "Headers should have parentSpanId header."

        testCase "should extract inactive trace from empty headers" <| fun _ ->
            let span = Inactive
            Expect.isNone (span |> Trace.context) "Inactive span should not have any context"

            let headers = Trace.inject span []

            let extracted = Trace.extractFromHeaders headers

            Expect.equal (span |> Trace.context) extracted (sprintf "inject inactive trace (%s) to headers and extract it again to (%s)" (string span) (string extracted))

            let childOfExtracted =
                "continue"
                |> Trace.ChildOf.continueOrStart (fun () -> extracted |> Trace.ofContextOption)

            Expect.isSome (childOfExtracted |> Trace.spanId) "Extracted span should have span id"
            Expect.isNone (childOfExtracted |> Trace.parentId) "Extracted span should not have parent span id"
            Expect.equal (childOfExtracted |> Trace.parentId) (span |> Trace.spanId) (sprintf "Parent of extracted trace (%s) should original span (%s)" (string childOfExtracted) (string span))

        testCase "should extract injected trace from headers" <| fun _ ->
            let span = Trace.Span.start "span"
            let headers = Trace.inject span []

            let extracted = Trace.extractFromHeaders headers

            Expect.equal (span |> Trace.context) extracted (sprintf "inject trace (%s) to headers and extract it again to (%s)" (string span) (string extracted))

            let childOfExtracted =
                "continue"
                |> Trace.ChildOf.continueOrStart (fun () -> extracted |> Trace.ofContextOption)

            Expect.equal (childOfExtracted |> Trace.parentId) (span |> Trace.spanId) (sprintf "Parent of extracted trace (%s) should original span (%s)" (string childOfExtracted) (string span))

        testCase "should extract injected child trace from headers" <| fun _ ->
            let span = "main" |> Trace.Span.start
            let child = "child" |> Trace.ChildOf.start span
            let headers = Trace.inject child []

            let extracted = Trace.extractFromHeaders headers

            Expect.equal (child |> Trace.context) extracted (sprintf "inject trace (%s) to headers and extract it again to (%s)" (string child) (string extracted))

            let childOfExtracted =
                "continue"
                |> Trace.ChildOf.continueOrStart (fun () -> extracted |> Trace.ofContextOption)

            Expect.equal (childOfExtracted |> Trace.parentId) (child |> Trace.spanId) (sprintf "Parent of extracted trace (%s) should original child span (%s)" (string childOfExtracted) (string child))
    ]
