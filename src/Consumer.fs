namespace Kafka

module Consumer =
    open System
    open Confluent.Kafka

    type private Consumer = Consumer<Ignore, string>

    let internal createConsumer brokerList (topic: string) groupId: Consumer =
        let groupId =
            match groupId with
            | Some groupId -> groupId
            | _ -> sprintf "consumer-%d" DateTime.Now.Ticks    // unique group id means, it will always starts from the beginning

        let config = ConsumerConfig()
        config.GroupId <- groupId
        config.BootstrapServers <- brokerList
        config.AutoOffsetReset <- AutoOffsetResetType.Earliest |> Nullable

        let consumer = new Consumer<Ignore, string>(config)
        consumer.Subscribe topic

        consumer

    let private closeConsumer log (consumer: Consumer) =
        log "consumer closing ..."
        consumer.Close()

    let private readMessage = function
        | DecodedMessageReader { ReadMessage = readMessage } -> readMessage
        | ParsedMessageReader { ParseEvent = parse; OnEvent = onEvent } -> parse >> onEvent

    let private logStartReading log groupId =
        let stringOptionToString = function
            | Some string -> string
            | _ -> ""

        groupId
        |> Option.map (sprintf " with %s")
        |> stringOptionToString
        |> sprintf "Reading stream%s ..."
        |> log

    let private consume log configuration reader groupId =
        log "Connecting ..."
        use consumer = createConsumer configuration.BrokerList configuration.Topic groupId

        Console.CancelKeyPress.Add <| fun _args ->
            log "\ncanceled ..."
            closeConsumer log consumer

        try
            logStartReading log groupId
            while true do
                consumer.Consume()
                |> (fun result -> result.Value)
                |> readMessage reader
        finally
            closeConsumer log consumer

    let consumeStream log (configuration: Configuration) (reader: MessageReader<_>): unit =
        None
        |> consume log configuration reader

    let consumeStreamWithGroupId log (configuration: Configuration) groupId (reader: MessageReader<_>): unit =
        Some groupId
        |> consume log configuration reader

    let private tee f a =
        f a
        a

    let consumeStreamToOffset (configuration: Configuration) maxOffset (reader: MessageReader<_>) =
        use consumer = createConsumer configuration.BrokerList configuration.Topic None

        let rec consumeToOffset currentOffset =
            if currentOffset < (maxOffset - int64 1) then
                consumer.Consume()
                |> tee (fun result -> result.Value |> readMessage reader)
                |> fun result -> result.Offset.Value
                |> consumeToOffset

        try
            consumeToOffset (int64 0)
        finally
            closeConsumer ignore consumer
