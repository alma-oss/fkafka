namespace Kafka

module Consumer =
    open System
    open Confluent.Kafka

    type private Consumer = Consumer<Ignore, string>

    let internal createConsumer (BrokerList brokerList) (StreamName topic) groupId: Consumer =
        let config = ConsumerConfig()
        config.GroupId <- groupId |> GroupId.value
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
        let groupIdToLog = function
            | Id groupId -> groupId
            | Random -> ""

        groupId
        |> GroupId.map (sprintf " with %s")
        |> groupIdToLog
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
        GroupId.Random
        |> consume log configuration reader

    let consumeStreamWithGroupId log (configuration: Configuration) groupId (reader: MessageReader<_>): unit =
        GroupId.Id groupId
        |> consume log configuration reader

    let private tee f a =
        f a
        a

    let consumeStreamToOffset (configuration: Configuration) maxOffset (reader: MessageReader<_>) =
        use consumer = createConsumer configuration.BrokerList configuration.Topic GroupId.Random

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
