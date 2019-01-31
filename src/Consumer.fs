namespace Kafka

module Consumer =
    open System
    open Confluent.Kafka

    let private createConsumer brokerList (topic: string) =
        let uniqueGroupId = sprintf "consumer-%d" DateTime.Now.Ticks    // unique group id means, it will always starts from the beginning

        let config = ConsumerConfig()
        config.GroupId <- uniqueGroupId
        config.BootstrapServers <- brokerList
        config.AutoOffsetReset <- AutoOffsetResetType.Earliest |> Nullable

        let consumer = new Consumer<Ignore, string>(config)
        consumer.Subscribe topic

        consumer

    let private readMessage = function
        | DecodedMessageReader { ReadMessage = readMessage } -> readMessage
        | ParsedMessageReader { ParseEvent = parse; OnEvent = onEvent } -> parse >> onEvent

    let consumeStream log (configuration: Configuration) incrementMessageCount (reader: MessageReader<_>): unit =
        log "Connecting ..."
        use consumer = createConsumer configuration.BrokerList configuration.Topic

        try
            log "Reading stream ..."
            while true do
                consumer.Consume()
                |> (fun result -> result.Value)
                |> incrementMessageCount
                |> readMessage reader
        finally
            consumer.Close()
