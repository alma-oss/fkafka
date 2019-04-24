namespace Kafka

type ConsumerConfiguration = {
    Connection: ConnectionConfiguration
    GroupId: GroupId
    Logger: Logger option
    Checker: Checker option
    ServiceStatus: ServiceStatus option
}

module ConsumerConfiguration =
    let createWithConnection connection groupId =
        {
            Connection = connection
            GroupId = groupId
            Logger = None
            Checker = None
            ServiceStatus = None
        }

    let createWithDefaults brokerList topic =
        createWithConnection {
            BrokerList = brokerList
            Topic = topic
        }

//
// Kafka parser
//

type ParseEvent<'Event> = string -> 'Event

//
// Kafka readers
//

type DecodedMessageReader = {
    ReadMessage: string -> unit
}

type ParsedMessageReader<'Event> = {
    ParseEvent: ParseEvent<'Event>
    OnEvent: 'Event -> unit
}

type MessageReader<'Event> =
    | DecodedMessageReader of DecodedMessageReader
    | ParsedMessageReader of ParsedMessageReader<'Event>

//
// Consumer
//

module Consumer =
    open System
    open Confluent.Kafka

    [<AutoOpenAttribute>]
    module GenericHelpers =
        let tee f a =
            f a
            a

        let doWith service action =
            service
            |> Option.map action
            |> ignore

    type private Consumer = IConsumer<Ignore, string>

    [<Struct>]
    type Message = {
        Offset: int64 option
        Value: string
    }

    module Message =
        let value ({ Value = value }) = value

    module internal Consumer =
        let private createDefaultConfig (BrokerList brokerList) groupId =
            let config = ConsumerConfig()
            config.GroupId <- groupId |> GroupId.value
            config.BootstrapServers <- brokerList
            config.AutoOffsetReset <- AutoOffsetReset.Earliest |> Nullable

            config

        let private createConsumer (StreamName topic) (config: ConsumerConfig): Consumer =
            let consumer = ConsumerBuilder(config).Build()

            consumer.Subscribe topic
            consumer

        let private createConsumerForLastMessage (StreamName topic) (config: ConsumerConfig): Consumer =
            let consumer = ConsumerBuilder(config).Build()
            let topicPartition = TopicPartition(topic, Partition(0))

            let lastMessageOffset =
                consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds 5.0)
                |> fun offset ->
                    if offset.High.IsSpecial || offset.High.Value = 0L
                    then failwithf "There is no last message."
                    else offset.High.Value - 1L

            consumer.Assign(TopicPartitionOffset(topicPartition, Offset(lastMessageOffset)))
            consumer

        let internal create brokerList topic groupId =
            createDefaultConfig brokerList groupId
            |> createConsumer topic

        let internal createForLastMessage brokerList topic =
            createDefaultConfig brokerList GroupId.Random
            |> createConsumerForLastMessage topic

        let connect log configuration =
            log "Connecting ..."
            create configuration.Connection.BrokerList configuration.Connection.Topic configuration.GroupId

        let connectLastMessage log configuration =
            log "Connecting for last message ..."
            createForLastMessage configuration.Connection.BrokerList configuration.Connection.Topic

        let close log (consumer: Consumer) =
            log "Consumer closing ..."
            consumer.Close()

    module private Consume =
        let private logStartReading log groupId =
            let groupIdToLog = function
                | Id groupId -> groupId
                | Random -> ""

            groupId
            |> GroupId.map (sprintf " with %s")
            |> groupIdToLog
            |> sprintf "Reading stream%s ..."
            |> log

        let private serviceStatus configuration =
            match configuration.ServiceStatus with
            | Some { MarkAsEnabled = markAsEnabled; MarkAsDisabled = markAsDisabled } -> (markAsEnabled, markAsDisabled)
            | _ -> (ignore, ignore)

        let private consume (consumer: Consumer) =
            try
                consumer.Consume()
                |> (fun result ->
                    if isNull result then None
                    else Some result
                )
            with
            | :? KafkaException as e ->
                // exlicitly print error, because consume is in seq {} and it handles exceptions and just prints a message
                eprintfn "ConsumeError: %A" e
                raise e

        let consumeMessageValue (consumer: Consumer) =
            consumer
            |> consume
            |> Option.map (fun result -> result.Value)

        let consumeMessage (consumer: Consumer) =
            consumer
            |> consume
            |> Option.map (fun result -> {
                Offset = if result.Offset.IsSpecial then None else Some result.Offset.Value
                Value = result.Value
            })

        let private consumeMessageSeq connect consumeMessage log configuration =
            let (markAsEnabled, markAsDisabled) = configuration |> serviceStatus

            seq {
                use consumer: Consumer = configuration |> connect log

                try
                    markAsEnabled()
                    logStartReading log configuration.GroupId

                    while true do
                        let message: 'a option = consumer |> consumeMessage
                        if message.IsSome then
                            yield message.Value
                finally
                    markAsDisabled()
                    Consumer.close log consumer
            }

        let private consumeMessageSeqWithChecker connect consumeMessage checker log configuration =
            let maxRetries = checker.MaxRetries
            let defaultWaitForResource = checker.WaitForResourceDefault

            let mutable attempt = 1
            let mutable waitForResource = defaultWaitForResource

            let (markAsEnabled, markAsDisabled) = configuration |> serviceStatus

            let markAsEnabledAndRestartWaitTime () =
                markAsEnabled()
                defaultWaitForResource

            let markAsDisableAndWaitForResources waitForResource =
                markAsDisabled()
                let waitForResourceSeconds = int waitForResource

                log (sprintf "[Attempt: %i/%i] Waiting for resource %s" attempt maxRetries (String.replicate waitForResourceSeconds "."))

                attempt <- attempt + 1

                System.Threading.Thread.Sleep(TimeSpan.FromSeconds (float waitForResourceSeconds))
                Math.Min(waitForResourceSeconds * 2, 30) |> LanguagePrimitives.Int32WithMeasure<second>

            seq {
                use consumer: Consumer = configuration |> connect log

                try
                    while attempt <= maxRetries do
                        match checker.CheckCluster consumer.Handle, checker.CheckTopic configuration.Connection.Topic consumer.Handle with
                        | true, true ->
                            logStartReading log configuration.GroupId
                            waitForResource <- markAsEnabledAndRestartWaitTime ()

                            while true do
                                let message: 'a option = consumer |> consumeMessage
                                if message.IsSome then
                                    yield message.Value
                        | _ ->
                            waitForResource <- markAsDisableAndWaitForResources waitForResource
                finally
                    markAsDisabled()
                    consumer |> Consumer.close log
            }

        let seq connect consumeMessage configuration =
            let log message =
                (fun { Log = log } -> log message)
                |> doWith configuration.Logger

            match configuration.Checker with
            | Some checker -> consumeMessageSeqWithChecker connect consumeMessage checker log configuration
            | _ -> consumeMessageSeq connect consumeMessage log configuration

    let private readMessage = function
        | DecodedMessageReader { ReadMessage = readMessage } -> readMessage
        | ParsedMessageReader { ParseEvent = parse; OnEvent = onEvent } -> parse >> onEvent

    //
    // Public api
    //

    let consume (configuration: ConsumerConfiguration) (parse: ParseEvent<'Event>): 'Event seq =
        configuration
        |> Consume.seq Consumer.connect Consume.consumeMessageValue
        |> Seq.map parse

    let consumeMessages (configuration: ConsumerConfiguration): Message seq =
        configuration
        |> Consume.seq Consumer.connect Consume.consumeMessage

    let consumeLastMessage (configuration: ConsumerConfiguration): Message option =
        try
            configuration
            |> Consume.seq Consumer.connectLastMessage Consume.consumeMessage
            |> Seq.take 1
            |> Seq.head
            |> Some
        with
        | _ -> None

    let read (configuration: ConsumerConfiguration) (reader: MessageReader<'Event>): unit =
        configuration
        |> Consume.seq Consumer.connect Consume.consumeMessageValue
        |> Seq.iter (readMessage reader)

    let readToOffset (configuration: ConsumerConfiguration) maxOffset (reader: MessageReader<'Event>) =
        configuration
        |> consumeMessages
        |> Seq.takeWhile (fun message ->
            match message.Offset with
            | Some currentOffset -> currentOffset < (maxOffset - int64 1)
            | _ -> true
        )
        |> Seq.iter (Message.value >> readMessage reader)
