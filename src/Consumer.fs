namespace Kafka

open System.Threading
open FSharp.Control
open Confluent.Kafka
open Metrics.ServiceStatus

type ConfigureConnsumer = ConsumerConfig -> ConsumerConfig

type ConsumerConfiguration = {
    Connection: ConnectionConfiguration
    GroupId: GroupId
    Configure: ConfigureConnsumer option
    Logger: Logger option
    Checker: Checker option
    IntervalChecker: IntervalChecker option
    ServiceStatus: ServiceStatus option
}

[<RequireQualifiedAccess>]
module ConsumerConfiguration =
    let createWithConnection connection groupId =
        {
            Connection = connection
            GroupId = groupId
            Logger = None
            Checker = None
            IntervalChecker = None
            ServiceStatus = None
            Configure = None
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

    type private Consumer = IConsumer<Ignore, string>

    [<Struct>]
    type Message = {
        Offset: int64 option
        Value: string
    }

    [<RequireQualifiedAccess>]
    module Message =
        let value ({ Value = value }) = value

    module internal Consumer =
        let private createDefaultConfig (BrokerList brokerList) groupId configure =
            let config =
                ConsumerConfig(
                    GroupId = (groupId |> GroupId.value),
                    BootstrapServers = brokerList,
                    AutoOffsetReset = (AutoOffsetReset.Earliest |> Nullable)
                )

            match configure with
            | Some configure -> configure config
            | _ -> config

        let private createConsumer topic (config: ConsumerConfig): Consumer =
            let consumer = ConsumerBuilder(config).Build()

            topic
            |> StreamName.value
            |> consumer.Subscribe
            consumer

        let private createConsumerForLastMessage topic (config: ConsumerConfig): Consumer =
            let consumer = ConsumerBuilder(config).Build()
            let topicPartition = TopicPartition(topic |> StreamName.value, Partition(0))

            let lastMessageOffset =
                consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds 5.0)
                |> fun offset ->
                    if offset.High.IsSpecial || offset.High.Value = 0L
                    then failwithf "There is no last message."
                    else offset.High.Value - 1L

            consumer.Assign(TopicPartitionOffset(topicPartition, Offset(lastMessageOffset)))
            consumer

        let internal create brokerList topic groupId configure =
            createDefaultConfig brokerList groupId configure
            |> createConsumer topic

        let internal createForLastMessage brokerList topic configure =
            createDefaultConfig brokerList GroupId.Random configure
            |> createConsumerForLastMessage topic

        let connect log configuration =
            log "Connecting ..."
            create configuration.Connection.BrokerList configuration.Connection.Topic configuration.GroupId configuration.Configure

        let connectLastMessage log configuration =
            log "Connecting for last message ..."
            createForLastMessage configuration.Connection.BrokerList configuration.Connection.Topic configuration.Configure

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

        let private consume (consumer: Consumer) =
            try
                consumer.Consume()
                |> (fun result ->
                    if isNull result then None
                    else Some result
                )
            with
            | :? KafkaException as e ->
                // explicitly print error, because consume is in seq {} and it handles exceptions and just prints a message
                eprintfn "ConsumeError: %A" e
                raise e

        let consumeMessageValue (consumer: Consumer) =
            consumer
            |> consume
            |> Option.map (fun result -> result.Message.Value)

        let consumeMessage (consumer: Consumer) =
            consumer
            |> consume
            |> Option.map (fun result -> {
                Offset = if result.Offset.IsSpecial then None else Some result.Offset.Value
                Value = result.Message.Value
            })

        let private consumeMessageSeq connect consumeMessage log configuration =
            let (markAsEnabled, markAsDisabled) = configuration.ServiceStatus |> ServiceStatus.resolve

            seq {
                use consumer: Consumer = configuration |> connect log

                try
                    markAsEnabled |> MarkAsEnabled.execute
                    logStartReading log configuration.GroupId

                    while true do
                        let message: 'a option = consumer |> consumeMessage
                        if message.IsSome then
                            yield message.Value
                finally
                    markAsDisabled |> MarkAsDisabled.execute
                    Consumer.close log consumer
            }

        type private ConsumeError =
            | BrokerError
            | TopicError

        let private consumeMessageSeqWithChecker connect consumeMessage checker intervalChecker log configuration =
            let maxRetries = checker.MaxRetries
            let defaultWaitForResource = checker.WaitForResourceDefault

            let mutable attempt = 1<Attempt>
            let mutable waitForResource = defaultWaitForResource

            let (markAsEnabled, markAsDisabled) = configuration.ServiceStatus |> ServiceStatus.resolve

            let markAsEnabledAndRestartWaitTime () =
                markAsEnabled |> MarkAsEnabled.execute
                defaultWaitForResource

            seq {
                use consumer: Consumer = configuration |> connect log
                use cancellationTokenSource = new CancellationTokenSource()

                try
                    let mutable consumeError: ConsumeError option = None

                    let asyncStartWithCancellation computation =
                        Async.Start (computation, cancellationTokenSource.Token)

                    while attempt <= maxRetries do
                        match checker.CheckCluster consumer.Handle, checker.CheckTopic configuration.Connection.Topic consumer.Handle with
                        | true, true ->
                            logStartReading log configuration.GroupId
                            waitForResource <- markAsEnabledAndRestartWaitTime ()

                            intervalChecker.CheckClusterInInterval consumer.Handle
                            |> AsyncSeq.iter intervalChecker.ClusterHandler
                            |> asyncStartWithCancellation

                            intervalChecker.CheckTopicInInterval configuration.Connection.Topic consumer.Handle
                            |> AsyncSeq.iter (intervalChecker.TopicHandler configuration.Connection.Topic)
                            |> asyncStartWithCancellation

                            while true do
                                let message: 'a option = consumer |> consumeMessage
                                if message.IsSome then
                                    yield message.Value
                        | isBrokerOk, isTopicOk ->
                            let (currentAttempt, waitFor) = MarkAsDisabled.executeAndWait log attempt maxRetries markAsDisabled waitForResource

                            attempt <- currentAttempt
                            waitForResource <- waitFor

                            let error =
                                match isBrokerOk, isTopicOk with
                                | true, false -> TopicError
                                | _ -> BrokerError
                            consumeError <- Some error

                    if attempt > maxRetries then
                        let createError code problem =
                            let message = sprintf "Max attempts was reached and connection could not be estabilished. Problem is with %s." problem
                            KafkaException(Confluent.Kafka.Error(code, message))

                        consumeError
                        |> Option.map (function
                            | TopicError ->
                                sprintf "%A" configuration.Connection.Topic
                                |> createError ErrorCode.TopicException
                            | BrokerError ->
                                sprintf "%A" configuration.Connection.BrokerList
                                |> createError ErrorCode.BrokerNotAvailable
                        )
                        |> Option.map raise
                        |> ignore
                finally
                    log "Cancel checker tokens ..."
                    cancellationTokenSource.Cancel()

                    markAsDisabled |> MarkAsDisabled.execute
                    consumer |> Consumer.close log
            }

        let seq connect consumeMessage configuration =
            let log = configuration.Logger |> Logger.resolve

            match (configuration.Checker, configuration.IntervalChecker) with
            | Some checker, Some intervalChecker -> consumeMessageSeqWithChecker connect consumeMessage checker intervalChecker log configuration
            | Some checker, None -> consumeMessageSeqWithChecker connect consumeMessage checker IntervalChecker.empty log configuration
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

    let consumeLast (configuration: ConsumerConfiguration) (parse: ParseEvent<'Event>): 'Event option =
        try
            configuration
            |> Consume.seq Consumer.connectLastMessage Consume.consumeMessage
            |> Seq.take 1
            |> Seq.head
            |> Message.value
            |> parse
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
