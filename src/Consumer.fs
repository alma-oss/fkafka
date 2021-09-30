namespace Lmc.Kafka

open System.Threading
open FSharp.Control
open Confluent.Kafka

open Lmc.Metrics.ServiceStatus
open Lmc.Tracing

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
    /// Partition used for consuming - since we don't use them yet, it is always a default one - 0
    let [<Literal>] internal DefaultPartition = 0

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

[<RequireQualifiedAccess>]
type ConsumeError =
    | KafkaException of KafkaException
    | RuntimeException of exn
    | BrokerError
    | TopicError
    | MaxRetriesReached of KafkaException

//
// Consumer
//

[<RequireQualifiedAccess>]
module Consumer =
    open System

    type internal KafkaConsumer = KafkaConsumer of IConsumer<Ignore, string>
    type internal KafkaMessage = KafkaMessage of ConsumeResult<Ignore, string>

    [<RequireQualifiedAccess>]
    module private KafkaMessage =
        let value (KafkaMessage message) = message

    [<RequireQualifiedAccess>]
    module private KafkaConsumer =
        let value (KafkaConsumer kafkaConsumer) = kafkaConsumer

    [<Struct>]
    type internal ConsumeRuntime = {
        /// Acutal list of bootstrap servers used for consuming
        BootstrapServers: string
        /// Acutal group id used for consuming
        GroupId: string
        /// Acutal topic used for consuming
        Topic: string
        /// Acutal partition used for consuming
        Partition: int

        UseTracing: bool
    }

    [<Struct>]
    type TracedMessage<'Message> = {
        Message: 'Message
        Trace: Trace
    }

    [<RequireQualifiedAccess>]
    module TracedMessage =
        let message ({ Message = message }: TracedMessage<'Message>) = message
        let trace ({ Trace = trace }: TracedMessage<'Message>) = trace
        let finish message = message |> tee (trace >> Trace.finish)

        let internal map f message =
            { Message = message.Message |> KafkaMessage.value |> f; Trace = message.Trace }

    type private Consumer =
        {
            /// Actual kafka consumer used for consuming events
            KafkaConsumer: KafkaConsumer
            /// Actual Runtime information about consume
            Runtime: ConsumeRuntime
        }

        member this.Close() =
            (this.KafkaConsumer |> KafkaConsumer.value).Close()

        interface IDisposable with
            member this.Dispose() =
                this.Close()

    [<Struct>]
    type Message = {
        Offset: int64 option
        Value: string
    }

    [<RequireQualifiedAccess>]
    module Message =
        let value ({ Value = value }: Message) = value
        let offset ({ Offset = offset }: Message) = offset

    module private Consumer =
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
            let topicValue = topic |> StreamName.value

            let consumer = ConsumerBuilder(config).Build()
            topicValue |> consumer.Subscribe

            {
                KafkaConsumer = KafkaConsumer consumer
                Runtime = {
                    BootstrapServers = config.BootstrapServers
                    GroupId = config.GroupId
                    Topic = topicValue
                    Partition = ConsumerConfiguration.DefaultPartition
                    UseTracing = Trace.Check.isTracerAvailable()
                }
            }

        let private createConsumerForLastMessage topic (config: ConsumerConfig): Consumer =
            let topicValue = topic |> StreamName.value

            let consumer = ConsumerBuilder(config).Build()
            let topicPartition = TopicPartition(topicValue, Partition(ConsumerConfiguration.DefaultPartition))

            let lastMessageOffset =
                consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds 5.0)
                |> fun offset ->
                    if offset.High.IsSpecial || offset.High.Value = 0L
                    then failwithf "There is no last message."
                    else offset.High.Value - 1L

            consumer.Assign(TopicPartitionOffset(topicPartition, Offset(lastMessageOffset)))

            {
                KafkaConsumer = KafkaConsumer consumer
                Runtime = {
                    BootstrapServers = config.BootstrapServers
                    GroupId = config.GroupId
                    Topic = topicValue
                    Partition = ConsumerConfiguration.DefaultPartition
                    UseTracing = Trace.Check.isTracerAvailable()
                }
            }

        let private create brokerList topic groupId configure =
            createDefaultConfig brokerList groupId configure
            |> createConsumer topic

        let private createForLastMessage brokerList topic configure =
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
        type ConsumeMessage<'Message> = Consumer -> Result<TracedMessage<'Message>, ConsumeError> option

        let private logStartReading log groupId =
            let groupIdToLog = function
                | Id groupId -> groupId
                | Random -> ""

            groupId
            |> GroupId.map (sprintf " with %s")
            |> groupIdToLog
            |> sprintf "Reading stream%s ..."
            |> log

        let private consume: ConsumeMessage<KafkaMessage> = fun consumer ->
            try
                let consumeResult = (consumer.KafkaConsumer |> KafkaConsumer.value).Consume()

                if isNull consumeResult then None
                else
                    let trace =
                        if consumer.Runtime.UseTracing then
                            "Consume event"
                            |> Trace.FollowFrom.continueOrStart (Trace.extractFromKafkaHeaders consumeResult.Message.Headers >> Trace.ofContextOption)
                            |> Trace.addTags [
                                "peer.service", "kafka"
                                "peer.address", consumer.Runtime.BootstrapServers
                                "component:", (sprintf "fkafka (%s)" AssemblyVersionInformation.AssemblyVersion)
                                "kafka.topic", consumer.Runtime.Topic
                                "message_bus.destination", consumer.Runtime.Topic
                                "kafka.partition", string consumer.Runtime.Partition
                                "kafka.group_id", consumer.Runtime.GroupId
                                "span.kind", "consumer"
                            ]
                        else Inactive

                    Some (Ok {
                        Message = KafkaMessage consumeResult
                        Trace = trace
                    })
            with
            | :? KafkaException as e -> Result.Error (ConsumeError.KafkaException e) |> Some
            | e -> Result.Error (ConsumeError.RuntimeException e) |> Some

        /// Helper function to map TracedMessage on Message result inside an Option
        let private map f = Option.map (Result.map (TracedMessage.map f))

        let consumeMessageValue: ConsumeMessage<string> = fun consumer ->
            consumer
            |> consume
            |> map (fun message -> message.Message.Value)

        let consumeMessage: ConsumeMessage<Message> = fun consumer ->
            consumer
            |> consume
            |> map (fun message -> {
                Offset = if message.Offset.IsSpecial then None else Some message.Offset.Value
                Value = message.Message.Value
            })

        let private consumeMessageSeq connect (consumeMessage: ConsumeMessage<'Message>) log configuration =
            let (markAsEnabled, markAsDisabled) = configuration.ServiceStatus |> ServiceStatus.resolve

            seq {
                use consumer: Consumer = configuration |> connect log

                try
                    markAsEnabled |> MarkAsEnabled.execute
                    logStartReading log configuration.GroupId

                    while true do
                        let message = consumer |> consumeMessage
                        if message.IsSome then
                            yield message.Value

                finally
                    markAsDisabled |> MarkAsDisabled.execute
                    // todo - tohle by tady idealne nebylo, log muze byt primo v close, protoze se predava do connectu, tak muze byt v RuntimeParts
                    Consumer.close log consumer
            }

        let private consumeMessageSeqWithChecker connect (consumeMessage: ConsumeMessage<'Message>) checker intervalChecker log configuration =
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

                let (KafkaConsumer kafkaConsumer) = consumer.KafkaConsumer

                try
                    let mutable consumeError: ConsumeError option = None

                    let asyncStartWithCancellation computation =
                        Async.Start (computation, cancellationTokenSource.Token)

                    while attempt <= maxRetries do
                        match checker.CheckCluster kafkaConsumer.Handle, checker.CheckTopic configuration.Connection.Topic kafkaConsumer.Handle with
                        | true, true ->
                            logStartReading log configuration.GroupId
                            waitForResource <- markAsEnabledAndRestartWaitTime ()

                            intervalChecker.CheckClusterInInterval kafkaConsumer.Handle
                            |> AsyncSeq.iter intervalChecker.ClusterHandler
                            |> asyncStartWithCancellation

                            intervalChecker.CheckTopicInInterval configuration.Connection.Topic kafkaConsumer.Handle
                            |> AsyncSeq.iter (intervalChecker.TopicHandler configuration.Connection.Topic)
                            |> asyncStartWithCancellation

                            while true do
                                let message = consumer |> consumeMessage
                                if message.IsSome then
                                    yield message.Value

                        | isBrokerOk, isTopicOk ->
                            let (currentAttempt, waitFor) = MarkAsDisabled.executeAndWait log attempt maxRetries markAsDisabled waitForResource

                            attempt <- currentAttempt
                            waitForResource <- waitFor

                            let error =
                                match isBrokerOk, isTopicOk with
                                | true, false -> ConsumeError.TopicError
                                | _ -> ConsumeError.BrokerError
                            consumeError <- Some error

                    if attempt > maxRetries then
                        let createError code problem =
                            let message = sprintf "Max attempts was reached and connection could not be estabilished. Problem is with %s." problem
                            ConsumeError.MaxRetriesReached <| KafkaException(Confluent.Kafka.Error(code, message))

                        let consumeError =
                            consumeError
                            |> Option.map (function
                                | ConsumeError.TopicError ->
                                    sprintf "%A" configuration.Connection.Topic
                                    |> createError ErrorCode.TopicException
                                | ConsumeError.BrokerError ->
                                    sprintf "%A" configuration.Connection.BrokerList
                                    |> createError ErrorCode.BrokerNotAvailable
                                | consumeError -> consumeError
                            )

                        if consumeError.IsSome then
                            yield Result.Error consumeError.Value
                finally
                    log "Cancel checker tokens ..."
                    cancellationTokenSource.Cancel()

                    markAsDisabled |> MarkAsDisabled.execute
                    // todo - tady je taky close - viz jiny todo
                    consumer |> Consumer.close log
            }

        let seq connect (consumeMessage: ConsumeMessage<'Message>) configuration =
            let log = configuration.Logger |> Logger.resolve

            match (configuration.Checker, configuration.IntervalChecker) with
            | Some checker, Some intervalChecker -> consumeMessageSeqWithChecker connect consumeMessage checker intervalChecker log configuration
            | Some checker, None -> consumeMessageSeqWithChecker connect consumeMessage checker IntervalChecker.empty log configuration
            | _ -> consumeMessageSeq connect consumeMessage log configuration

    //
    // Public api
    //

    // Consume events as string values

    type ParseEvent<'Event> = TracedMessage<string> -> 'Event

    let consume (configuration: ConsumerConfiguration) (parse: ParseEvent<'Event>): Result<'Event, ConsumeError> seq =
        configuration
        |> Consume.seq Consumer.connect Consume.consumeMessageValue
        |> Seq.map (Result.map (TracedMessage.finish >> parse))

    let consumeLast (configuration: ConsumerConfiguration) (parse: ParseEvent<'Event>): Result<'Event, ConsumeError> option =
        configuration
        |> Consume.seq Consumer.connectLastMessage Consume.consumeMessageValue
        |> Seq.tryHead
        |> Option.map (Result.map (TracedMessage.finish >> parse))

    // Consume events as Messages

    type ParseEventMessage<'Event> = TracedMessage<Message> -> 'Event

    let consumeMessages (configuration: ConsumerConfiguration) (parse: ParseEventMessage<'Event>): Result<'Event, ConsumeError> seq =
        configuration
        |> Consume.seq Consumer.connect Consume.consumeMessage
        |> Seq.map (Result.map (TracedMessage.finish >> parse))

    let consumeLastMessage (configuration: ConsumerConfiguration) (parse: ParseEventMessage<'Event>): Result<'Event, ConsumeError> option =
        configuration
        |> Consume.seq Consumer.connectLastMessage Consume.consumeMessage
        |> Seq.tryHead
        |> Option.map (Result.map (TracedMessage.finish >> parse))
