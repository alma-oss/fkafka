namespace Lmc.Kafka

open System.Threading
open FSharp.Control
open Confluent.Kafka
open Microsoft.Extensions.Logging

open Lmc.Metrics.ServiceStatus
open Lmc.Tracing

[<System.Obsolete("Define a configuration in Consumer configuration directly and do not use this hidden configure.")>]
type ConfigureConnsumer =
    ConfigureConnsumer of (ConsumerConfig -> ConsumerConfig)

[<RequireQualifiedAccess>]
module ConfigureConnsumer =
    /// see https://docs.confluent.io/clients-confluent-kafka-dotnet/current/overview.html#synchronous-commits
    let [<System.Obsolete("todo remove")>] setUpManualCommiting = ConfigureConnsumer (fun config ->
        config.EnableAutoCommit <- false
        config
    )

    /// see https://docs.confluent.io/clients-confluent-kafka-dotnet/current/overview.html#store-offsets
    let [<System.Obsolete("todo remove")>] setUpManualOffsetStoring = ConfigureConnsumer (fun config ->
        config.EnableAutoCommit <- true
        config.EnableAutoOffsetStore <- false
        config
    )

[<RequireQualifiedAccess>]
type FailOnNotCommittedMessage =
    | WithException
    | IgnoringAndContinue

[<RequireQualifiedAccess>]
type CommitMessage =
    | Automatically
    | Manually of FailOnNotCommittedMessage

type ConsumerConfiguration = {
    Connection: ConnectionConfiguration
    GroupId: GroupId
    Configure: ConfigureConnsumer option
    Logger: ILogger option
    Checker: Checker option
    IntervalChecker: IntervalChecker option
    ServiceStatus: ServiceStatus option

    /// Default: Automatically (same as Kafka.EnableAutocommit: true)
    CommitMessage: CommitMessage
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
            CommitMessage = CommitMessage.Automatically
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
    | PreviousMessageWasNotCommited

//
// Consumer
//

[<RequireQualifiedAccess>]
module Consumer =
    open System

    type internal KafkaConsumer = KafkaConsumer of IConsumer<Ignore, string>
    type KafkaMessage = internal KafkaMessage of ConsumeResult<Ignore, string>

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

        /// If autocommit is not enabled, consumer client must commit the processed result manually
        IsAutocommitEnabled: bool
        FailOnNotCommittedMessage: bool

        UseTracing: bool
    }

    [<Struct>]
    type TracedMessage<'Message> = {
        Commit: unit -> Result<unit, exn>
        Message: 'Message
        Trace: Trace
    }

    [<RequireQualifiedAccess>]
    module TracedMessage =
        let message ({ Message = message }: TracedMessage<'Message>) = message
        let trace ({ Trace = trace }: TracedMessage<'Message>) = trace
        let finish message = message |> tee (trace >> Trace.finish)

        let map f message =
            { Message = message.Message |> f; Trace = message.Trace; Commit = message.Commit }

    type TracedMessageResult<'Message> = Result<TracedMessage<'Message>, ConsumeError>

    [<RequireQualifiedAccess>]
    module TracedMessageResult =
        let map f =
            Result.map (TracedMessage.map f)

    type private Consumer =
        {
            /// Actual kafka consumer used for consuming events
            KafkaConsumer: KafkaConsumer
            /// Actual Runtime information about consume
            Runtime: ConsumeRuntime
            Logger: ILogger option
        }

        member this.LogDebug(message: string): unit =
            this.Logger |> Option.iter (fun logger -> logger.LogDebug(message))

        member this.Close() =
            this.LogDebug("Consumer closing ...")
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

    type ConsumedMessage<'Message> = {
        Commit: unit -> Result<unit, exn>
        Message: 'Message
    }

    [<RequireQualifiedAccess>]
    module private Consumer =
        let private createDefaultConfig (BrokerList brokerList) groupId commitMessage configure =
            let config =
                ConsumerConfig(
                    GroupId = (groupId |> GroupId.value),
                    BootstrapServers = brokerList,
                    AutoOffsetReset = (AutoOffsetReset.Earliest |> Nullable)
                )

            match commitMessage with
            | CommitMessage.Automatically -> config.EnableAutoCommit <- true
            | CommitMessage.Manually _ -> config.EnableAutoCommit <- false

            match configure with
            | Some (ConfigureConnsumer configure) -> configure config
            | _ -> config

        let private createConsumer configuration (config: ConsumerConfig): Consumer =
            let topicValue = configuration.Connection.Topic |> StreamName.value

            let consumer = ConsumerBuilder(config).Build()
            topicValue |> consumer.Subscribe

            {
                KafkaConsumer = KafkaConsumer consumer
                Runtime = {
                    BootstrapServers = config.BootstrapServers
                    GroupId = config.GroupId
                    Topic = topicValue
                    Partition = ConsumerConfiguration.DefaultPartition

                    IsAutocommitEnabled = config.EnableAutoCommit.GetValueOrDefault true
                    FailOnNotCommittedMessage =
                        match configuration.CommitMessage with
                        | CommitMessage.Manually FailOnNotCommittedMessage.WithException -> true
                        | _ -> false

                    UseTracing = Tracer.Check.isTracerAvailable()
                }
                Logger = configuration.Logger
            }

        let private createConsumerForLastMessage configuration (config: ConsumerConfig): Consumer =
            let topicValue = configuration.Connection.Topic |> StreamName.value

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

                    IsAutocommitEnabled = config.EnableAutoCommit.GetValueOrDefault true
                    FailOnNotCommittedMessage =
                        match configuration.CommitMessage with
                        | CommitMessage.Manually FailOnNotCommittedMessage.WithException -> true
                        | _ -> false

                    UseTracing = Tracer.Check.isTracerAvailable()
                }
                Logger = configuration.Logger
            }

        let private create (configuration: ConsumerConfiguration) =
            createDefaultConfig configuration.Connection.BrokerList configuration.GroupId configuration.CommitMessage configuration.Configure
            |> createConsumer configuration

        let private createForLastMessage (configuration: ConsumerConfiguration) =
            createDefaultConfig configuration.Connection.BrokerList GroupId.Random configuration.CommitMessage configuration.Configure
            |> createConsumerForLastMessage configuration

        let connect (configuration: ConsumerConfiguration) =
            configuration.Logger |> Option.iter (fun logger -> logger.LogDebug("Connecting"))
            create configuration

        let connectLastMessage (configuration: ConsumerConfiguration) =
            configuration.Logger |> Option.iter (fun logger -> logger.LogDebug("Connecting for last message ..."))
            createForLastMessage configuration

        let close (consumer: Consumer) =
            consumer.Close()

    [<RequireQualifiedAccess>]
    module private Consume =
        type ConsumeMessage<'Message> = Consumer -> Result<TracedMessage<'Message>, ConsumeError> option

        let private createStartReadingMessage groupId =
            let groupIdToLog = function
                | Id groupId -> groupId
                | Random -> ""

            groupId
            |> GroupId.map (sprintf " with %s")
            |> groupIdToLog
            |> sprintf "Reading stream%s ..."

        type private LastMessageManuallyCommitted =
            | NoConsumedMessage
            | MessageIsNotCommitedYet
            | MessageIsCommited

        // todo: this should be in state: State<Topic * GroupId, LastMessageManuallyCommitted>
        let mutable private isLastMessageManuallyCommited: LastMessageManuallyCommitted = NoConsumedMessage

        let private manualCommit (consumer: Consumer) (KafkaMessage result) =
            try
                if not consumer.Runtime.IsAutocommitEnabled then
                    let (KafkaConsumer consumer) = consumer.KafkaConsumer
                    consumer.Commit(result)
                    isLastMessageManuallyCommited <- MessageIsCommited

                Ok ()
            with e -> Result.Error e    // todo - type error

        let private consume: ConsumeMessage<KafkaMessage> = fun consumer ->
            try
                let consumeResult = (consumer.KafkaConsumer |> KafkaConsumer.value).Consume()

                if isNull consumeResult then
                    isLastMessageManuallyCommited <- NoConsumedMessage
                    None
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

                    let message = {
                        Commit = fun () -> manualCommit consumer (KafkaMessage consumeResult)
                        Message = KafkaMessage consumeResult
                        Trace = trace
                    }

                    let messageResult =
                        match isLastMessageManuallyCommited with
                        | MessageIsNotCommitedYet when not consumer.Runtime.IsAutocommitEnabled && consumer.Runtime.FailOnNotCommittedMessage ->
                            Result.Error ConsumeError.PreviousMessageWasNotCommited
                        | _ ->
                            isLastMessageManuallyCommited <- MessageIsNotCommitedYet
                            Ok message

                    Some messageResult
            with
            | :? KafkaException as e -> Result.Error (ConsumeError.KafkaException e) |> Some
            | e -> Result.Error (ConsumeError.RuntimeException e) |> Some

        /// Helper function to combine all map functions
        let private map f = Option.map (TracedMessageResult.map (KafkaMessage.value >> f))

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

        let private consumeMessageSeq connect (consumeMessage: ConsumeMessage<'Message>) configuration =
            let (markAsEnabled, markAsDisabled) = configuration.ServiceStatus |> ServiceStatus.resolve

            seq {
                try
                    use consumer: Consumer = configuration |> connect

                    configuration.GroupId |> createStartReadingMessage |> consumer.LogDebug
                    markAsEnabled |> MarkAsEnabled.execute

                    while true do
                        let message = consumer |> consumeMessage
                        if message.IsSome then
                            yield message.Value

                finally
                    markAsDisabled |> MarkAsDisabled.execute
            }

        let private consumeMessageSeqWithChecker connect (consumeMessage: ConsumeMessage<'Message>) checker intervalChecker configuration =
            let maxRetries = checker.MaxRetries
            let defaultWaitForResource = checker.WaitForResourceDefault

            let mutable attempt = 1<Attempt>
            let mutable waitForResource = defaultWaitForResource

            let (markAsEnabled, markAsDisabled) = configuration.ServiceStatus |> ServiceStatus.resolve

            let markAsEnabledAndRestartWaitTime () =
                markAsEnabled |> MarkAsEnabled.execute
                defaultWaitForResource

            seq {
                use consumer: Consumer = configuration |> connect
                use cancellationTokenSource = new CancellationTokenSource()

                let (KafkaConsumer kafkaConsumer) = consumer.KafkaConsumer

                try
                    let mutable consumeError: ConsumeError option = None

                    let asyncStartWithCancellation computation =
                        Async.Start (computation, cancellationTokenSource.Token)

                    while attempt <= maxRetries do
                        match checker.CheckCluster kafkaConsumer.Handle, checker.CheckTopic configuration.Connection.Topic kafkaConsumer.Handle with
                        | true, true ->
                            configuration.GroupId |> createStartReadingMessage |> consumer.LogDebug
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
                            let (currentAttempt, waitFor) = MarkAsDisabled.executeAndWait consumer.LogDebug attempt maxRetries markAsDisabled waitForResource

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
                    consumer.LogDebug "Cancel checker tokens ..."
                    cancellationTokenSource.Cancel()

                    markAsDisabled |> MarkAsDisabled.execute
            }

        let seq connect (consumeMessage: ConsumeMessage<'Message>) configuration =
            match (configuration.Checker, configuration.IntervalChecker) with
            | Some checker, Some intervalChecker -> consumeMessageSeqWithChecker connect consumeMessage checker intervalChecker configuration
            | Some checker, None -> consumeMessageSeqWithChecker connect consumeMessage checker IntervalChecker.empty configuration
            | _ -> consumeMessageSeq connect consumeMessage configuration

    //
    // Public api
    //

    // Consume events as string values

    type ParseEvent<'Event> = TracedMessage<string> -> 'Event
    type ConsumedResult<'Event> = Result<ConsumedMessage<'Event>, ConsumeError>

    let private parseConsumedMessage (parseEvent: TracedMessage<'Message> -> 'Event) (tracedMessage: TracedMessageResult<'Message>): ConsumedResult<'Event> =
        tracedMessage
        |> Result.map (fun tracedMessage ->
            {
                Commit = tracedMessage.Commit
                Message =
                    tracedMessage
                    |> TracedMessage.finish
                    |> parseEvent
            }
        )

    let consume (configuration: ConsumerConfiguration) (parse: ParseEvent<'Event>): ConsumedResult<'Event> seq =
        configuration
        |> Consume.seq Consumer.connect Consume.consumeMessageValue
        |> Seq.map (parseConsumedMessage parse)

    let consumeLast (configuration: ConsumerConfiguration) (parse: ParseEvent<'Event>): ConsumedResult<'Event> option =
        configuration
        |> Consume.seq Consumer.connectLastMessage Consume.consumeMessageValue
        |> Seq.tryHead
        |> Option.map (parseConsumedMessage parse)

    // Consume events as Messages

    type ParseEventMessage<'Event> = TracedMessage<Message> -> 'Event

    let consumeMessages (configuration: ConsumerConfiguration) (parse: ParseEventMessage<'Event>): ConsumedResult<'Event> seq =
        configuration
        |> Consume.seq Consumer.connect Consume.consumeMessage
        |> Seq.map (parseConsumedMessage parse)

    let consumeLastMessage (configuration: ConsumerConfiguration) (parse: ParseEventMessage<'Event>): ConsumedResult<'Event> option =
        configuration
        |> Consume.seq Consumer.connectLastMessage Consume.consumeMessage
        |> Seq.tryHead
        |> Option.map (parseConsumedMessage parse)
