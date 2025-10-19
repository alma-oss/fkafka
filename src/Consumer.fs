namespace Alma.Kafka

open System.Threading
open FSharp.Control
open Confluent.Kafka
open Microsoft.Extensions.Logging

open Alma.Metrics.ServiceStatus
open Alma.Tracing
open Alma.ErrorHandling

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
    Logger: ILogger option
    Checker: Checker option
    IntervalChecker: IntervalChecker option
    ServiceStatus: ServiceStatus option
    Cancellation: CancellationToken option
    CountLag: bool

    /// Default: Automatically (same as Kafka.EnableAutocommit: true)
    CommitMessage: CommitMessage

    /// Custom checkpoint, when using external storage for offsets
    GetCheckpoint: GetCheckpoint option
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
            Cancellation = None
            CountLag = false
            CommitMessage = CommitMessage.Automatically
            GetCheckpoint = None
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
    | RuntimeError of string
    | BrokerError
    | TopicError
    | MaxRetriesReached of KafkaException
    | PreviousMessageWasNotCommited

[<RequireQualifiedAccess>]
type ManualCommitError =
    | KafkaException of KafkaException
    | RuntimeException of exn

//
// Consumer
//

type ManualCommit = ManualCommit of (unit -> Result<unit, ManualCommitError>)

[<RequireQualifiedAccess>]
module ManualCommit =
    let execute (ManualCommit commit) = commit ()

[<RequireQualifiedAccess>]
module Consumer =
    open System

    type private KafkaMessageKey = Ignore
    type private KafkaMessageValue = string

    type internal KafkaConsumer = KafkaConsumer of IConsumer<KafkaMessageKey, KafkaMessageValue>
    type KafkaMessage = internal KafkaMessage of ConsumeResult<KafkaMessageKey, KafkaMessageValue>

    [<RequireQualifiedAccess>]
    module private KafkaMessage =
        let value (KafkaMessage message) = message

    [<RequireQualifiedAccess>]
    module private KafkaConsumer =
        let value (KafkaConsumer kafkaConsumer) = kafkaConsumer

    [<RequireQualifiedAccess>]
    module private KafkaOffset =
        let value: Offset -> int64 option = function
            | special when special.IsSpecial -> None
            | offset -> Some offset.Value

    [<Struct>]
    type internal ConsumeRuntime = {
        /// Acutal list of bootstrap servers used for consuming
        BootstrapServers: string
        /// Acutal group id used for consuming
        GroupId: string
        /// Acutal topic used for consuming
        Topic: string

        /// If autocommit is not enabled, consumer client must commit the processed result manually
        IsAutocommitEnabled: bool
        FailOnNotCommittedMessage: bool
        CountLag: bool

        UseTracing: bool
        Cancellation: CancellationToken option
    }

    [<Struct>]
    type TracedMessage<'Message> = {
        Commit: ManualCommit
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

    type internal TracedMessageResult<'Message> = Result<TracedMessage<'Message>, ConsumeError * Trace>

    [<RequireQualifiedAccess>]
    module internal TracedMessageResult =
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
        Partition: int option
        Offset: int64 option
        Value: string
        Lag: int64 option
    }

    [<RequireQualifiedAccess>]
    module Message =
        let value ({ Value = value }: Message) = value
        let offset ({ Offset = offset }: Message) = offset
        let partition ({ Partition = partition }: Message) = partition

    type ConsumedMessage<'Message> = {
        Commit: ManualCommit
        Message: 'Message
    }

    type private OnPartitionsAssigned = IConsumer<KafkaMessageKey,KafkaMessageValue> -> Collections.Generic.List<TopicPartition> -> Collections.Generic.IEnumerable<TopicPartitionOffset>

    let private partitionOffsetHandler getOffset (logger: ILogger option): OnPartitionsAssigned = fun c partitions ->
        logger |> Option.iter (fun logger -> logger.LogDebug "Getting checkpoint offsets for partitions")
        partitions
        |> Seq.toList
        |> List.choose (
            TopicPartition.ofKafka
            >> Option.map (
                getOffset
                >> AsyncResult.retryWithExponential
                    (fun message -> logger |> Option.iter (fun logger -> logger.LogInformation message))
                    1000
                    10
            )
        )
        |> AsyncResult.ofParallelAsyncResults id
        |> Async.RunSynchronously
        |> function
            | Ok offsets ->
                offsets
                |> List.map TopicPartitionOffset.toKafka
                |> List.toSeq

            | Error e ->
                logger |> Option.iter (fun logger -> logger.LogCritical("Could not get offsets for partitions: {error}", e))
                e |> List.head |> raise

    [<RequireQualifiedAccess>]
    module private Consumer =
        let private createDefaultConfig (BrokerList brokerList) groupId commitMessage =
            let config =
                ConsumerConfig(
                    GroupId = (groupId |> GroupId.value),
                    BootstrapServers = brokerList,
                    AutoOffsetReset = (AutoOffsetReset.Earliest |> Nullable)
                )

            match commitMessage with
            | CommitMessage.Automatically -> config.EnableAutoCommit <- true
            | CommitMessage.Manually _ -> config.EnableAutoCommit <- false

            config

        let private createConsumer configuration (config: ConsumerConfig): Consumer =
            let topicValue = configuration.Connection.Topic |> StreamName.value

            let consumer =
                let builder =
                    configuration.GetCheckpoint
                    |> Option.fold (fun (builder: ConsumerBuilder<_, _>) getCheckpoint ->
                        configuration.Logger |> Option.iter (fun logger -> logger.LogInformation "Using custom GetCheckpoint for partitions assignment")
                        builder.SetPartitionsAssignedHandler(partitionOffsetHandler (getCheckpoint configuration.GroupId) configuration.Logger)
                    ) (ConsumerBuilder config)

                builder.Build()

            topicValue |> consumer.Subscribe

            {
                KafkaConsumer = KafkaConsumer consumer
                Runtime = {
                    BootstrapServers = config.BootstrapServers
                    GroupId = config.GroupId
                    Topic = topicValue

                    IsAutocommitEnabled = config.EnableAutoCommit.GetValueOrDefault true
                    FailOnNotCommittedMessage =
                        match configuration.CommitMessage with
                        | CommitMessage.Manually FailOnNotCommittedMessage.WithException -> true
                        | _ -> false
                    CountLag = true

                    UseTracing = Tracer.Check.isTracerAvailable()
                    Cancellation = configuration.Cancellation
                }
                Logger = configuration.Logger
            }

        let private create (configuration: ConsumerConfiguration) =
            createDefaultConfig configuration.Connection.BrokerList configuration.GroupId configuration.CommitMessage
            |> createConsumer configuration

        let connect (configuration: ConsumerConfiguration) =
            configuration.Logger |> Option.iter (fun logger -> logger.LogDebug("Connecting"))
            create configuration

        let close (consumer: Consumer) =
            consumer.Close()

    [<RequireQualifiedAccess>]
    module private Lag =
        let private countForPartition offset partition (KafkaConsumer consumer) =
            let watermark = consumer.GetWatermarkOffsets(partition)

            if watermark.High.IsSpecial || watermark.Low.IsSpecial then None
            else
                let lowOffset = offset |> Option.defaultValue watermark.Low.Value
                let result = watermark.High.Value - lowOffset

                //! For debugging only
                (* if (watermark.High.Value + lowOffset > 0) then
                    printfn $"  -[{partition}]-> {watermark.High.Value} - {lowOffset} = {result}" *)

                Some result

        let count: Consumer -> int64 option = function
            | { Runtime = { CountLag = false }} -> None
            | consumer ->
                let (KafkaConsumer kafkaConsumer) = consumer.KafkaConsumer

                // todo - tady bude nejaky problem v tom, pocitani - bud na `.Assignment` nedostanu vsechy partitiony nebo maji partitiony lag jako special a nespocitam ho
                // - ale kdyz ctu lag na streamu s vice partitionama, tak dostavam lag jen z jedne (asi)
                kafkaConsumer.Assignment
                |> Seq.fold
                    (fun acc partition ->
                        let currentOffset =
                            kafkaConsumer.Position(partition)
                            |> KafkaOffset.value

                        match consumer.KafkaConsumer |> countForPartition currentOffset partition with
                        | Some lag -> acc + lag
                        | _ -> acc
                    )
                    (int64 0)
                |> Some

    [<RequireQualifiedAccess>]
    module private Consume =
        open Alma.State.ConcurrentStorage

        type ConsumeMessage<'Message> = Consumer -> Result<TracedMessage<'Message>, ConsumeError * Trace> option

        let private createStartReadingMessage groupId =
            let groupIdToLog = function
                | GroupId.Id groupId -> groupId
                | GroupId.Random -> ""

            groupId
            |> GroupId.map (sprintf " with %s")
            |> groupIdToLog
            |> sprintf "Reading stream%s ..."

        type private LastMessageManuallyCommitted =
            | NoConsumedMessage
            | MessageIsNotCommitedYet
            | MessageIsCommited

        type private ManualCommitKey = ManualCommitKey of (StreamName * GroupId)
        let private lastMessageManuallyCommittedState: State<ManualCommitKey, LastMessageManuallyCommitted> = State.empty()

        let logLastMessageManuallyCommittedState (logger: ILogger) =
            lastMessageManuallyCommittedState
            |> State.iter (fun (Key key, value) ->
                logger.LogInformation("Last message state: {key} -> {value}", key, value)
            )

        let private clearLastMessageManuallyCommittedState (logger: ILogger option) (key: ManualCommitKey) =
            logger |> Option.iter (fun logger -> logger.LogDebug("Clearing manual commit state for {key}", key))

            lastMessageManuallyCommittedState
            |> State.set (Key key) NoConsumedMessage

        let private manualCommit (consumer: Consumer) manualCommitKey (KafkaMessage result) = ManualCommit (fun () ->
            try
                if not consumer.Runtime.IsAutocommitEnabled then
                    let (KafkaConsumer kafkaConsumer) = consumer.KafkaConsumer
                    kafkaConsumer.Commit(result)
                    lastMessageManuallyCommittedState |> State.set (Key manualCommitKey) MessageIsCommited
                    consumer.Logger |> Option.iter (fun logger -> logger.LogDebug("Event in {manualCommitKey} is commited!", manualCommitKey))

                Ok ()
            with
            | :? KafkaException as e -> Result.Error (ManualCommitError.KafkaException e)
            | e -> Result.Error (ManualCommitError.RuntimeException e)
        )

        let private consume: ConsumeMessage<KafkaMessage> = fun consumer ->
            try
                let consumeResult =
                    let kafkaConsumer = consumer.KafkaConsumer |> KafkaConsumer.value
                    match consumer.Runtime.Cancellation with
                    | Some cancelationToken -> kafkaConsumer.Consume(cancelationToken)
                    | _ -> kafkaConsumer.Consume()

                let manualCommitKey = ManualCommitKey (StreamName consumer.Runtime.Topic, GroupId.Id consumer.Runtime.GroupId)

                if isNull consumeResult then
                    lastMessageManuallyCommittedState |> State.tryRemove (Key manualCommitKey)
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
                                "kafka.partition", string consumeResult.Partition.Value
                                "kafka.offset", string consumeResult.Offset.Value
                                "kafka.group_id", consumer.Runtime.GroupId
                                "span.kind", "consumer"
                            ]
                        else Inactive

                    if not consumer.Runtime.IsAutocommitEnabled then
                        consumer.Logger |> Option.iter (fun logger -> logger.LogDebug("Event in {manualCommitKey} is consumed and waiting for commit ...", manualCommitKey))

                    let messageResult =
                        match lastMessageManuallyCommittedState |> State.tryFind (Key manualCommitKey) with
                        | Some MessageIsNotCommitedYet when not consumer.Runtime.IsAutocommitEnabled && consumer.Runtime.FailOnNotCommittedMessage ->
                            Result.Error (ConsumeError.PreviousMessageWasNotCommited, trace)
                        | _ ->
                            lastMessageManuallyCommittedState |> State.set (Key manualCommitKey) MessageIsNotCommitedYet
                            Ok {
                                Commit = manualCommit consumer manualCommitKey (KafkaMessage consumeResult)
                                Message = KafkaMessage consumeResult
                                Trace = trace
                            }

                    Some messageResult
            with
            | :? KafkaException as e -> Result.Error (ConsumeError.KafkaException e, Trace.Inactive) |> Some
            | e -> Result.Error (ConsumeError.RuntimeException e, Trace.Inactive) |> Some

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
                Partition = if message.Partition.IsSpecial then None else Some message.Partition.Value
                Offset = message.Offset |> KafkaOffset.value
                Value = message.Message.Value
                Lag = consumer |> Lag.count
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
                            yield Result.Error (consumeError.Value, Trace.Inactive)
                finally
                    consumer.LogDebug "Cancel checker tokens ..."
                    cancellationTokenSource.Cancel()

                    markAsDisabled |> MarkAsDisabled.execute
            }

        let seq connect (consumeMessage: ConsumeMessage<'Message>) (configuration: ConsumerConfiguration) =
            ManualCommitKey (configuration.Connection.Topic, configuration.GroupId)
            |> clearLastMessageManuallyCommittedState configuration.Logger

            match (configuration.Checker, configuration.IntervalChecker) with
            | Some checker, Some intervalChecker -> consumeMessageSeqWithChecker connect consumeMessage checker intervalChecker configuration
            | Some checker, None -> consumeMessageSeqWithChecker connect consumeMessage checker IntervalChecker.empty configuration
            | _ -> consumeMessageSeq connect consumeMessage configuration

    [<RequireQualifiedAccess>]
    module private Parse =
        let private handleTracedConsumeError (error: ConsumeError, trace: Trace) =
            trace
            |> Trace.addError (error |> TracedError.ofError (sprintf "%A"))
            |> Trace.finish

            error

        let consumedMessage
            (parseEvent: TracedMessage<'Message> -> 'Event)
            (tracedMessage: TracedMessageResult<'Message>)
            : Result<ConsumedMessage<'Event>, ConsumeError> =
            result {
                let! (tracedMessage: TracedMessage<'Message>) = tracedMessage

                let (parsedEvent: 'Event) =
                    tracedMessage
                    |> TracedMessage.finish
                    |> parseEvent

                return {
                    Commit = tracedMessage.Commit
                    Message = parsedEvent
                }
            }
            |> Result.mapError handleTracedConsumeError

        let consumedMessageAsync<'Message, 'Event>
            (parseEvent: TracedMessage<'Message> -> AsyncResult<'Event, ConsumeError>)
            (tracedMessageResult: TracedMessageResult<'Message>)
            : AsyncResult<ConsumedMessage<'Event>, ConsumeError> =
            asyncResult {
                let! (tracedMessage: TracedMessage<'Message>) = tracedMessageResult

                let! (parsedMessage: 'Event) =
                    tracedMessage
                    |> TracedMessage.finish
                    |> parseEvent
                    |> AsyncResult.mapError (fun consumeError -> consumeError, tracedMessage.Trace)

                let consumedMessage: ConsumedMessage<'Event> = {
                    Commit = tracedMessage.Commit
                    Message = parsedMessage
                }

                return consumedMessage
            }
            |> AsyncResult.mapError handleTracedConsumeError

    //
    // Public api
    //

    type ConsumedResult<'Event> = Result<ConsumedMessage<'Event>, ConsumeError>
    type ConsumedAsyncResult<'Event> = AsyncResult<ConsumedMessage<'Event>, ConsumeError>

    // Consume events as string values

    type ParseEvent<'Event> = TracedMessage<string> -> 'Event
    type ParseEventAsyncResult<'Event> = TracedMessage<string> -> AsyncResult<'Event, ConsumeError>

    let consume (configuration: ConsumerConfiguration) (parse: ParseEvent<'Event>): ConsumedResult<'Event> seq =
        configuration
        |> Consume.seq Consumer.connect Consume.consumeMessageValue
        |> Seq.map (Parse.consumedMessage parse)

    let consumeAsync (configuration: ConsumerConfiguration) (parse: ParseEventAsyncResult<'Event>): ConsumedAsyncResult<'Event> seq =
        configuration
        |> Consume.seq Consumer.connect Consume.consumeMessageValue
        |> Seq.map (Parse.consumedMessageAsync parse)

    // Consume events as Messages

    type ParseEventMessage<'Event> = TracedMessage<Message> -> 'Event
    type ParseEventMessageAsyncResult<'Event> = TracedMessage<Message> -> AsyncResult<'Event, ConsumeError>

    let consumeMessages (configuration: ConsumerConfiguration) (parse: ParseEventMessage<'Event>): ConsumedResult<'Event> seq =
        configuration
        |> Consume.seq Consumer.connect Consume.consumeMessage
        |> Seq.map (Parse.consumedMessage parse)

    let consumeMessagesAsync (configuration: ConsumerConfiguration) (parse: ParseEventMessageAsyncResult<'Event>): ConsumedAsyncResult<'Event> seq =
        configuration
        |> Consume.seq Consumer.connect Consume.consumeMessage
        |> Seq.map (Parse.consumedMessageAsync parse)

    // Handle Consumer state for manual commits

    let logLastMessageManuallyCommittedState = Consume.logLastMessageManuallyCommittedState
