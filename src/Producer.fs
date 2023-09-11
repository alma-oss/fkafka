namespace Alma.Kafka

open System
open Microsoft.Extensions.Logging
open Alma.Metrics.ServiceStatus
open Alma.Tracing

type ProducerConfiguration = {
    Connection: ConnectionConfiguration
    Logger: ILogger option
    Checker: Checker option
    MarkAsDisabled: MarkAsDisabled option
}

[<RequireQualifiedAccess>]
module ProducerConfiguration =
    let createWithConnection connection =
        {
            Connection = connection
            Logger = None
            Checker = None
            MarkAsDisabled = None
        }

    let createWithDefaults brokerList topic =
        createWithConnection {
            BrokerList = brokerList
            Topic = topic
        }

//
// Message
//

open Confluent.Kafka

type private KafkaMessageKey = string
type private KafkaMessageValue = string

type private KafkaMessage = Message<KafkaMessageKey, KafkaMessageValue>

/// Key used for a message in kafka. Format is a string with values delimited by `,`. (this format is used by KSQL, etc.)
[<RequireQualifiedAccess>]
type MessageKey =
    | Simple of KafkaMessageKey
    | Delimited of KafkaMessageKey list

[<RequireQualifiedAccess>]
module MessageKey =
    let private normalize (key: KafkaMessageKey) =
        key.Replace(" ", "")

    let value = function
        | MessageKey.Simple key -> normalize key
        | MessageKey.Delimited values -> values |> String.concat "," |> normalize

[<RequireQualifiedAccess>]
type MessageToProduce = {
    Key: MessageKey
    Headers: Alma.Kafka.Header list
    Value: KafkaMessageValue
}

type internal CreateKafkaMessage = MessageToProduce -> KafkaMessage

[<RequireQualifiedAccess>]
module MessageToProduce =
    let internal asKafkaHeaders headers =
        let messageHeaders = Headers()
        headers |> List.iter (Header.toKafkaHeader >> messageHeaders.Add)
        messageHeaders

    let internal createKafkaMessage: CreateKafkaMessage = function
        | { Key = key; Value = value; Headers = [] } ->
            KafkaMessage(Key = (key |> MessageKey.value), Value = value )

        | { Key = key; Value = value; Headers = headers } ->
            KafkaMessage(Key = (key |> MessageKey.value), Value = value, Headers = (headers |> asKafkaHeaders) )

    let createWithHeaders headers (key, value): MessageToProduce = { Key = key; Value = value; Headers = headers }
    let create = createWithHeaders []

    let key { Key = key } = key
    let value { Value = value } = value

//
// Producer
//

type private KafkaProducer = KafkaProducer of IProducer<KafkaMessageKey, KafkaMessageValue>

[<RequireQualifiedAccess>]
module private KafkaProducer =
    let value (KafkaProducer kafkaProducer) = kafkaProducer

    let create (BrokerList brokerList): KafkaProducer =
        let config =
            ProducerConfig(
                BootstrapServers = brokerList
            )
        ProducerBuilder(config).Build() |> KafkaProducer

[<Struct>]
type private ProduceRuntime = {
    /// Acutal list of bootstrap servers used for producing
    BootstrapServers: string
    /// Acutal topic used for producing
    Topic: string

    UseTracing: bool
}

type Producer =
    private {
        KafkaProducer: KafkaProducer
        Topic: StreamName
        Runtime: ProduceRuntime
        Logger: ILogger option
    }

    member internal this.LogDebug(message: string): unit =
        this.Logger |> Option.iter (fun logger -> logger.LogDebug(message))

    member this.Flush() =
        this.LogDebug("Flushing producer ...")
        (this.KafkaProducer |> KafkaProducer.value).Flush()

    member this.Close() =
        this.Flush()
        this.LogDebug("Closing producer ...")
        (this.KafkaProducer |> KafkaProducer.value).Dispose()

    interface IDisposable with
        member this.Dispose() =
            this.Close()

[<RequireQualifiedAccess>]
module Producer =
    type NotConnected = private NotConnectedProducer of (unit -> Producer)

    let private createProducer (configuration: ProducerConfiguration): Producer =
        configuration.Logger |> Option.iter (fun logger -> logger.LogDebug("Connecting producer ..."))
        {
            KafkaProducer = configuration.Connection.BrokerList |> KafkaProducer.create
            Topic = configuration.Connection.Topic
            Runtime = {
                BootstrapServers = configuration.Connection.BrokerList |> BrokerList.value
                Topic = configuration.Connection.Topic |> StreamName.value
                UseTracing = Tracer.Check.isTracerAvailable()
            }
            Logger = configuration.Logger
        }

    let private createProducerWithChecker checker (configuration: ProducerConfiguration): Producer =
        let maxRetries = checker.MaxRetries
        let markAsDisabled = configuration.MarkAsDisabled |> ServiceStatus.resolveMarkAsDisabled

        let mutable attempt = 1<Attempt>
        let mutable waitForResource = checker.WaitForResourceDefault

        configuration.Logger |> Option.iter (fun logger -> logger.LogDebug("Connecting producer ..."))
        let (KafkaProducer producer) = configuration.Connection.BrokerList |> KafkaProducer.create

        seq {
            while attempt <= maxRetries do
                match checker.CheckCluster producer.Handle, checker.CheckTopic configuration.Connection.Topic producer.Handle with
                | true, true ->
                    yield {
                        KafkaProducer = KafkaProducer producer
                        Topic = configuration.Connection.Topic
                        Runtime = {
                            BootstrapServers = configuration.Connection.BrokerList |> BrokerList.value
                            Topic = configuration.Connection.Topic |> StreamName.value
                            UseTracing = Tracer.Check.isTracerAvailable()
                        }
                        Logger = configuration.Logger
                    }
                | _ ->
                    let log message = configuration.Logger |> Option.iter (fun logger -> logger.LogDebug(message))
                    let (currentAttempt, waitFor) = MarkAsDisabled.executeAndWait log attempt maxRetries markAsDisabled waitForResource

                    attempt <- currentAttempt
                    waitForResource <- waitFor
        }
        |> tee (fun producers ->
            if producers |> Seq.isEmpty then
                sprintf "There is no connected producer. Problem is with either %A and/or a %A." configuration.Connection.BrokerList configuration.Connection.Topic
                |> tee (fun message -> configuration.Logger |> Option.iter (fun logger -> logger.LogError(message)))
                |> failwith
        )
        |> Seq.head

    //
    // Public Producer functions
    //

    let create configuration =
        match configuration.Checker with
        | Some checker -> createProducerWithChecker checker configuration
        | _ -> createProducer configuration

    let prepare configuration = NotConnectedProducer (fun () -> createProducer configuration)
    let connect (NotConnectedProducer connect) = connect()

    let flush (producer: Producer) = producer.Flush()
    let close (producer: Producer) = producer.Close()

    //
    // Produce messages
    //

    [<RequireQualifiedAccess>]
    module private Produce =
        type private ProduceError =
            | BrokerError of ErrorCode * string
            | LocalError of ErrorCode * string
            | FatalError of ErrorCode * string
            | RuntimeError of ErrorCode * string

        module private ProduceError =
            let format = function
                | BrokerError (code, reason) -> $"Kafka broker error({code}): {reason}"
                | LocalError (code, reason) -> $"Kafka local error({code}): {reason}"
                | FatalError (code, reason) -> $"Kafka fatal error({code}): {reason}"
                | RuntimeError (code, reason) -> $"Kafka runtime error({code}): {reason}"

        let messageWith (producer: Producer) (message: MessageToProduce) =
            let topicValue = producer.Runtime.Topic

            let produceTrace =
                let headers = message.Headers |> MessageToProduce.asKafkaHeaders

                if producer.Runtime.UseTracing then
                    "Produce event"
                    |> Trace.ChildOf.continueOrStart (Trace.extractFromKafkaHeaders headers >> Trace.ofContextOption)
                    |> Trace.addTags [
                        "peer.service", "kafka"
                        "peer.address", producer.Runtime.BootstrapServers
                        "component:", (sprintf "fkafka (%s)" AssemblyVersionInformation.AssemblyVersion)
                        "kafka.topic", topicValue
                        "message_bus.destination", topicValue
                        "span.kind", "producer"
                    ]
                else Inactive

            let messageWithProduceTrace =
                { message with Headers = message.Headers |> Trace.inject produceTrace}
                |> MessageToProduce.createKafkaMessage

            (producer.KafkaProducer |> KafkaProducer.value).Produce(topicValue, messageWithProduceTrace, fun delivery ->
                let traceError error =
                    produceTrace
                    |> Trace.addError (error |> TracedError.ofError ProduceError.format)
                    |> ignore

                if delivery.Error.IsBrokerError then traceError (BrokerError (delivery.Error.Code, delivery.Error.Reason))
                elif delivery.Error.IsLocalError then traceError (LocalError (delivery.Error.Code, delivery.Error.Reason))
                elif delivery.Error.IsFatal then traceError (FatalError (delivery.Error.Code, delivery.Error.Reason))
                elif delivery.Error.IsError then traceError (RuntimeError (delivery.Error.Code, delivery.Error.Reason))

                produceTrace
                |> Trace.addTags [
                    "kafka.partition", string delivery.Partition.Value
                    "kafka.offset", string delivery.Offset.Value
                ]
                |> Trace.finish
            )

    // Produce message only

    let produce producer message =
        message
        |> Produce.messageWith producer

    let produceSingle producer message =
        message |> produce producer
        producer |> flush

    // Produce message with trace

    let produceWithTrace producer trace (message: MessageToProduce) =
        { message with Headers = message.Headers |> Trace.inject trace}
        |> produce producer

    let produceSingleWithTrace producer trace message =
        message |> produceWithTrace producer trace
        producer |> flush
