namespace Lmc.Kafka

open System
open Microsoft.Extensions.Logging
open Lmc.Metrics.ServiceStatus
open Lmc.Tracing

type ProducerConfiguration = {
    Connection: ConnectionConfiguration
    Logger: ILogger option
    Checker: Checker option
    MarkAsDisabled: MarkAsDisabled option
}

[<RequireQualifiedAccess>]
module ProducerConfiguration =
    /// Partition used for producing - since we don't use them yet, it is always a default one - 0
    let [<Literal>] internal DefaultPartition = 0

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
// Producer
//

open Confluent.Kafka

type private KafkaProducer = KafkaProducer of IProducer<Null, string>
type private KafkaMessage = Message<Null, string>

[<RequireQualifiedAccess>]
module private KafkaProducer =
    let value (KafkaProducer kafkaProducer) = kafkaProducer

[<Struct>]
type private ProduceRuntime = {
    /// Acutal list of bootstrap servers used for producing
    BootstrapServers: string
    /// Acutal topic used for producing
    Topic: string
    /// Acutal partition used for producing
    Partition: int

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

    let private createKafkaProducer (BrokerList brokerList): KafkaProducer =
        let config =
            ProducerConfig(
                BootstrapServers = brokerList
            )
        ProducerBuilder(config).Build() |> KafkaProducer

    let private createProducer (configuration: ProducerConfiguration): Producer =
        configuration.Logger |> Option.iter (fun logger -> logger.LogDebug("Connecting producer ..."))
        {
            KafkaProducer = configuration.Connection.BrokerList |> createKafkaProducer
            Topic = configuration.Connection.Topic
            Runtime = {
                BootstrapServers = configuration.Connection.BrokerList |> BrokerList.value
                Topic = configuration.Connection.Topic |> StreamName.value
                Partition = ProducerConfiguration.DefaultPartition
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
        let (KafkaProducer producer) = createKafkaProducer configuration.Connection.BrokerList

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
                            Partition = ProducerConfiguration.DefaultPartition
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

    let private createKafkaMessage message =
        KafkaMessage(
            Value = message
        )

    let private createMessageWithHeaders headers message =
        let messageHeaders = Headers()
        headers
        |> List.iter (Header.toKafkaHeader >> messageHeaders.Add)

        KafkaMessage(
            Value = message,
            Headers = messageHeaders
        )

    [<RequireQualifiedAccess>]
    module private Produce =
        let messageWith (producer: Producer) (message: KafkaMessage) =
            let topicValue = producer.Runtime.Topic

            use __ =
                if producer.Runtime.UseTracing then
                    "Produce event"
                    |> Trace.ChildOf.continueOrStart (Trace.extractFromKafkaHeaders message.Headers >> Trace.ofContextOption)
                    |> Trace.addTags [
                        "peer.service", "kafka"
                        "peer.address", producer.Runtime.BootstrapServers
                        "component:", (sprintf "fkafka (%s)" AssemblyVersionInformation.AssemblyVersion)
                        "kafka.topic", topicValue
                        "message_bus.destination", topicValue
                        "kafka.partition", string producer.Runtime.Partition
                        "span.kind", "producer"
                    ]
                else Inactive

            (producer.KafkaProducer |> KafkaProducer.value).Produce(topicValue, message)

    // Produce message only

    let produce producer message =
        message
        |> createKafkaMessage
        |> Produce.messageWith producer

    let produceSingle producer message =
        message |> produce producer
        producer |> flush

    // Produce message with headers

    let produceWithHeaders producer headers message =
        message
        |> createMessageWithHeaders headers
        |> Produce.messageWith producer

    let produceSingleWithHeaders producer headers message =
        message |> produceWithHeaders producer headers
        producer |> flush

    // Produce message with trace

    let produceWithTrace producer trace message =
        message
        |> createMessageWithHeaders (Trace.inject trace [])
        |> Produce.messageWith producer

    let produceSingleWithTrace producer trace message =
        message |> produceWithTrace producer trace
        producer |> flush
