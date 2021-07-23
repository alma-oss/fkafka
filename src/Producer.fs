namespace Lmc.Kafka

open System
open Lmc.Metrics.ServiceStatus

type ProducerConfiguration = {
    Connection: ConnectionConfiguration
    Logger: Logger option
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
// Producer
//

[<RequireQualifiedAccess>]
module Producer =
    open Confluent.Kafka

    /// Partition used for producing - since we don't use them yet, it is always a default one - 0
    let [<Literal>] private DefaultPartition = 0

    //
    // Producer
    //

    type KafkaProducer = IProducer<Null, string>
    type private KafkaMessage = Message<Null, string>

    type TopicProducer =
        {
            KafkaProducer: KafkaProducer
            Topic: StreamName
            Partition: int
        }

        member this.Flush() =
            this.KafkaProducer.Flush()

        member this.Close() =
            this.Flush()
            this.KafkaProducer.Dispose()

        interface IDisposable with
            member this.Dispose() =
                this.Close()

    type NotConnectedProducer = private NotConnectedProducer of (unit -> TopicProducer)

    module private Producer =
        let createProducer (BrokerList brokerList): KafkaProducer =
            let config =
                ProducerConfig(
                    BootstrapServers = brokerList
                )
            ProducerBuilder(config).Build()

        let private createTopicProducer (configuration: ProducerConfiguration): TopicProducer =
            {
                KafkaProducer = configuration.Connection.BrokerList |> createProducer
                Topic = configuration.Connection.Topic
                Partition = DefaultPartition
            }

        let private createProducerWithChecker checker (configuration: ProducerConfiguration): TopicProducer =
            let maxRetries = checker.MaxRetries
            let log = configuration.Logger |> Logger.resolve
            let markAsDisabled = configuration.MarkAsDisabled |> ServiceStatus.resolveMarkAsDisabled

            let mutable attempt = 1<Attempt>
            let mutable waitForResource = checker.WaitForResourceDefault

            let producer = createProducer configuration.Connection.BrokerList

            seq {
                while attempt <= maxRetries do
                    match checker.CheckCluster producer.Handle, checker.CheckTopic configuration.Connection.Topic producer.Handle with
                    | true, true ->
                        yield { KafkaProducer = producer; Topic = configuration.Connection.Topic; Partition = DefaultPartition }
                    | _ ->
                        let (currentAttempt, waitFor) = MarkAsDisabled.executeAndWait log attempt maxRetries markAsDisabled waitForResource

                        attempt <- currentAttempt
                        waitForResource <- waitFor
            }
            |> tee (fun producers ->
                if producers |> Seq.isEmpty then
                    failwithf "There is no connected producer. Problem is with either %A and/or a %A." configuration.Connection.BrokerList configuration.Connection.Topic
            )
            |> Seq.head

        let create configuration =
            match configuration.Checker with
            | Some checker -> createProducerWithChecker checker configuration
            | _ -> createTopicProducer configuration

        let flush (producer: KafkaProducer) =
            producer.Flush()

        let close (producer: KafkaProducer) =
            producer.Dispose()

    //
    // Public Producer functions
    //

    let createProducer = Producer.create
    let createUniversalProducer = Producer.createProducer

    let prepareProducer configuration = NotConnectedProducer (fun () -> createProducer configuration)
    let connect (NotConnectedProducer create) = create()

    [<RequireQualifiedAccess>]
    module TopicProducer =
        let flush (producer: TopicProducer) = producer.Flush()
        let close (producer: TopicProducer) = producer.Close()

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
        open Lmc.Tracing

        let messageWith (producer: TopicProducer) (message: KafkaMessage) =
            let topicValue = producer.Topic |> StreamName.value

            use __ =
                "Produce event"
                |> Trace.ChildOf.continueOrStartActiveFromActive
                |> Trace.addTags [
                    "peer.service", "kafka"
                    "component:", (sprintf "fkafka (%s)" AssemblyVersionInformation.AssemblyVersion)
                    "kafka.topic", topicValue
                    "message_bus.destination", topicValue
                    "kafka.partition", string producer.Partition
                    "span.kind", "producer"
                ]

            producer.KafkaProducer.Produce(topicValue, message)

    // Produce message only

    let produce producer message =
        message
        |> createKafkaMessage
        |> Produce.messageWith producer

    let produceSingle producer message =
        message |> produce producer
        producer |> TopicProducer.flush

    let produceTo (producer: KafkaProducer) topic =
        produce { KafkaProducer = producer; Topic = topic; Partition = DefaultPartition }

    // Produce message with headers

    let produceWithHeaders producer headers message =
        message
        |> createMessageWithHeaders headers
        |> Produce.messageWith producer

    let produceSingleWithHeaders producer headers message =
        message |> produceWithHeaders producer headers
        producer |> TopicProducer.flush

    let produceWithHeadersTo (producer: KafkaProducer) topic =
        produceWithHeaders { KafkaProducer = producer; Topic = topic; Partition = DefaultPartition }
