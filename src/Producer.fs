namespace Kafka

type ProducerConfiguration = {
    Connection: ConnectionConfiguration
    Logger: Logger option
    Checker: Checker option
    MarkAsDisabled: MarkAsDisabled option
}

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

module Producer =
    open Confluent.Kafka

    //
    // Producer
    //

    type Producer = IProducer<Null, string>
    type private Message = Message<Null, string>

    type TopicProducer = {
        Producer: Producer
        Topic: StreamName
    }

    module private Producer =
        let createProducer (BrokerList brokerList): Producer =
            let config =
                ProducerConfig(
                    BootstrapServers = brokerList
                )
            ProducerBuilder(config).Build()

        let private createTopicProducer (configuration: ProducerConfiguration): TopicProducer =
            {
                Producer = configuration.Connection.BrokerList |> createProducer
                Topic = configuration.Connection.Topic
            }

        let private createProducerWithChecker checker (configuration: ProducerConfiguration): TopicProducer =
            let maxRetries = checker.MaxRetries
            let log = configuration.Logger |> Logger.resolve
            let markAsDisabled = configuration.MarkAsDisabled |> ServiceStatus.resolveMarkAsDisabled

            let mutable attempt = 1<attempt>
            let mutable waitForResource = checker.WaitForResourceDefault

            let producer = createProducer configuration.Connection.BrokerList

            seq {
                while attempt <= maxRetries do
                    match checker.CheckCluster producer.Handle, checker.CheckTopic configuration.Connection.Topic producer.Handle with
                    | true, true ->
                        yield { Producer = producer; Topic = configuration.Connection.Topic }
                    | _ ->
                        let (currentAttempt, waitFor) = MarkAsDisabled.executeAndWait log attempt maxRetries markAsDisabled waitForResource

                        attempt <- currentAttempt
                        waitForResource <- waitFor
            }
            |> Seq.take 1
            |> Seq.head

        let create configuration =
            match configuration.Checker with
            | Some checker -> createProducerWithChecker checker configuration
            | _ -> createTopicProducer configuration

        let flush (producer: Producer) =
            producer.Flush()
        
        let close (producer: Producer) =
            producer.Dispose()

    let createProducer = Producer.create
    let createUniversalProducer = Producer.createProducer

    let flush = Producer.flush
    let close = Producer.close

    module TopicProducer =
        let flush { Producer = producer } = producer |> flush
        let close { Producer = producer } = producer |> (tee Producer.flush) |> close

    //
    // Produce messages
    //

    let private createMessage message =
        Message(
            Value = message
        )

    let private produceMessageTo (producer: Producer) (StreamName topic) (message: Message) =
        producer.Produce(topic, message)

    let produce producer message =
        message
        |> createMessage
        |> produceMessageTo producer.Producer producer.Topic

    let produceSingle producer message =
        message |> produce producer
        producer |> TopicProducer.flush

    let produceTo (producer: Producer) topic message =
        message
        |> createMessage
        |> produceMessageTo producer topic
