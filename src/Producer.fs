namespace Kafka

module Producer =
    open System
    open System.Collections.Concurrent
    open Confluent.Kafka

    let private tee f a =
        f a
        a

    type private Producer = IProducer<Null, string>
    type private Message = Message<Null, string>

    let createProducer (BrokerList brokerList): Producer =
        let config =
            ProducerConfig(
                BootstrapServers = brokerList
            )
        ProducerBuilder(config).Build()

    let private createMessage message =
        Message(
            Value = message
        )

    let produce (configuration: ConnectionConfiguration) messages =
        use producer = createProducer configuration.BrokerList

        messages
        |> List.iter (fun message ->
            producer.Produce(configuration.Topic |> StreamName.value, message |> createMessage)
        )

        producer.Flush(TimeSpan.FromSeconds(10.0))
        |> ignore

    let produceMessage (producer: Producer) (StreamName topic) message =
        producer.Produce(topic, message |> createMessage)

    let private flush (producer: Producer) =
        producer.Flush()

    let produceSingleMessage (producer: Producer) topic message =
        producer
        |> (tee (fun producer -> produceMessage producer topic message))
        |> flush

    let private batch = new ConcurrentQueue<string>()

    let produceBatch current (producer: Producer) topic batchSize maxForBatching message =
        if current() > maxForBatching then
            produceMessage producer topic message
        else
            batch.Enqueue message

            if batch.Count = batchSize then
                let batch' =
                    batch.ToArray()
                    |> List.ofArray
                batch.Clear()

                batch'
                |> Seq.iter (produceMessage producer topic)
                flush producer
