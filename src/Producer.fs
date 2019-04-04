namespace Kafka

module Producer =
    open System
    open System.Collections.Concurrent
    open Confluent.Kafka

    type private Producer = Producer<Null, string>
    type private Message = Message<Null, string>

    let createProducer (BrokerList brokerList) =
        let config =
            ProducerConfig(
                BootstrapServers = brokerList
            )
        new Producer(config)

    let private createMessage message =
        Message(
            Value = message
        )

    let produce configuration messages =
        use producer = createProducer configuration.BrokerList

        messages
        |> List.iter (fun message ->
            producer.BeginProduce(configuration.Topic |> StreamName.value, message |> createMessage)
        )

        producer.Flush(TimeSpan.FromSeconds(10.0))
        |> ignore

    let produceMessage (producer: Producer) (StreamName topic) message =
        producer.BeginProduce(topic, message |> createMessage)

    let private flush (producer: Producer) =
        producer.Flush(TimeSpan.FromSeconds(10.0))
        |> ignore

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
