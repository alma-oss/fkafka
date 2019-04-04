namespace Kafka

module Admin =
    open System
    open Confluent.Kafka

    let createAdmin (BrokerList brokerList) =
        new AdminClient(
            AdminClientConfig(
                BootstrapServers = brokerList
            )
        )

    let createAdminFromConsumer configuration groupId =
        use consumer = Consumer.createConsumer configuration.BrokerList configuration.Topic groupId
        new AdminClient(consumer.Handle)

    let getAllTopics (admin: AdminClient) =
        admin.GetMetadata(TimeSpan.FromSeconds 10.0)
        |> fun metadata ->
            metadata.Topics
            |> Seq.choose (fun topic ->
                if topic.Error.IsError then None
                else Some topic.Topic
            )
            |> Seq.map StreamName
            |> List.ofSeq

    let topicExists admin topic =
        admin
        |> getAllTopics
        |> List.contains topic
