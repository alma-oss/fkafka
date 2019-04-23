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

    let createAdminFromHandle (handle: Handle) =
        new AdminClient(handle)

    let getAllTopics (admin: AdminClient) =
        admin.GetMetadata(TimeSpan.FromSeconds 5.0)
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

    let isUp (admin: AdminClient) =
        try
            admin.GetMetadata(TimeSpan.FromSeconds 5.0)
            |> fun metadata ->
                metadata.Brokers
                |> Seq.isEmpty
                |> not
        with
        | :? KafkaException -> false
