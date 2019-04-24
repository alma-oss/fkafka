namespace Kafka

module Admin =
    open System
    open Confluent.Kafka

    type AdminClient = IAdminClient

    let createAdmin (BrokerList brokerList) =
        AdminClientBuilder(
            AdminClientConfig(
                BootstrapServers = brokerList
            )
        ).Build()

    let createAdminFromHandle (handle: Handle) =
        DependentAdminClientBuilder(handle).Build()

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
