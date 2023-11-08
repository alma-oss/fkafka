namespace Alma.Kafka

module Admin =
    open System
    open Confluent.Kafka
    open Microsoft.Extensions.Logging
    open Alma.ErrorHandling

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
        |> List.map StreamName.value
        |> List.contains (topic |> StreamName.value)

    let isUp (admin: AdminClient) =
        try
            admin.GetMetadata(TimeSpan.FromSeconds 5.0)
            |> fun metadata ->
                metadata.Brokers
                |> Seq.isEmpty
                |> not
        with
        | :? KafkaException -> false

    type PartitionLag = {
        Partition: int
        Lag: int64
    }

    [<RequireQualifiedAccess>]
    module PartitionLag =
        let lag { Lag = lag } = lag

    let private getTopicMetadata (logger: ILogger) timeout { BrokerList = brokerList; Topic = topic } =
        logger.LogDebug("Connecting to Kafka.admin")
        use admin = createAdmin brokerList

        logger.LogDebug("Getting topic metadata")
        let meta = admin.GetMetadata(timeout)

        [
            let topicMeta =
                meta.Topics
                |> Seq.find (fun t -> t.Topic = (topic |> StreamName.value))

            yield!
                topicMeta.Partitions
                |> Seq.map (fun p -> TopicPartition(topic |> StreamName.value, p.PartitionId))
        ]

    let lags (logger: ILogger) connection groupId = async {
        let timeout = TimeSpan.FromSeconds 5.0

        let config =
            ConsumerConfig(
                GroupId = (groupId |> GroupId.value),
                BootstrapServers = (connection.BrokerList |> BrokerList.value),
                AutoOffsetReset = (AutoOffsetReset.Earliest |> Nullable),
                EnableAutoCommit = false
            )

        logger.LogDebug("Connecting to Kafka")
        use consumer: IConsumer<Ignore, string> = ConsumerBuilder(config).Build()
        let tps = connection |> getTopicMetadata logger timeout

        consumer.Assign(tps)

        return
            consumer.Committed(timeout)
            |> Seq.choose(fun tpo ->
                try
                    let tp = tpo.TopicPartition
                    let watermark = consumer.QueryWatermarkOffsets(tp, timeout)
                    let committed =
                        match tpo.Offset with
                        | value when not value.IsSpecial -> value.Value
                        | _ -> 0L

                    let logEndOffset =
                        match watermark.High with
                        | value when not value.IsSpecial -> value.Value
                        | _ -> 0L

                    let lag = logEndOffset - committed

                    logger.LogDebug(
                        "Committed offset for Topic {topic} Partition {partition} is {committed} out of watermark end offset {logEndOffset} Lag is: {lag}",
                        tp.Topic,
                        tp.Partition.Value,
                        committed,
                        logEndOffset,
                        lag
                    )

                    Some { Partition = tpo.TopicPartition.Partition.Value; Lag = lag }
                with _ -> None
            )
            |> Seq.toList
    }
