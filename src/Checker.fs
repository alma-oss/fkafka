namespace Kafka

type Checker = {
    WaitForResourceDefault: int<second>
    MaxRetries: int<attempt>
    CheckCluster: Confluent.Kafka.Handle -> bool
    CheckTopic: StreamName -> Confluent.Kafka.Handle -> bool
}

module Checker =
    open Confluent.Kafka

    let checkCluster handle =
        try
            use admin = handle |> Admin.createAdminFromHandle
            admin |> Admin.isUp
        with
        | :? KafkaException -> false

    let checkTopic topic handle =
        try
            use admin = handle |> Admin.createAdminFromHandle
            topic |> Admin.topicExists admin
        with
        | :? KafkaException -> false

    let defaultChecker =
        {
            WaitForResourceDefault = 1<second>
            MaxRetries = 10<attempt>
            CheckCluster = checkCluster
            CheckTopic = checkTopic
        }
