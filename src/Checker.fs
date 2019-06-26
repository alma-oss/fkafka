namespace Kafka

open FSharp.Control

type Checker = {
    WaitForResourceDefault: int<Second>
    MaxRetries: int<Attempt>
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
            WaitForResourceDefault = 1<Second>
            MaxRetries = 10<Attempt>
            CheckCluster = checkCluster
            CheckTopic = checkTopic
        }

type IntervalChecker = {
    CheckClusterInInterval: Confluent.Kafka.Handle -> AsyncSeq<bool>
    ClusterHandler: bool -> unit
    CheckTopicInInterval: StreamName -> Confluent.Kafka.Handle -> AsyncSeq<bool>
    TopicHandler: StreamName -> bool -> unit
}

module IntervalChecker =
    let private checkInInterval: int<Second> -> (unit -> bool) -> AsyncSeq<bool> =
        fun interval handler ->
            asyncSeq {
                let waitMilliseconds = (int interval) * 1000

                while true do
                    yield handler()

                    do! Async.Sleep waitMilliseconds
            }

    let checkClusterInInterval interval handle =
        fun () -> Checker.checkCluster handle
        |> checkInInterval interval

    let checkTopicInInterval interval topic handle =
        fun () -> Checker.checkTopic topic handle
        |> checkInInterval interval

    let defaultChecker =
        {
            CheckClusterInInterval = checkClusterInInterval 60<Second>
            ClusterHandler = ignore
            CheckTopicInInterval = checkTopicInInterval 60<Second>
            TopicHandler = fun _ -> ignore
        }

    let empty =
        {
            CheckClusterInInterval = fun _ -> AsyncSeq.empty
            ClusterHandler = ignore
            CheckTopicInInterval = fun _ _ -> AsyncSeq.empty
            TopicHandler = fun _ -> ignore
        }
