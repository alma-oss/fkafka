// Learn more about F# at http://fsharp.org

open System
open Confluent.Kafka
open MF.ConsoleStyle
open Kafka.Admin
open System
open Kafka
open Confluent.Kafka

type private Consumer = Consumer<Ignore, string>

let private createConsumer brokerList (topic: string) groupId: Consumer =
    let config = ConsumerConfig()
    config.GroupId <- groupId
    config.BootstrapServers <- brokerList
    config.AutoOffsetReset <- AutoOffsetResetType.Earliest |> Nullable
    config.EnableAutoCommit <- false |> Nullable

    let consumer = new Consumer<Ignore, string>(config)
    consumer.Subscribe topic

    consumer

[<EntryPoint>]
let main argv =
    Console.title "Get current offset"
    //let groupId = "consumer-group-id-x1"
    //let brokerList = "kfall-1.dev1.services.lmc:9092"
    //let topic = "consumer-offset-test"

    // DEVEL1 - interactions/aggregator
    //let groupId = "consents-intentStreamAggregator-common-stable"
    //let brokerList = "kfall-1.devel1.services.lmc:9092,kfall-2.devel1.services.lmc:9092"
    //let topic = "consents-interactionStream-integration-v1"

    // DEV1 - interactions/aggregator
    let groupId = "consents-intentStreamDeriver-common-stable"
    let brokerList = "kfall-1.dev1.services.lmc:9092"
    let topic = "consents-interactionStream-development-v1"
    // DEV1 - intents/deriver
    //let groupId = "consents-intentStreamAggregator-common-stable"
    //let brokerList = "kfall-1.dev1.services.lmc:9092"
    //let topic = "consents-intentStream-development-v1"

    // PROD - interactions/aggregator
    //let groupId = "consents-intentStreamAggregator-common-stable"
    //let brokerList = "kfall-21.prod.services.lmc:9092,kfall-31.prod.services.lmc:9092,kfall-41.prod.services.lmc:9092"
    //let topic = "consents-interactionStream-prod-v1"
    // PROD - intents/deriver
    //let groupId = "consents-intentStreamAggregator-common-stable"
    //let brokerList = "kfall-21.prod.services.lmc:9092,kfall-31.prod.services.lmc:9092,kfall-41.prod.services.lmc:9092"
    //let topic = "consents-interactionStream-prod-v1"

    let config = ConsumerConfiguration.createWithDefaults (BrokerList brokerList) (StreamName topic) (GroupId.Random)

    Console.options "Configuration" [
        //("groupId", groupId)
        ("brokerList", brokerList)
        ("topic", topic)
    ]

    Console.message "Connecting ..."
    //use consumer = createConsumer brokerList topic groupId
    //use admin = createAdminFromHandle consumer.Handle

    //let topicPartition = TopicPartition(topic, Partition())
    //let timeout = TimeSpan.FromSeconds 5.0

    //admin.ListGroups(timeout) |> printfn "Groups: %A"
    //admin.GetMetadata(topic, timeout) |> printfn "Meta: %A"

    //admin.QueryWatermarkOffsets(topicPartition, timeout) |> printfn "Query: %A"

    //admin.GetWatermarkOffsets(topicPartition) |> printfn "GetCached: %A"

    //let lastCorrelationId = Guid("d3a1321c-a1a5-4ce1-aeb8-3cbc619dc7a9")    // prod  - vytazeno z intent streamu (consent_acquired)
    let lastCorrelationId = Guid("fd6c2baa-16a9-4131-b840-2b9f144712f4")    // dev1

    // todo
    // - aggregator si to precte z intentStreamu a najde posledni constent_[acquired|lost] a jeji correlation_id
    // - podle correlation id najde posledni interakci v interactionStreamu (a jeji offset) - tohle bude misto consumeToOffset (consumeMessage |> Seq.takeWhile)
    // - po tom, co se tohle udela, se producne systemova udalost "service_restarted"
    // - pak se zacne dal cist

    let isCorrelationId (message: Consumer.Message) =
        message.Value
        |> RawEvent.parse
        |> fun {CorrelationId = (CorrelationId correlationId)} -> correlationId <> lastCorrelationId

    Console.message "Start consuming ..."

    // todo - takeWhile !uzJsemPrecetlCorrelationId || isCorrelationId
    // - takeWhile cte pokud vraci true, pokud vrati false, tak hned breakne

    let tee f a =
        f a
        a

    Consumer.consumeMessages config
    //|> Seq.filter isCorrelationId
    //|> Seq.take 1
    |> Seq.map (tee (printfn "Consume: %A\n"))
    |> Seq.takeWhile isCorrelationId
    |> Seq.iter ignore

    //try
    //    consumer.Consume(timeout)
    //    |> (fun result ->
    //        if isNull result then
    //            (null, None)
    //        else
    //            (result.Value, result.Offset.Value |> Some)
    //    )
    //    |> Console.messagef "Current: %A"
    //finally
    //    //consumer.Close()
    //    Console.success "Done"

    //admin.GetWatermarkOffsets(topicPartition) |> printfn "GetCached: %A"

    0 // return an integer exit code
