# Changelog

<!-- There is always Unreleased section on the top. Subsections (Add, Changed, Fix, Removed) should be Add as needed. -->
## Unreleased
- Add `Admin.PartitionLag` type and module
- Add `Admin.lags` function

## 25.0.0 - 2023-09-11
- [**BC**] Use `Alma` namespace

## 24.0.0 - 2023-08-11
- [**BC**] Use net7.0

## 23.2.0 - 2023-06-30
- Update dependencies
- Lock version of `Confluent.Kafka and librdkafka` in 2.0.2
    > There is no issue for it, but since version 2.1.0, first 1 (or 2) consumed messages are not connected correctly. After those, everything works fine.

    Error:
    Confluent.Kafka.ConsumeException: Subscribed topic not available: {topic}: Broker: Unknown topic or partition
      at Confluent.Kafka.Consumer`2.Consume(Int32 millisecondsTimeout)
      at Confluent.Kafka.Consumer`2.Consume(CancellationToken cancellationToken)
      at Lmc.Kafka.Consumer.Consume.consume(Consumer consumer) in /Users/petr.chromec/fsharp/fkafka/src/Consumer.fs:line 332

## 23.1.0 - 2023-01-30
- Update dependencies
    - Use `librdkafka` and `Confluent.Kafka` 2.0.2

## 23.0.1 - 2022-08-29
- Fix counting a lag

## 23.0.0 - 2022-08-26
- [**BC**] Add `CountLag` option for `ConsumerConfiguration`
- [**BC**] Add `Lag` field in `Consumer.Message`
- Update dependencies

## 22.0.0 - 2022-05-17
- Update dependencies
    - [**BC**] Use `OpenTelemetry` tracing

## 21.0.0 - 2022-03-04
- Allow to cancel consuming
    - [**BC**] Add `Cancellation` field to the `Consumer` configuration

## 20.5.0 - 2022-03-02
- Add `ConsumeError.RuntimeError` case

## 20.4.0 - 2022-03-02
- Allow `async` parse
    - Add `ParseEventAsyncResult<'Event>` type
    - Add `ParseEventMessageAsyncResult<'Event>` type
    - Add `Consumer.consumeAsync` function
    - Add `Consumer.consumeMessagesAsync` function

## 20.3.0 - 2022-02-28
- Update dependencies

## 20.2.0 - 2022-02-22
- Update dependencies

## 20.1.2 - 2022-02-18
- Fix custom equality problem with `StreamName` and `GroupId` types which messes with clearing the manual commit state

## 20.1.1 - 2022-02-18
- Remove unwanted function `Consumer.clearLastMessageManuallyCommittedState` and handle the clearing state internally, when consuming starts

## 20.1.0 - 2022-02-17
- Fix `TracedMessageResult` type and module to be `internal` as it was meant
    - Also internally change the error to have a trace as well
- Fix consume results to have a trace with error when consume fails
- Add `Consumer` functions
    - `Consumer.logLastMessageManuallyCommittedState`
    - `Consumer.clearLastMessageManuallyCommittedState`
- Fix trace in produced message to be the `produce event` trace (not its parent)

## 20.0.0 - 2022-01-24
- [**BC**] Use string key in produced message
- Add `MessageKey` type and module
- [**BC**] Add `MessageToProduce` type and module and use it instead of a `string` message in all produce functions
    - Remove functions
        - `produceWithHeaders`
        - `produceSingleWithHeaders`
- [**BC**] Remove partition from a producer runtime information and use a partition from a delivery report
- [**BC**] Remove function for consuming last message
    - `consumeLast`
    - `consumeLastMessage`
- [**BC**] Remove partition from consumer runtime information and use a partition from consumed message
- [**BC**] Add `Partition` field to consumed `Message`
- Trace offset in both producing and consuming message

## 19.0.0 - 2022-01-06
- [**BC**] Use net6.0
- [**BC**] Remove deprecated `configure` in Consumer configuration

## 18.0.0 - 2021-10-29
- [**BC**] Add MetaData types
    - `MetaDataOnlyCreatedAt`
    - `MetaDataCreatedAndProcessed`

## 17.2.0 - 2021-10-21
- Log debug of commit message key, if manual committing is set up

## 17.1.0 - 2021-10-14
- Add MetaData module (*moved from consent-events*)

## 17.0.0 - 2021-10-11
- [**BC**] Consume with error handling
- Fix to work without a tracing
- [**BC**] Remove `Logger` type and use `ILogger` from `Microsoft.Extensions.Logging`
- [**BC**] Add configuration option to set `CommitMessage` *mode*
- [**BC**] Require qualified access for `GroupId`

## 16.2.0 - 2021-09-14
- Update dependencies

## 16.1.0 - 2021-08-03
- Update extracting trace from headers to work with new version of Lmc.Trace

## 16.0.0 - 2021-07-27
- Add `Producer` functions
    - `produceWithTrace`
    - `produceSingleWithTrace`
    - `produceWithTraceTo`
- Fix start time of the consume event trace
- Add runtime groupId information to the consume event tag
- Add `peer.address` tag to consumer and producer trace
- [**BC**] Remove `Producer` functions
    - `produceTo`
    - `produceWithHeadersTo`
    - `produceWithTraceTo`
    - `createUniversalProducer`
- [**BC**] Make `KafkaProducer` internal
- [**BC**] Rename `TopicProducer` type and module to `Producer`
- [**BC**] Make `Producer` attributes internal
- [**BC**] Rename functions
    - `createProducer` to `create`
    - `prepareProducer` to `prepare`

## 15.1.0 - 2021-07-26
- Update dependencies

## 15.0.0 - 2021-07-26
- [**BC**] Remove `Headers` from `Message` type
- [**BC**] Remove `ConsumedMessage`  type and module
- Add `TracedMessage` type and module
- [**BC**] Change `ParseEvent` and `ParseEventMessage` to get `TracedMessage`
- Trace `Consumer.consume` functions automatically

## 14.0.0 - 2021-07-26
- [**BC**] Change `Consumer` module so consumed events have a actual runtime data usable for tracing
    - Add types and modules
        - `ConsumeRuntime` (_contains an actual configuration used for consuming_)
        - `ConsumedEvent<'Event>` (_wrapping an 'Event with a runtime configuration used for consume_)
    - Remove deprecated functions
        - `read`
        - `readToOffset`
    - Remove types and modules
        - `Message`
        - `DecodedMessageReader`
        - `ParsedMessageReader`
        - `MessageReader`
    - Rename type `MessageWithHeaders` to `Message`
    - Remove functions
        - `consumeMessages`
        - `consumeLastMessage`
        - `consumeMessagesWithHeaders`
        - `consumeLastMessageWithHeaders`
    - Change `ParseEvent` signature to get `ConsumedEvent<string>` instead of just `string`
    - Rename type `ParseEventWithHeaders` to `ParseEventMessage`
    - Rename function `consumeWithHeaders` to `consumeMessages`
    - Rename function `consumeLastWithHeaders` to `consumeLastMessage`
- [**BC**] Change `Producer` module to trace all produce functions
    - Rename type `Producer` to `KafkaProducer`
    - Implement `IDisposable` with `TopicProducer`
    - Add `RequiredQualifiedAccess` to `TopicProducer` module
    - Remove functions
        - `Producer.flush` (_use `TopicProducer` module or instance instead_)
        - `Producer.close` (_use `TopicProducer` module or instance instead_)
    - Trace all **produce** functions
        - `produce`
        - `produceSingle`
        - `produceTo`
        - `produceWithHeaders`
        - `produceSingleWithHeaders`
        - `produceWithHeadersTo`
- [**BC**] Remove `RawEvent.messageReader` function

## 13.0.0 - 2021-06-10
- Add types and modules
    - `HeaderKey`
    - `Header`
- Add `Consumer` functions
    - `Consumer.consumeWithHeaders`
    - `Consumer.consumeMessagesWithHeaders`
    - `Consumer.consumeLastMessageWithHeaders`
    - `Consumer.consumeLastWithHeaders`
- Add `Producer` functions
    - `Producer.produceWithHeaders`
    - `Producer.produceSingleWithHeaders`
    - `Producer.produceWithHeadersTo`
- Mark `read` and `readToOffset` functions as deprecated, since `consume` functions are recommended
- [**BC**] Add `RequireQualifiedAccess` to `Consumer` and `Producer` modules

## 12.1.0 - 2021-02-15
- Update dependencies

## 12.0.0 - 2020-11-23
- [**BC**] Use .netcore 5.0

## 11.0.0 - 2020-11-23
- Update dependencies
- [**BC**] Change namespace to `Lmc.Kafka`

## 10.3.0 - 2020-05-29
- Fix `EventId.create` function
- Add  `CorrelationId.fromEventId` function

## 10.2.0 - 2020-05-28
- Add `EventId.create` function

## 10.1.0 - 2020-05-25
- Add `EventDto.serialize` function

## 10.0.0 - 2020-05-25
- [**BC**] Use .netcore 3.1
- [**BC**] Update dependencies
- [**BC**] Change `DTO` format to PascalCase
- Add EventDto subtypes `WithResource` and `WithoutResource`
    - Add `Serialize` types for that
    - [**BC**] Change `Event.toDto` function
    - Add functions:
        - `Event.withResourceToDto`
        - `Event.withoutResourceToDto`

## 9.3.0 - 2019-11-14
- Allow `Lmc.ServiceIdentification` in version `3`

## 9.2.0 - 2019-11-13
- Add `CommonEvent` functions:
    - `schema`
    - `id`
    - `correlationId`
    - `causationId`
    - `timestamp`
    - `eventType`
    - `resource`

## 9.1.0 - 2019-11-12
- Add type/module for `ResourceDto`
- Add function `RawData.toJson`
- Change git host
- Add `AssemblyInfo`
- Add `SerializeEvent` type and `Event.toDto: SerializeEvent` function

## 9.0.0 - 2019-10-22
- [**BC**] Remove function `connectWith`
- [**BC**] Add `Configure` field to `ConsumerConfiguration`, to allow pass an additional configuration.
- [**BC**] Add `RequireQualifiedAccess` attribute to the Modules

## 8.1.0 - 2019-10-22
- Allow to create `Consumer` with `connectWith` function, to allow pass an additional configuration.

## 8.0.0 - 2019-06-26
- Add lint
    - Rename types `second` and `attempt` to _PascalCase_

## 7.1.0 - 2019-06-11
- Add `IntervalChecker` module and type
    - Allow checking kafka cluster and topic in interval and handle their state while consuming

## 7.0.0 - 2019-06-10
- [**BC**] Update dependencies
    - Update `Confluent.Kafka` to the most up to date version `1.0.1`
    - Update `Lmc.Metrics` to allow `ResourceAvailability.Service` and more
- [**BC**] Allow `StreamName` to be defined by `Instance`

## 6.4.0 - 2019-05-29
- Add `CommonEvent.box` function to get whole Box out of a CommonEvent

## 6.3.1 - 2019-05-24
- Fix consume with error handling, to raise a proper exception, when connection could not be established

## 6.3.0 - 2019-05-23
- Improve error when no producer is connected

## 6.2.0 - 2019-05-22
- Add `RawEvent.toCommon` alias function for `Event.toCommon`

## 6.1.0 - 2019-05-22
- Add `CommonEvent` type to allow common operations above all events without any specific data
- Add `Event.toCommon` function to transform a generic `Event` into `CommonEvent`

## 6.0.0 - 2019-05-17
- Add `Consumer` function `consumeLast` to consume last and parse it to the specific Event type
- Make `Producer` type public
- Refactor **Producer**
    - [**BC**] Remove function `produceBatch`
    - [**BC**] Remove function `produce`
    - [**BC**] Rename function `produceMessage` to `produce`
    - [**BC**] Rename function `produceSingleMessage` to `produceSingle`
    - [**BC**] Rename function `createProducer` to `createUniversalProducer`
    - Add `ProducerConfiguration`
    - Add `flush` function to send all remaining messages
    - Add `produceTo` function to keep the previous implementation, with universal producer
    - Add `prepareProducer` function to prepare producer (_lazy create producer_)
    - Add `TopicProducer` type to pack a producer with its topic
    - Add `NotConnectedProducer` type to allow lazy creation and later connection
- [**BC**] Make Checker retries typed as `attempt`

## 5.2.0 - 2019-04-25
- Add `Producer` function `produceSingleMessage` to produce a message and flush right away, so producer can be safely disposed

## 5.1.3 - 2019-04-24
- Fix `RawEvent` function `parse` to correctly parse optional keys

## 5.1.2 - 2019-04-24
- Fix `Consumer` function `consumeLastMessage` to return `None` if the highest offset is `0`

## 5.1.1 - 2019-04-24
- Fix `Consumer` function `consumeLastMessage` to returns `Message option` with `None` if there is no last message

## 5.1.0 - 2019-04-24
- Update `Confluent.Kafka` library to the most up to date version `1.0.0-RC6`
- Add `Consumer` function `consumeLastMessage`, which reads last message of the given topic, by the highest offset

## 5.0.0 - 2019-04-23
- [**BC**] Add more common types
    - `BrokerList`
    - `StreamName`
    - `GroupId`
- [**BC**] Split `Configuration` to common `ConnectionConfiguration` and `ConsumerConfiguration` and add module to simplify creating the `ConsumerConfiguration` record
- Add parameters to the `ConsumerConfiguration`
    - `GroupId` - to explicitly pass group id to the Consumer
    - _optional_ `Logger` - to allow more complex Logger logic in the future
    - _optional_ `Checker` - to allow resource checking in consuming
    - _optional_ `ServiceStatus` - to allow mark service as `enabled`/`disabled`
- **Consumer**
    - [**BC**] Remove `log` parameter of `consumeStream` function, since `Logger` is already in the `Configuration`
    - [**BC**] Rename `consumeStream*` functions which uses `Reader` to `read*`
    - [**BC**] Remove `consumeStreamWithGroupId` function
    - Add `consume` function which generates `sequence` of Events
    - Allow consuming with custom Checker, which is called before consuming to ensure that Kafka cluster and topic is available
        - When resource is not available, consuming will wait until it is available again to resume consuming
    - Allow to mark service as `enabled`/`disabled` when resource availability changes
    - Add `Message` type, `Message` has both the value and offset
    - Add `consumeMessages` function to consume stream as sequence of `Message`s
- **Admin**
    - Add `createAdminFromHandle` function
    - [**BC**] Remove `createAdminFromConsumer` function (_use `createAdminFromHandle` instead_)
    - Add `isUp` function to check, whether a connection is up (_has any active brokers and not throw an exception_)
- Add `Checker` module with
    - common type (_interface_) for Kafka `Checker`
    - basic checking functions for Kafka cluster and topic
    - default `Checker` implementation

## 4.4.0 - 2019-04-04
- Add `Admin` module with simple topic meta information

## 4.3.0 - 2019-03-27
- Fix `consumeStreamToOffset` to read up to max offset (_`- 1`, because kafka counts offset from 0_)
- Add `ConsoleCancelEventHandler` in `consume` stream, to close the consumer

## 4.2.0 - 2019-03-14
- Allow `ServiceIdentifiaction` in version `2.0.0`

## 4.1.0 - 2019-03-13
- Add `CausationId.fromEventId` function

## 4.0.0 - 2019-03-13
- [**BC**] Make event fields type safe
- [**BC**] Add `causation_id` to events

## 3.2.0 - 2019-02-27
- Add `consumeStreamToOffset` function to `Consumer`

## 3.1.0 - 2019-02-08
- Make `produceMessage` function public

## 3.0.0 - 2019-02-07
- Add `consumeStreamWithGroupId` function to `Consumer`
- [**BC**] Remove `incrementMessageCount` parameter of consume stream

## 2.0.0 - 2019-02-06
- [**BC**] Rename `BaseEvent` to `RawEvent`

## 1.0.0 - 2019-01-31
- Initial implementation
