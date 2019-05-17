# Changelog

<!-- There is always Unreleased section on the top. Subsections (Add, Changed, Fix, Removed) should be Add as needed. -->
## Unreleased
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
