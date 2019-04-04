# Changelog

<!-- There is always Unreleased section on the top. Subsections (Add, Changed, Fix, Removed) should be Add as needed. -->
## Unreleased
- [**BC**] Add more common types
    - `BrokerList`
    - `StreamName`
    - `GroupId`

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
