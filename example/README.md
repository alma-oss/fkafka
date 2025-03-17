Kafka (redpanda) example
========================

## Run local panda for testing/example
> https://docs.redpanda.com/current/get-started/quick-start/?tab=tabs-1-three-brokers

1. docker compose up
2. docker exec -it redpanda-0 rpk cluster info
3. docker exec -it redpanda-0 rpk topic create XXX -p 10
4. use port from panda starting with `1` - for kafka `19092` for admin `19644`

panda console will run on http://127.0.0.1:8000

## Run example

1. run `watch.sh` script, which will compile example project
2. run `bin/console list` and choose command, you want to run
