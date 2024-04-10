#!/usr/bin/env bash

set -e

export RPK_BROKERS="host.docker.internal:49834,host.docker.internal:49839,host.docker.internal:49844"
export RPK_ADMIN_HOSTS="host.docker.internal:49833,host.docker.internal:49843,host.docker.internal:49836"
rpk cluster info
rpk cluster health

# change ports according to a local cluster (ports for kafka - in RPK_BROKERS)

# see https://github.com/redpanda-data/console
docker run -p 8000:8080 -e KAFKA_BROKERS="host.docker.internal:49834,host.docker.internal:49839,host.docker.internal:49844" docker.redpanda.com/redpandadata/console:latest
