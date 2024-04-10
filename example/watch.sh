#!/usr/bin/env bash

set -e

# first run: rpk container start -n 3
# update the brokers and admin hosts by: rpk cluster info

export RPK_BROKERS="127.0.0.1:19092"
export RPK_ADMIN_HOSTS="127.0.0.1:19644"
rpk cluster info
rpk cluster health

#echo "---"
#echo "Create topic"
#rpk topic create development-local-experimental-v1 -p 10

# Service identification
DOMAIN=consents
CONTEXT=fkafka
PURPOSE=example
VERSION=local
ENVIRONMENT=dev1-services

# Logging common
LOG_TO="console"
VERBOSITY=vvv

# Tracing specific
export TRACING_THRIFT_HOST="tracing-thrift.service.$ENVIRONMENT.consul:80"
export TRACING_SERVICE_NAME="$DOMAIN-$CONTEXT"
export TRACING_TAGS="svc_domain=$DOMAIN,svc_context=$CONTEXT,svc_purpose=$PURPOSE,svc_version=$VERSION"

# Tracing common
export TRACING_LOG_TO="$LOG_TO"
export TRACING_LOG_META="$LOGGER_TAGS"
export TRACING_LOG_LEVEL="$VERBOSITY"

dotnet watch run
