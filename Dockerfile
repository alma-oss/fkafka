FROM dcreg.service.consul/prod/development-dotnet-core-sdk-common:latest

# build scripts
COPY ./fake.sh /fkafka/
COPY ./build.fsx /fkafka/
COPY ./paket.dependencies /fkafka/
COPY ./paket.references /fkafka/
COPY ./paket.lock /fkafka/

# sources
COPY ./Kafka.fsproj /fkafka/
COPY ./src /fkafka/src

WORKDIR /fkafka

RUN \
    ./fake.sh build target Build no-clean

CMD ["./fake.sh", "build", "target", "Tests", "no-clean"]
