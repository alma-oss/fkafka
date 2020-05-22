FROM dcreg.service.consul/dev/development-dotnet-core-sdk-common:3.1

# build scripts
COPY ./fake.sh /fkafka/
COPY ./build.fsx /fkafka/
COPY ./paket.dependencies /fkafka/
COPY ./paket.references /fkafka/
COPY ./paket.lock /fkafka/

# sources
COPY ./Kafka.fsproj /fkafka/
COPY ./src /fkafka/src

# others
COPY ./.git /fkafka/.git
COPY ./CHANGELOG.md /fkafka/

WORKDIR /fkafka

RUN \
    ./fake.sh build target Build no-clean

CMD ["./fake.sh", "build", "target", "Tests", "no-clean"]
