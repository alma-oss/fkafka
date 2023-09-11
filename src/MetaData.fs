namespace Alma.Kafka

module MetaData =
    open System
    open Alma.ServiceIdentification

    type NotParsed = NotParsed

    type CreatedAt = CreatedAt of DateTime

    [<RequireQualifiedAccess>]
    module CreatedAt =
        let value (CreatedAt date) = date
        let now () = CreatedAt (DateTime.Now)

    type GitCommit = GitCommit of string

    [<RequireQualifiedAccess>]
    module GitCommit =
        let value (GitCommit value) = value

    type DockerImageVersion = DockerImageVersion of string

    [<RequireQualifiedAccess>]
    module DockerImageVersion =
        let value (DockerImageVersion value) = value

    type ProcessedBy = {
        Instance: Instance
        Commit: GitCommit
        ImageVersion: DockerImageVersion
    }

    type MetaDataOnlyCreatedAt = MetaDataOnlyCreatedAt of CreatedAt
    type MetaDataCreatedAndProcessed = MetaDataCreatedAndProcessed of CreatedAt * ProcessedBy

    type MetaData =
        | OnlyCreatedAt of MetaDataOnlyCreatedAt
        | CreatedAndProcessed of MetaDataCreatedAndProcessed

    [<RequireQualifiedAccess>]
    type MetaDataParseError =
        | InvalidSchema of data: string * message: string

    [<RequireQualifiedAccess>]
    module MetaDataParseError =
        let format = function
            | MetaDataParseError.InvalidSchema (data, message) -> sprintf "MetaData are invalid - %s.\n%A" message data

    module private Parser =
        open FSharp.Data
        open Alma.Kafka

        type private MetaDataSchema = JsonProvider<"src/schema/metaData.json", SampleIsList = true>

        let parse metaDataJsonValue =
            try
                let parsedMetaData = metaDataJsonValue |> RawData.toJson |> MetaDataSchema.Parse
                let createdAt = CreatedAt parsedMetaData.CreatedAt.DateTime

                match parsedMetaData.ProcessedBy with
                | Some processedBy ->
                    match processedBy.Instance |> Instance.parse "-" with
                    | Some instance ->
                        let processedBy = {
                            Instance = instance
                            Commit = GitCommit processedBy.Commit
                            ImageVersion = DockerImageVersion processedBy.ImageVersion
                        }

                        CreatedAndProcessed (MetaDataCreatedAndProcessed (createdAt, processedBy))
                    | _ -> OnlyCreatedAt (MetaDataOnlyCreatedAt createdAt)
                | _ -> OnlyCreatedAt (MetaDataOnlyCreatedAt createdAt)
                |> Ok
            with
            | error -> Error (MetaDataParseError.InvalidSchema (metaDataJsonValue.ToString(), error.Message))

    [<RequireQualifiedAccess>]
    module MetaData =
        let createdAt = function
            | OnlyCreatedAt (MetaDataOnlyCreatedAt createdAt) -> createdAt
            | CreatedAndProcessed (MetaDataCreatedAndProcessed (createdAt, _)) -> createdAt

        let parse = Parser.parse

        let forProcessedEvent processedBy =
            CreatedAndProcessed (MetaDataCreatedAndProcessed (CreatedAt.now(), processedBy))

    [<RequireQualifiedAccess>]
    module MetaDataDto =
        open Alma.Serializer

        type OnlyCreatedAt = {
            CreatedAt: string
        }

        type ProcessedByDto = {
            Instance: string
            Commit: string
            ImageVersion: string
        }

        type CreatedAtAndProcessedBy = {
            CreatedAt: string
            ProcessedBy: ProcessedByDto
        }

        let fromCreatedAt (CreatedAt createdAt) =
            {
                CreatedAt = createdAt |> Serialize.dateTime
            }

        let fromMetaDataCreatedAt (MetaDataOnlyCreatedAt createdAt) =
            createdAt |> fromCreatedAt

        let fromProcessed ((CreatedAt createdAt), processedBy: ProcessedBy): CreatedAtAndProcessedBy =
            {
                CreatedAt = createdAt |> Serialize.dateTime
                ProcessedBy = {
                    Instance = processedBy.Instance |> Instance.concat "-"
                    Commit = processedBy.Commit |> GitCommit.value
                    ImageVersion = processedBy.ImageVersion |> DockerImageVersion.value
                }
            }

        let fromMetaProcessed (MetaDataCreatedAndProcessed (createdAt, processed)) =
            (createdAt, processed) |> fromProcessed
