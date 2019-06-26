```fs
type Username = Username of string
type Password = Password of string

type SaslPlaintextConfiguration = {
    Username: Username
    Password: Password
}

type Security =
    | Anonymous
    | SaslPlaintext of SaslPlaintextConfiguration

module Consumer =
    let createWithAuthentication =
        //...
        config.SecurityProtocol <- SecurityProtocolType.Sasl_Plaintext |> Nullable
        config.SaslMechanism <- SaslMechanismType.Plain |> Nullable
        config.SaslUsername <- "consentor"
        config.SaslPassword <- "consentor-pwd"
```
