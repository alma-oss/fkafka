#load ".fake/build.fsx/intellisense.fsx"
open Fake.Core
open Fake.DotNet
open Fake.IO
open Fake.IO.FileSystemOperators
open Fake.IO.Globbing.Operators
open Fake.Core.TargetOperators

let runDotNet cmd workingDir =
    let result =
        DotNet.exec (DotNet.Options.withWorkingDirectory workingDir) cmd ""
    if result.ExitCode <> 0 then failwithf "'dotnet %s' failed in %s" cmd workingDir

let tee f a =
    f a
    a

let sourceDir = "src"

let nugetServer = "http://development-nugetserver-common-stable.service.devel1-services.consul:31794"
let apiKey = "123456"

Target.create "Clean" (fun _ ->
    !! "src/bin"
    ++ "src/obj"
    |> Shell.cleanDirs
)

Target.create "Build" (fun _ ->
    !! "src/*.*proj"
    |> Seq.iter (DotNet.build id)
)

Target.create "Release" (fun _ ->
    runDotNet "pack" sourceDir

    let pushToNuget path =
        sourceDir
        |> runDotNet (sprintf "nuget push %s -s %s -k %s" path nugetServer apiKey)

    !! "src/**/bin/**/*.nupkg"
    |> Seq.iter (fun path ->
        path
        |> tee pushToNuget
        |> Shell.moveFile "release"
    )
)

Target.create "Watch" (fun _ ->
    runDotNet "watch run" sourceDir
)

"Clean"
    ==> "Build"
    ==> "Release"

"Build"
    ==> "Watch"

Target.runOrDefault "Build"
