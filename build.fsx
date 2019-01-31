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

let sourceDir = "src"

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

    !! "src/**/bin/**/*.nupkg"
    |> Seq.iter (Shell.moveFile "release")
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
