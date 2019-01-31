F-Kafka
=======

Library for reading messages or producing messages to/from stream.

## Release
1. Increment version in `src/Kafka.fsproj`
2. Run `$ fake build target release`
3. Move Kafka package (`Kafka.VERSION.nupkg`) from `./release` dir to the NugetServer packages dir
4. Update `CHANGELOG.md`

## Development
### Requirements
- [dotnet core](https://dotnet.microsoft.com/learn/dotnet/hello-world-tutorial)
- [FAKE](https://fake.build/fake-gettingstarted.html)

### Build
```bash
fake build
```

### Watch
```bash
fake build target watch
```
