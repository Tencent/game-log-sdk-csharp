# Repository Guidelines

## Project Structure & Module Organization

This repository contains a .NET 8 C# SDK for TGLog.

- `TglogSdk.sln` is the Visual Studio solution.
- `src/TglogSdk/` contains the core library under the `Tencent.Tglog` namespace.
- `examples/TglogSdk.Examples/` contains runnable usage examples.
- `integration-test/TglogSdk.IntegrationTest/` contains an executable integration/performance test harness.
- `README.md`, `TUTORIAL.md`, and `CHANGELOG.md` document usage, examples, and release changes. Chinese versions use the `_CN.md` suffix.

## Build, Test, and Development Commands

- `dotnet restore TglogSdk.sln` restores NuGet dependencies.
- `dotnet build TglogSdk.sln` builds the SDK, examples, and integration test projects.
- `dotnet run --project examples/TglogSdk.Examples -- 127.0.0.1 20001` runs the sample client against a TGLog endpoint.
- `dotnet run --project integration-test/TglogSdk.IntegrationTest -- --help` checks available integration test options.
- `dotnet run --project integration-test/TglogSdk.IntegrationTest -- 127.0.0.1 20001` runs the integration/performance harness against a local server.

Integration commands require a reachable TGLog server; use `SETUP_SERVER.md` or `SETUP_SERVER_EN.md` when preparing one locally.

## Coding Style & Naming Conventions

Use C# 10 targeting `net8.0`, with nullable reference types and implicit usings enabled. Follow the existing style: 4-space indentation, file-scoped namespaces, PascalCase for public types and methods, camelCase for local variables and parameters, and `_camelCase` for private fields. Keep public APIs in the `Tencent.Tglog` namespace and add XML documentation for user-facing types or methods. Prefer existing async, batching, metrics, and logging patterns before introducing new abstractions.

## Testing Guidelines

There is no dedicated unit test framework in the solution today. Validate changes with `dotnet build TglogSdk.sln` at minimum, then run the integration test when behavior touches networking, batching, retries, callbacks, DNS updates, or metrics. Name new test or harness files after the behavior under test, for example `TglogClientTest.cs` or `EndpointManagerTest.cs`.

## Commit & Pull Request Guidelines

Recent commits use concise Conventional Commit-style prefixes such as `fix(worker): ...`, `refactor: ...`, `docs(worker): ...`, and `chore: ...`. Keep messages imperative and scoped when useful. Pull requests should describe behavior changes, list validation commands run, note any server/config assumptions, and link related issues. Include screenshots only for documentation or tooling changes with visible output.
