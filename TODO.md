### Code Review 01

1. fix reader abstraction 

```go
// TODO: iterator design pattern
// https://refactoring.guru/design-patterns/iterator
// https://refactoring.guru/design-patterns/iterator/go/example
```

2. use slog.Logger and refactor all logging with structred logs
    1. implement a logger package with singleton pattern

```go
// TODO: use structured logging with
var logger *slog.Logger
logger.Debug("mongorespotiory.Connect", slog.String("error", err.Error()))

```