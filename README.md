# retryq

Simple Go library for dead-letter queue retry logic with pluggable backoff strategies.

## Installation

```bash
go get github.com/yourusername/retryq
```

## Usage

```go
package main

import (
    "fmt"
    "time"
    "github.com/yourusername/retryq"
)

func main() {
    // Create a new retry queue with exponential backoff
    rq := retryq.New(
        retryq.WithMaxRetries(5),
        retryq.WithBackoff(retryq.ExponentialBackoff(500*time.Millisecond)),
    )

    // Enqueue a job for retry
    err := rq.Enqueue("job-123", func() error {
        // your processing logic here
        return processMessage("job-123")
    })

    if err != nil {
        fmt.Println("Job moved to dead-letter queue:", err)
    }

    // Start processing
    rq.Start()
    defer rq.Stop()
}
```

### Available Backoff Strategies

| Strategy | Description |
|---|---|
| `ExponentialBackoff` | Doubles delay on each retry |
| `LinearBackoff` | Adds a fixed delay on each retry |
| `ConstantBackoff` | Uses a fixed delay between all retries |

## Features

- Pluggable backoff strategies
- Configurable max retry attempts
- Automatic dead-letter queue routing on exhaustion
- Concurrency-safe job processing

## License

MIT © [yourusername](https://github.com/yourusername)