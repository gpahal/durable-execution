# durable-execution

A durable execution engine for running tasks durably and resiliently.

Tasks can range from being a simple function to a complex workflow. The tasks are resilient to
logic failures, process failures, network connectivity issues, and other transient errors. The
tasks logic should be idempotent as they may be executed multiple times if there is a process
failure or if the task is retried.

The main package is [durable-execution](durable-execution). The documentation is available at
[https://gpahal.github.io/durable-execution](https://gpahal.github.io/durable-execution).

## Other packages

- [durable-execution-orpc-utils](durable-execution-orpc-utils): oRPC utilities for durable
  execution to create a separate server process for durable execution
- [durable-execution-storage-test-utils](durable-execution-storage-test-utils): Test utilities for
  validating durable-execution storage implementations
- [durable-execution-storage-drizzle](durable-execution-storage-drizzle): A storage implementation
  for durable-execution using Drizzle ORM
- [durable-execution-storage-convex](durable-execution-storage-convex): A storage implementation for
  durable-execution using Convex

## License

This project is licensed under the MIT License. See the
[LICENSE](https://github.com/gpahal/durable-execution/blob/main/LICENSE) file for details.
