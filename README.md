# durable-execution

A durable execution engine for running tasks durably and resiliently.

Tasks can range from being a simple function to a complex workflow. The tasks are resilient to
logic failures, process failures, network connectivity issues, and other transient errors. The
tasks logic should be idempotent as they may be executed multiple times if there is a process
failure or if the task is retried.

The main package is [durable-execution](durable-execution). The documentation is available at
[https://gpahal.github.io/durable-execution](https://gpahal.github.io/durable-execution).

## Other packages

- [storage-drizzle](storage-drizzle): A storage implementation for durable-execution using Drizzle
  ORM

## License

This project is licensed under the MIT License. See the
[LICENSE](https://github.com/gpahal/durable-execution/blob/main/LICENSE) file for details.
