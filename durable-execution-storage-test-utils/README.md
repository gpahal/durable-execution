# durable-execution-storage-test-utils

[![NPM Version](https://img.shields.io/npm/v/durable-execution-storage-test-utils)](https://www.npmjs.com/package/durable-execution-storage-test-utils)
[![License](https://img.shields.io/npm/l/durable-execution-storage-test-utils)](https://github.com/gpahal/durable-execution/blob/main/LICENSE)
[![Coverage](https://img.shields.io/codecov/c/github/gpahal/durable-execution/main?flag=durable-execution-storage-test-utils)](https://codecov.io/gh/gpahal/durable-execution?flag=durable-execution-storage-test-utils)

Test utilities for validating [durable-execution](https://github.com/gpahal/durable-execution)
storage implementations.

## Installation

- npm

```bash
npm install effect durable-execution durable-execution-storage-test-utils
```

- pnpm

```bash
pnpm add effect durable-execution durable-execution-storage-test-utils
```

## Usage

### Testing

The primary use case is validating that your storage implementation correctly handles all aspects
of durable task execution:

```ts
import { describe, test } from 'vitest'
import { runStorageTest } from 'durable-execution-storage-test-utils'
import { MyDatabaseStorage } from './my-storage'

describe('MyDatabaseStorage', () => {
  test('validates complete storage behavior', async () => {
    const storage = new MyDatabaseStorage(connectionString)

    await runStorageTest(storage, {
      storageCleanup: async () => {
        // Cleanup database after tests
        await storage.close()
      },
    })
  })
})
```

The test suite will automatically verify:

- ✅ Task lifecycle management (ready → running → completed/failed)
- ✅ Parent-child task relationships and coordination
- ✅ Retry logic with exponential backoff
- ✅ Concurrent execution handling
- ✅ Error propagation and status transitions
- ✅ Background processing (expiration, closure)
- ✅ Storage consistency under high concurrency

See [`tests/index.test.ts`](./tests/index.test.ts) for a complete example using
`InMemoryTaskExecutionsStorage`. The suite internally spins up a `DurableExecutor` and validates
storage behavior across many scenarios.

### Benchmarking

The benchmark suite measures the performance of storage implementations under various workloads.

```ts
import { runStorageBench } from 'durable-execution-storage-test-utils'
import { InMemoryTaskExecutionsStorage } from 'durable-execution'

await runStorageBench("in memory", () => new InMemoryTaskExecutionsStorage())
```

See [`scripts/bench.ts`](./scripts/bench.ts) for a complete example using
`InMemoryTaskExecutionsStorage`. Benchmarks report per-executor timing stats aggregated across
runs.

## API Reference

### `runStorageTest`

The main test suite that comprehensively validates storage implementations through complex
scenarios.

**Parameters:**

- `storage: TaskExecutionsStorage` - The storage implementation to test
- `options?: object` - Optional configuration options
  - `storageCleanup?: () => void | Promise<void>` - Cleanup function called after tests complete
    (default: no cleanup)
  - `enableStorageBatching?: boolean` - Whether to enable storage batching (default: false)
  - `storageBatchingBackgroundProcessIntraBatchSleepMs?: number` - Artificial delay to add to
    storage batching operations (default: 10ms)

**Features tested:**

- **DurableExecutor integration**: Complex task hierarchies with parent-child relationships
- **Concurrency**: 250+ concurrent tasks with proper coordination
- **Retry mechanisms**: Automatic retry logic with configurable options
- **Error handling**: Various error types and failure scenarios
- **Task types**: Simple tasks, parent tasks, sequential tasks, finalize tasks
- **Storage operations**: CRUD operations, batch updates, status transitions
- **Parent-child relationships**: Nested task hierarchies and active child tracking
- **Background processing**: Task expiration, closure processes, and cleanup

**Example:**

```ts
import { runStorageTest } from 'durable-execution-storage-test-utils'
import { InMemoryTaskExecutionsStorage } from 'durable-execution'

describe('My Storage Implementation', () => {
  test('comprehensive storage test', async () => {
    const storage = new InMemoryTaskExecutionsStorage()
    await runStorageTest(storage)
  })
})
```

### `runStorageBench`

The benchmark suite that measures the performance of storage implementations under various
workloads.

**Parameters:**

- `name: string` - The name of the storage implementation
- `getStorage: () => TaskExecutionsStorage | Promise<TaskExecutionsStorage>` - A function that
  returns the storage implementation to test
- `options?: object` - Optional configuration options
  - `storageCleanup?: (storage: TaskExecutionsStorage) => void | Promise<void>` - Cleanup function
    called after benchmark completes (default: no cleanup)
  - `storageSlowdownMs?: number` - Artificial delay to add to storage operations (default: 0)
  - `executorsCount?: number` - Number of concurrent executors to run (default: 1)
  - `backgroundProcessesCount?: number` - Number of background processes per executor (default: 3)
  - `enableStorageBatching?: boolean` - Whether to enable storage batching. (default: false)
  - `storageBatchingBackgroundProcessIntraBatchSleepMs?: number` - Artificial delay to add to
    storage batching operations (default: 10ms)
  - `childTasksCount?: number` - Number of child tasks per parent task (default: 50)
  - `parentTasksCount?: number` - Number of parent tasks to create (default: 100)
  - `sequentialTasksCount?: number` - Number of sequential tasks to create (default: 100)
  - `pollingTasksCount?: number` - Number of polling tasks to create (default: 100)

**Example:**

```ts
import { runStorageBench } from 'durable-execution-storage-test-utils'
import { InMemoryTaskExecutionsStorage } from 'durable-execution'

await runStorageBench("in memory", () => new InMemoryTaskExecutionsStorage())

// With cleanup for database storage
await runStorageBench(
  "custom",
  () => new CustomTaskExecutionsStorage({ url: process.env.DATABASE_URL! }),
  {
    storageCleanup: async (storage) => {
      // Clean up database after benchmark
      await storage.close()
    },
    executorsCount: 1,
    parentTasksCount: 50,
    childTasksCount: 25,
  },
)
```

### Temporary Resource Helpers

Utilities for managing temporary files and directories in tests:

- `withTemporaryDirectory(fn: (dirPath: string) => Promise<void>)` - Creates and cleans up a
  temporary directory
- `withTemporaryFile(filename: string, fn: (filePath: string) => Promise<void>)` - Creates and
  cleans up a temporary file
- `cleanupTemporaryFiles()` - Removes any leftover temporary files

## Links

- [Durable Execution docs](https://gpahal.github.io/durable-execution)
- [GitHub](https://github.com/gpahal/durable-execution)
- [NPM package](https://www.npmjs.com/package/durable-execution-storage-test-utils)

## License

This project is licensed under the MIT License. See the
[LICENSE](https://github.com/gpahal/durable-execution/blob/main/LICENSE) file for details.
