# durable-execution-storage-test-utils

[![NPM Version](https://img.shields.io/npm/v/durable-execution-storage-test-utils)](https://www.npmjs.com/package/durable-execution-storage-test-utils)
[![License](https://img.shields.io/npm/l/durable-execution-storage-test-utils)](https://github.com/gpahal/durable-execution/blob/main/LICENSE)
[![Coverage](https://img.shields.io/codecov/c/github/gpahal/durable-execution/main?flag=durable-execution-storage-test-utils)](https://codecov.io/gh/gpahal/durable-execution?flag=durable-execution-storage-test-utils)

Test utilities for validating [durable-execution](https://github.com/gpahal/durable-execution) storage implementations.

## Installation

- npm

```bash
npm install durable-execution durable-execution-storage-test-utils
```

- pnpm

```bash
pnpm add durable-execution durable-execution-storage-test-utils
```

## API Reference

### `runStorageTest(storage, cleanup?)`

The main test suite that comprehensively validates storage implementations through complex scenarios.

**Parameters:**

- `storage: TaskExecutionsStorage` - The storage implementation to test
- `cleanup?: () => void | Promise<void>` - Optional cleanup function called after tests complete

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

### Temporary Resource Helpers

Utilities for managing temporary files and directories in tests:

- `withTemporaryDirectory(fn: (dirPath: string) => Promise<void>)` - Creates and cleans up a temporary directory
- `withTemporaryFile(filename: string, fn: (filePath: string) => Promise<void>)` - Creates and cleans up a temporary file
- `cleanupTemporaryFiles()` - Removes any leftover temporary files

## Usage

The primary use case is validating that your storage implementation correctly handles all aspects of durable task execution:

```ts
import { describe, test } from 'vitest'
import { runStorageTest } from 'durable-execution-storage-test-utils'
import { MyDatabaseStorage } from './my-storage'

describe('MyDatabaseStorage', () => {
  test('validates complete storage behavior', async () => {
    const storage = new MyDatabaseStorage(connectionString)

    await runStorageTest(storage, async () => {
      // Cleanup database after tests
      await storage.close()
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

See [`tests/index.test.ts`](./tests/index.test.ts) for a complete example using `InMemoryTaskExecutionsStorage`.

## Links

- [Durable Execution docs](https://gpahal.github.io/durable-execution)
- [GitHub](https://github.com/gpahal/durable-execution)
- [NPM package](https://www.npmjs.com/package/durable-execution-storage-test-utils)

## License

This project is licensed under the MIT License. See the
[LICENSE](https://github.com/gpahal/durable-execution/blob/main/LICENSE) file for details.
