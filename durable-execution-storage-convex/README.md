# durable-execution-storage-convex

[![NPM Version](https://img.shields.io/npm/v/durable-execution-storage-convex)](https://www.npmjs.com/package/durable-execution-storage-convex)
[![License](https://img.shields.io/npm/l/durable-execution-storage-convex)](https://github.com/gpahal/durable-execution/blob/main/LICENSE)

A storage implementation for [durable-execution](https://github.com/gpahal/durable-execution) using
[Convex](https://www.convex.dev/).

## Installation

- npm

```bash
npm install effect durable-execution durable-execution-storage-convex convex
```

- pnpm

```bash
pnpm add effect durable-execution durable-execution-storage-convex convex
```

## Features

- Full support for Convex
- Transaction support for consistent state management
- Type-safe schema definitions
- Optimized indexes for performance
- Support for all durable-execution features including parent-child tasks

## Usage

- Add the storage component to your Convex project

```ts
// src/convex/convex.config.ts

import { defineApp } from 'convex/server'
import taskExecutionsStorage from 'durable-execution-storage-convex/convex.config'

const app = defineApp()
app.use(taskExecutionsStorage)

export default app
```

- Expose the public api needed by the storage implementation. Use a random string as the
  auth secret. Use the same auth secret in the storage implementation. The publicly exposed api is
  protected by the auth secret.

```ts
// src/convex/taskExecutionsStorage.ts

import {
  convertDurableExecutionStorageComponentToPublicApiImpl
} from 'durable-execution-storage-convex'

import { components } from './_generated/api'

export const {
  insertMany,
  getManyById,
  getManyBySleepingTaskUniqueId,
  updateManyById,
  updateManyByIdAndInsertChildrenIfUpdated,
  updateByStatusAndStartAtLessThanAndReturn,
  updateByStatusAndOCFPStatusAndACCZeroAndReturn,
  updateByCloseStatusAndReturn,
  updateByStatusAndIsSleepingTaskAndExpiresAtLessThan,
  updateByOCFPExpiresAt,
  updateByCloseExpiresAt,
  updateByExecutorIdAndNPCAndReturn,
  getManyByParentExecutionId,
  updateManyByParentExecutionIdAndIsFinished,
  updateAndDecrementParentACCByIsFinishedAndCloseStatus,
  deleteById,
  deleteAll,
} = convertDurableExecutionStorageComponentToPublicApiImpl(
  components.taskExecutionsStorage,
  'SUPER_SECRET',
)
```

- Use the storage implementation

```ts
// src/index.ts

import { ConvexHttpClient } from 'convex/browser'
import { DurableExecutor } from 'durable-execution'
import { ConvexTaskExecutionsStorage } from 'durable-execution-storage-convex'

import { api } from '../convex/_generated/api'

// Create the convex client
const convexClient = new ConvexHttpClient(process.env.VITE_CONVEX_URL!)

// Create the storage instance
const storage = new ConvexTaskExecutionsStorage(
  convexClient,
  // Use a random string as the auth secret as the one in the public api
  'SUPER_SECRET',
  api.taskExecutionsStorage,
)

// Create and use the executor
const executor = await DurableExecutor.make(storage)

// Create a task
const task = executor.task({
  id: 'my-task',
  timeoutMs: 30_000,
  run: (ctx, input: { name: string }) => {
    return `Hello, ${input.name}!`
  },
})

// Use the executor
async function main() {
  const handle = await executor.enqueueTask(task, { name: 'World' })
  const result = await handle.waitAndGetFinishedExecution()
  console.log(result.output) // "Hello, World!"
}

// Start the durable executor
await executor.start()

// Run main
await main()

await executor.shutdown()
```

## Database Migrations

Utilities will be provided when migrations are needed.

## Links

- [Durable Execution docs](https://gpahal.github.io/durable-execution)
- [GitHub](https://github.com/gpahal/durable-execution)
- [NPM package](https://www.npmjs.com/package/durable-execution-storage-convex)
- [Convex](https://www.convex.dev/)

## License

This project is licensed under the MIT License. See the
[LICENSE](https://github.com/gpahal/durable-execution/blob/main/LICENSE) file for details.
