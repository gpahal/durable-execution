# durable-execution-storage-drizzle

[![NPM Version](https://img.shields.io/npm/v/durable-execution-storage-drizzle)](https://www.npmjs.com/package/durable-execution-storage-drizzle)
[![License](https://img.shields.io/npm/l/durable-execution-storage-drizzle)](https://github.com/gpahal/durable-execution/blob/main/LICENSE)
[![Code Coverage](https://codecov.io/github/gpahal/durable-execution/graph/badge.svg?flag=storage-drizzle)](https://codecov.io/github/gpahal/durable-execution)

A storage implementation for [durable-execution](https://github.com/gpahal/durable-execution)
using [Drizzle ORM](https://orm.drizzle.team/).

## Installation

```bash
npm install durable-execution durable-execution-storage-drizzle drizzle-orm
```

## Features

- Full support for PostgreSQL, MySQL, and SQLite
- Transaction support for consistent state management
- Type-safe schema definitions
- Optimized indexes for performance
- Support for all durable-execution features including parent-child tasks

## Usage

### PostgreSQL

```ts
import { drizzle } from 'drizzle-orm/node-postgres'
import { DurableExecutor } from 'durable-execution'
import {
  createDurableTaskExecutionsPgTable,
  createPgDurableStorage,
} from 'durable-execution-storage-drizzle'

// Create drizzle instance
const db = drizzle(process.env.DATABASE_URL!)

// Create the schema - you can customize the table name by passing a string to the function
const taskExecutionsTable = createDurableTaskExecutionsPgTable()
// Export the table from your schema file

// Create the storage instance
const storage = createPgDurableStorage(db, taskExecutionsTable)

// Create and use the executor
const executor = new DurableExecutor(storage)

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
  const result = await handle.waitAndGetExecution()
  console.log(result.output) // "Hello, World!"
}

await Promise.all([
  executor.start(),
  main(),
])

await executor.shutdown()
```

### MySQL

```ts
import { drizzle } from "drizzle-orm/mysql2"
import { DurableExecutor } from 'durable-execution'
import { createDurableTaskExecutionsMySQLTable, createMySQLDurableStorage } from 'durable-execution-storage-drizzle'

// Create drizzle instance
const db = drizzle(process.env.DATABASE_URL!)

// Create the table - you can customize the table name by passing a string to the function
const taskExecutionsTable = createDurableTaskExecutionsMySQLTable()
// Export the table from your schema file

// Create the storage instance
const storage = createMySQLDurableStorage(db, taskExecutionsTable)

// Create and use the executor
const executor = new DurableExecutor(storage)

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
  const result = await handle.waitAndGetExecution()
  console.log(result.output) // "Hello, World!"
}

await Promise.all([
  executor.start(),
  main(),
])

await executor.shutdown()
```

### SQLite

```ts
import { drizzle } from 'drizzle-orm/libsql'
import { DurableExecutor } from 'durable-execution'
import { createDurableTaskExecutionsSQLiteTable, createSQLiteDurableStorage } from 'durable-execution-storage-drizzle'

// Create drizzle instance
const db = drizzle(process.env.DATABASE_URL!)

// Create the table - you can customize the table name by passing a string to the function
const taskExecutionsTable = createDurableTaskExecutionsSQLiteTable()
// Export the table from your schema file

// Create the storage instance
const storage = createSQLiteDurableStorage(db, taskExecutionsTable)

// Create and use the executor
const executor = new DurableExecutor(storage)

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
  const result = await handle.waitAndGetExecution()
  console.log(result.output) // "Hello, World!"
}

await Promise.all([
  executor.start(),
  main(),
])

await executor.shutdown()
```

## Database Migrations

Make sure the table is discoverable by Drizzle ORM. You can do this by exporting the table from
your schema file. Once that is done, you can use Drizzle Kit to create the table in your database.

## License

This project is licensed under the MIT License. See the
[LICENSE](https://github.com/gpahal/durable-execution/blob/main/LICENSE) file for details.
