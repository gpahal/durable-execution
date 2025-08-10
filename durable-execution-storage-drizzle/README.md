# durable-execution-storage-drizzle

[![NPM Version](https://img.shields.io/npm/v/durable-execution-storage-drizzle)](https://www.npmjs.com/package/durable-execution-storage-drizzle)
[![License](https://img.shields.io/npm/l/durable-execution-storage-drizzle)](https://github.com/gpahal/durable-execution/blob/main/LICENSE)

A storage implementation for [durable-execution](https://github.com/gpahal/durable-execution)
using [Drizzle ORM](https://orm.drizzle.team/).

## Installation

- npm

```bash
npm install durable-execution durable-execution-storage-drizzle drizzle-orm
```

- pnpm

```bash
pnpm add durable-execution durable-execution-storage-drizzle drizzle-orm
```

## Features

- Full support for PostgreSQL and SQLite
- Transaction support for consistent state management
- Type-safe schema definitions
- Optimized indexes for performance
- Support for all durable-execution features including parent-child tasks

## Usage

### PostgreSQL

```ts
import { drizzle } from 'drizzle-orm/node-postgres'
import { DurableExecutor } from 'durable-execution'
import { createTaskExecutionsPgTable, createPgStorage } from 'durable-execution-storage-drizzle'

// Create drizzle instance
const db = drizzle(process.env.DATABASE_URL!)

// Create the schema - you can customize the table name by passing a string to the function
const taskExecutionsTable = createTaskExecutionsPgTable()
// Export the table from your schema file

// Create the storage instance
const storage = createPgStorage(db, taskExecutionsTable)

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
import { createTaskExecutionsSQLiteTable, createSQLiteStorage } from 'durable-execution-storage-drizzle'

// Create drizzle instance
const db = drizzle(process.env.DATABASE_URL!)

// Create the table - you can customize the table name by passing a string to the function
const taskExecutionsTable = createTaskExecutionsSQLiteTable()
// Export the table from your schema file

// Create the storage instance
const storage = createSQLiteStorage(db, taskExecutionsTable)

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

## Links

- Durable Execution docs: <https://gpahal.github.io/durable-execution>
- Repository: <https://github.com/gpahal/durable-execution>

## License

This project is licensed under the MIT License. See the
[LICENSE](https://github.com/gpahal/durable-execution/blob/main/LICENSE) file for details.
