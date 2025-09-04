# durable-execution-storage-drizzle

[![NPM Version](https://img.shields.io/npm/v/durable-execution-storage-drizzle)](https://www.npmjs.com/package/durable-execution-storage-drizzle)
[![License](https://img.shields.io/npm/l/durable-execution-storage-drizzle)](https://github.com/gpahal/durable-execution/blob/main/LICENSE)
[![Coverage](https://img.shields.io/codecov/c/github/gpahal/durable-execution/main?flag=durable-execution-storage-drizzle)](https://codecov.io/gh/gpahal/durable-execution?flag=durable-execution-storage-drizzle)

A storage implementation for [durable-execution](https://github.com/gpahal/durable-execution) using
[Drizzle ORM](https://orm.drizzle.team/).

## Installation

- npm

```bash
npm install effect durable-execution durable-execution-storage-drizzle drizzle-orm
```

- pnpm

```bash
pnpm add effect durable-execution durable-execution-storage-drizzle drizzle-orm
```

## Features

- Full support for PostgreSQL, MySQL and SQLite
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
  createPgTaskExecutionsTable,
  createPgTaskExecutionsStorage
} from 'durable-execution-storage-drizzle'

// Create drizzle instance
const db = drizzle(process.env.DATABASE_URL!)

// Create the schema - you can customize the table name by passing a string to the function
const taskExecutionsTable = createPgTaskExecutionsTable()
// Export the table from your schema file

// Create the storage instance
const storage = createPgTaskExecutionsStorage(db, taskExecutionsTable)

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
  const result = await handle.waitAndGetFinishedExecution()
  console.log(result.output) // "Hello, World!"
}

// Start the durable executor
executor.start()

// Run main
await main()

await executor.shutdown()
```

### MySQL

```ts
import { drizzle } from 'drizzle-orm/mysql2'
import { DurableExecutor } from 'durable-execution'
import {
  createMySqlTaskExecutionsTable,
  createMySqlTaskExecutionsStorage
} from 'durable-execution-storage-drizzle'

// Create drizzle instance
const db = drizzle(process.env.DATABASE_URL!)

// Create the schema - you can customize the table name by passing a string to the function
const taskExecutionsTable = createMySqlTaskExecutionsTable()
// Export the table from your schema file

// Create the storage instance
const storage = createMySqlTaskExecutionsStorage(
  db,
  taskExecutionsTable,
  (result) => result[0].affectedRows
)

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
  const result = await handle.waitAndGetFinishedExecution()
  console.log(result.output) // "Hello, World!"
}

// Start the durable executor
executor.start()

// Run main
await main()

await executor.shutdown()
```

### SQLite

```ts
import { drizzle } from 'drizzle-orm/libsql'
import { DurableExecutor } from 'durable-execution'
import {
  createSQLiteTaskExecutionsTable,
  createSQLiteTaskExecutionsStorage
} from 'durable-execution-storage-drizzle'

// Create drizzle instance
const db = drizzle(process.env.DATABASE_URL!)

// Create the schema - you can customize the table name by passing a string to the function
const taskExecutionsTable = createSQLiteTaskExecutionsTable()
// Export the table from your schema file

// Create the storage instance
const storage = createSQLiteTaskExecutionsStorage(db, taskExecutionsTable)

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
  const result = await handle.waitAndGetFinishedExecution()
  console.log(result.output) // "Hello, World!"
}

// Start the durable executor
executor.start()

// Run main
await main()

await executor.shutdown()
```

## Database Migrations

Make sure the table is discoverable by Drizzle ORM. You can do this by exporting the table from
your schema file. Once that is done, you can use Drizzle Kit to create the table in your database.

## Links

- [Durable Execution docs](https://gpahal.github.io/durable-execution)
- [GitHub](https://github.com/gpahal/durable-execution)
- [NPM package](https://www.npmjs.com/package/durable-execution-storage-drizzle)
- [Drizzle ORM](https://orm.drizzle.team/)

## License

This project is licensed under the MIT License. See the
[LICENSE](https://github.com/gpahal/durable-execution/blob/main/LICENSE) file for details.
