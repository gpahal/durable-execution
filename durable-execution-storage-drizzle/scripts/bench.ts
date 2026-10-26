import { createRequire } from 'node:module'

import { MySqlContainer } from '@testcontainers/mysql'
import { PostgreSqlContainer } from '@testcontainers/postgresql'
import type {
  generateMySQLDrizzleJson as generateMySQLDrizzleJsonType,
  generateMySQLMigration as generateMySQLMigrationType,
  pushSchema as pushSchemaType,
  pushSQLiteSchema as pushSQLiteSchemaType,
} from 'drizzle-kit/api'
import { drizzle as drizzleLibsql } from 'drizzle-orm/libsql'
import { drizzle as drizzleMySql } from 'drizzle-orm/mysql2'
import { drizzle as drizzlePg } from 'drizzle-orm/node-postgres'
import {
  cleanupTemporaryFiles,
  runStorageBench,
  withTemporaryFile,
} from 'durable-execution-storage-test-utils'
import mysql from 'mysql2/promise'
import { Pool } from 'pg'

import {
  createMySqlTaskExecutionsStorage,
  createMySqlTaskExecutionsTable,
  createPgTaskExecutionsStorage,
  createPgTaskExecutionsTable,
  createSQLiteTaskExecutionsStorage,
  createSQLiteTaskExecutionsTable,
} from '../src'

const require = createRequire(import.meta.url)
const { pushSchema, pushSQLiteSchema, generateMySQLDrizzleJson, generateMySQLMigration } =
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- require returns any, and the types come from drizzle-kit/api's declarations
  require('drizzle-kit/api') as {
    pushSchema: typeof pushSchemaType
    pushSQLiteSchema: typeof pushSQLiteSchemaType
    generateMySQLDrizzleJson: typeof generateMySQLDrizzleJsonType
    generateMySQLMigration: typeof generateMySQLMigrationType
  }

async function benchPg() {
  const container = await new PostgreSqlContainer('postgres:16.10').start()
  let pool: Pool | undefined
  try {
    const taskExecutionsTable = createPgTaskExecutionsTable()
    pool = new Pool({
      connectionString: `postgresql://${container.getUsername()}:${container.getPassword()}@${container.getHost()}:${container.getPort()}/${container.getDatabase()}`,
    })
    const db = drizzlePg(pool)
    const { apply } = await pushSchema({ taskExecutionsTable }, db)
    await apply()

    await runStorageBench('pg', () =>
      createPgTaskExecutionsStorage(db, taskExecutionsTable, { enableTestMode: true }),
    )
  } finally {
    if (pool) {
      await pool.end()
    }
    await container.stop()
  }
}

async function benchMySql() {
  const container = await new MySqlContainer('mysql:8.4').start()
  let pool: mysql.Pool | undefined
  try {
    const taskExecutionsTable = createMySqlTaskExecutionsTable()
    pool = mysql.createPool({
      uri: `mysql://${container.getUsername()}:${container.getUserPassword()}@${container.getHost()}:${container.getPort()}/${container.getDatabase()}`,
    })
    const db = drizzleMySql(pool)

    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle-kit/api's declarations don't resolve the snapshot type, so the result is error typed
    const emptyDrizzleJson = (await generateMySQLDrizzleJson({})) as Record<string, unknown>
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle-kit/api's declarations don't resolve the snapshot type, so the result is error typed
    const drizzleJson = (await generateMySQLDrizzleJson({
      taskExecutionsTable,
    })) as Record<string, unknown>
    const migration = await generateMySQLMigration(emptyDrizzleJson, drizzleJson)
    for (const statement of migration) {
      await db.execute(statement)
    }

    await runStorageBench('mysql', () =>
      createMySqlTaskExecutionsStorage(
        db,
        taskExecutionsTable,
        (result) => result[0].affectedRows,
        {
          enableTestMode: true,
        },
      ),
    )
  } finally {
    if (pool) {
      await pool.end()
    }
    await container.stop()
  }
}

async function benchSQLite() {
  await withTemporaryFile('test.db', async (filePath) => {
    const taskExecutionsTable = createSQLiteTaskExecutionsTable()
    const db = drizzleLibsql(`file:${filePath}`)
    const { apply } = await pushSQLiteSchema({ taskExecutionsTable }, db)
    await apply()

    await runStorageBench('sqlite', () =>
      createSQLiteTaskExecutionsStorage(db, taskExecutionsTable, {
        enableTestMode: true,
      }),
    )
  })
}

async function main() {
  try {
    await benchPg()
    await benchMySql()
    await benchSQLite()
  } finally {
    await cleanupTemporaryFiles()
  }
}

/**
 * Exit with an error, listing the resources that keep the process alive, when it hasn't exited
 * `timeoutMs` after the bench finished.
 */
function waitForExitOrLogActiveHandles(timeoutMs: number): void {
  setTimeout(() => {
    console.error(
      `Process did not exit within ${timeoutMs}ms. Active resources:`,
      process.getActiveResourcesInfo(),
    )
    // eslint-disable-next-line unicorn/no-process-exit -- the bench script exits when leftover resources keep it alive
    process.exit(1)
  }, timeoutMs).unref()
}

await main()

waitForExitOrLogActiveHandles(10_000)
