import { createRequire } from 'node:module'

import { MySqlContainer } from '@testcontainers/mysql'
import type {
  generateMySQLDrizzleJson as generateMySQLDrizzleJsonType,
  generateMySQLMigration as generateMySQLMigrationType,
  pushSchema as pushSchemaType,
  pushSQLiteSchema as pushSQLiteSchemaType,
} from 'drizzle-kit/api'
import { drizzle as drizzleLibsql } from 'drizzle-orm/libsql'
import { drizzle as drizzleMySql } from 'drizzle-orm/mysql2'
import { drizzle as drizzlePglite } from 'drizzle-orm/pglite'
import {
  cleanupTemporaryFiles,
  runStorageTest,
  withTemporaryDirectory,
  withTemporaryFile,
} from 'durable-execution-storage-test-utils'
import { describe, it } from 'vitest'

import {
  createFinishedChildTaskExecutionsMySqlTable,
  createFinishedChildTaskExecutionsPgTable,
  createFinishedChildTaskExecutionsSQLiteTable,
  createMySqlStorage,
  createPgStorage,
  createSQLiteStorage,
  createTaskExecutionsMySqlTable,
  createTaskExecutionsPgTable,
  createTaskExecutionsSQLiteTable,
} from '../src'

const require = createRequire(import.meta.url)
const { pushSchema, pushSQLiteSchema, generateMySQLDrizzleJson, generateMySQLMigration } =
  require('drizzle-kit/api') as {
    pushSchema: typeof pushSchemaType
    pushSQLiteSchema: typeof pushSQLiteSchemaType
    generateMySQLDrizzleJson: typeof generateMySQLDrizzleJsonType
    generateMySQLMigration: typeof generateMySQLMigrationType
  }

describe('index', () => {
  afterAll(cleanupTemporaryFiles)

  it('should complete with pg storage', { timeout: 120_000 }, async () => {
    await withTemporaryDirectory(async (dirPath) => {
      const taskExecutionsTable = createTaskExecutionsPgTable()
      const finishedChildTaskExecutionsTable = createFinishedChildTaskExecutionsPgTable()
      const db = drizzlePglite(dirPath)
      const { apply } = await pushSchema(
        { taskExecutionsTable, finishedChildTaskExecutionsTable },
        db,
      )
      await apply()

      const storage = createPgStorage(db, taskExecutionsTable, finishedChildTaskExecutionsTable)
      await runStorageTest(storage)
    })
  })

  it('should complete with mysql storage', { timeout: 300_000 }, async () => {
    const container = await new MySqlContainer('mysql:8.4').start()
    try {
      const taskExecutionsTable = createTaskExecutionsMySqlTable()
      const finishedChildTaskExecutionsTable = createFinishedChildTaskExecutionsMySqlTable()
      const db = drizzleMySql(
        `mysql://${container.getUsername()}:${container.getUserPassword()}@${container.getHost()}:${container.getPort()}/${container.getDatabase()}`,
      )

      const emptyDrizzleJson = (await generateMySQLDrizzleJson({})) as Record<string, unknown>
      const drizzleJson = (await generateMySQLDrizzleJson({
        taskExecutionsTable,
        finishedChildTaskExecutionsTable,
      })) as Record<string, unknown>
      const migration = await generateMySQLMigration(emptyDrizzleJson, drizzleJson)
      for (const statement of migration) {
        await db.execute(statement)
      }

      const storage = createMySqlStorage(
        db,
        taskExecutionsTable,
        finishedChildTaskExecutionsTable,
        (result) => result[0].affectedRows,
      )
      await runStorageTest(storage)
    } finally {
      await container.stop()
    }
  })

  it('should complete with sqlite storage', { timeout: 120_000 }, async () => {
    await withTemporaryFile('test.db', async (filePath) => {
      const taskExecutionsTable = createTaskExecutionsSQLiteTable()
      const finishedChildTaskExecutionsTable = createFinishedChildTaskExecutionsSQLiteTable()
      const db = drizzleLibsql(`file:${filePath}`)
      const { apply } = await pushSQLiteSchema(
        { taskExecutionsTable, finishedChildTaskExecutionsTable },
        db,
      )
      await apply()

      const storage = createSQLiteStorage(db, taskExecutionsTable, finishedChildTaskExecutionsTable)
      await runStorageTest(storage)
    })
  })
})
