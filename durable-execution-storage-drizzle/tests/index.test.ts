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
  runStorageTest,
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
  require('drizzle-kit/api') as {
    pushSchema: typeof pushSchemaType
    pushSQLiteSchema: typeof pushSQLiteSchemaType
    generateMySQLDrizzleJson: typeof generateMySQLDrizzleJsonType
    generateMySQLMigration: typeof generateMySQLMigrationType
  }

describe('index', () => {
  afterAll(cleanupTemporaryFiles)

  it('should complete with pg storage', { timeout: 300_000 }, async () => {
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

      const storage = createPgTaskExecutionsStorage(db, taskExecutionsTable, {
        enableTestMode: true,
      })
      await expect(runStorageTest(storage)).resolves.not.toThrow()
    } finally {
      if (pool) {
        await pool.end()
      }
      await container.stop()
    }
  })

  it('should complete with mysql storage', { timeout: 300_000 }, async () => {
    const container = await new MySqlContainer('mysql:8.4').start()
    let pool: mysql.Pool | undefined
    try {
      const taskExecutionsTable = createMySqlTaskExecutionsTable()
      pool = mysql.createPool({
        uri: `mysql://${container.getUsername()}:${container.getUserPassword()}@${container.getHost()}:${container.getPort()}/${container.getDatabase()}`,
      })
      const db = drizzleMySql(pool)

      const emptyDrizzleJson = (await generateMySQLDrizzleJson({})) as Record<string, unknown>
      const drizzleJson = (await generateMySQLDrizzleJson({
        taskExecutionsTable,
      })) as Record<string, unknown>
      const migration = await generateMySQLMigration(emptyDrizzleJson, drizzleJson)
      for (const statement of migration) {
        await db.execute(statement)
      }

      const storage = createMySqlTaskExecutionsStorage(
        db,
        taskExecutionsTable,
        (result) => result[0].affectedRows,
        {
          enableTestMode: true,
        },
      )
      await expect(runStorageTest(storage)).resolves.not.toThrow()
    } finally {
      if (pool) {
        await pool.end()
      }
      await container.stop()
    }
  })

  it('should complete with sqlite storage', { timeout: 120_000 }, async () => {
    await withTemporaryFile('test.db', async (filePath) => {
      const taskExecutionsTable = createSQLiteTaskExecutionsTable()
      const db = drizzleLibsql(`file:${filePath}`)
      const { apply } = await pushSQLiteSchema({ taskExecutionsTable }, db)
      await apply()

      const storage = createSQLiteTaskExecutionsStorage(db, taskExecutionsTable, {
        enableTestMode: true,
      })
      await expect(runStorageTest(storage)).resolves.not.toThrow()
    })
  })
})
