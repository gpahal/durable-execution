import { mkdtemp, rm } from 'node:fs/promises'
import { createRequire } from 'node:module'
import path from 'node:path'

import type {
  pushSchema as pushSchemaType,
  pushSQLiteSchema as pushSQLiteSchemaType,
} from 'drizzle-kit/api'
import { drizzle as drizzleLibsql } from 'drizzle-orm/libsql'
import { drizzle as drizzlePglite } from 'drizzle-orm/pglite'
import {
  createInMemoryStorage,
  DurableExecutor,
  type DurableStorage,
  type DurableTask,
} from 'durable-execution'
import { describe, expect, it } from 'vitest'

import { sleep } from '@gpahal/std/promises'

import {
  createDurableTaskExecutionsPgTable,
  createDurableTaskExecutionsSQLiteTable,
  createPgDurableStorage,
  createSQLiteDurableStorage,
} from '../src'

const require = createRequire(import.meta.url)
const { pushSchema, pushSQLiteSchema } = require('drizzle-kit/api') as {
  pushSchema: typeof pushSchemaType
  pushSQLiteSchema: typeof pushSQLiteSchemaType
}

async function runStorageTest(storage: DurableStorage, cleanup?: () => void | Promise<void>) {
  const executor = new DurableExecutor(storage, {
    enableDebug: false,
  })
  void executor.start()

  try {
    await runExecutorTest(executor)
  } finally {
    await executor.shutdown()
    if (cleanup) {
      await cleanup()
    }
  }
}

async function runExecutorTest(executor: DurableExecutor) {
  const taskB1 = executor.task({
    id: 'b1',
    timeoutMs: 1000,
    run: (ctx, input: { name: string }) => {
      return {
        name: input.name,
        taskB1Output: `Hello from task B1, ${input.name}!`,
      }
    },
  })
  const taskB2 = executor.task({
    id: 'b2',
    timeoutMs: 1000,
    run: (ctx, input: { name: string; taskB1Output: string }) => {
      return {
        name: input.name,
        taskB1Output: input.taskB1Output,
        taskB2Output: `Hello from task B2, ${input.name}!`,
      }
    },
  })
  const taskB3 = executor.task({
    id: 'b3',
    timeoutMs: 1000,
    run: (ctx, input: { name: string; taskB1Output: string; taskB2Output: string }) => {
      return {
        taskB1Output: input.taskB1Output,
        taskB2Output: input.taskB2Output,
        taskB3Output: `Hello from task B3, ${input.name}!`,
      }
    },
  })
  const taskB = executor.sequentialTasks(taskB1, taskB2, taskB3)

  const taskA1 = executor.task({
    id: 'a1',
    timeoutMs: 1000,
    run: (ctx, input: { name: string }) => {
      return `Hello from task A1, ${input.name}!`
    },
  })
  const taskA2 = executor.task({
    id: 'a2',
    timeoutMs: 1000,
    run: (ctx, input: { name: string }) => {
      return `Hello from task A2, ${input.name}!`
    },
  })
  const taskA3 = executor.task({
    id: 'a3',
    timeoutMs: 1000,
    run: (ctx, input: { name: string }) => {
      return `Hello from task A3, ${input.name}!`
    },
  })
  const taskA = executor.parentTask({
    id: 'a',
    timeoutMs: 1000,
    runParent: (ctx, input: { name: string }) => {
      return {
        output: `Hello from task A, ${input.name}!`,
        childrenTasks: [
          { task: taskA1, input: { name: input.name } },
          { task: taskA2, input: { name: input.name } },
          { task: taskA3, input: { name: input.name } },
        ],
      }
    },
    finalizeTask: {
      id: 'taskAFinalize',
      timeoutMs: 1000,
      run: (ctx, { output, childrenTasksOutputs }) => {
        return {
          taskAOutput: output,
          taskA1Output: childrenTasksOutputs[0]!.output as string,
          taskA2Output: childrenTasksOutputs[1]!.output as string,
          taskA3Output: childrenTasksOutputs[2]!.output as string,
        }
      },
    },
  })

  const rootTask = executor.parentTask({
    id: 'root',
    timeoutMs: 1000,
    runParent: (ctx, input: { name: string }) => {
      return {
        output: `Hello from root task, ${input.name}!`,
        childrenTasks: [
          { task: taskA, input: { name: input.name } },
          { task: taskB, input: { name: input.name } },
        ],
      }
    },
    finalizeTask: {
      id: 'rootFinalize',
      timeoutMs: 1000,
      run: (ctx, { output, childrenTasksOutputs }) => {
        const taskAOutput = childrenTasksOutputs[0]!.output as {
          taskAOutput: string
          taskA1Output: string
          taskA2Output: string
          taskA3Output: string
        }
        const taskBOutput = childrenTasksOutputs[1]!.output as {
          taskB1Output: string
          taskB2Output: string
          taskB3Output: string
        }
        return {
          rootOutput: output,
          taskAOutput: taskAOutput.taskAOutput,
          taskA1Output: taskAOutput.taskA1Output,
          taskA2Output: taskAOutput.taskA2Output,
          taskA3Output: taskAOutput.taskA3Output,
          taskB1Output: taskBOutput.taskB1Output,
          taskB2Output: taskBOutput.taskB2Output,
          taskB3Output: taskBOutput.taskB3Output,
        }
      },
    },
  })

  const concurrentTasks: Array<DurableTask<string, string>> = []
  for (let i = 0; i < 100; i++) {
    concurrentTasks.push(
      executor.task({
        id: `t${i}`,
        timeoutMs: 1000,
        run: async (ctx, input: string) => {
          await sleep(100 * Math.random())
          return `Hello from task T${i}, ${input}!`
        },
      }),
    )
  }

  const handle = await executor.enqueueTask(rootTask, { name: 'world' })
  const concurrentHandles = await Promise.all(
    concurrentTasks.map((task) => executor.enqueueTask(task, 'world')),
  )

  const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
  expect(finishedExecution.status).toBe('completed')
  assert(finishedExecution.status === 'completed')
  expect(finishedExecution.taskId).toBe('root')
  expect(finishedExecution.executionId).toMatch(/^te_/)
  expect(finishedExecution.output).toBeDefined()
  expect(finishedExecution.output.taskAOutput).toBe('Hello from task A, world!')
  expect(finishedExecution.output.taskA1Output).toBe('Hello from task A1, world!')
  expect(finishedExecution.output.taskA2Output).toBe('Hello from task A2, world!')
  expect(finishedExecution.output.taskA3Output).toBe('Hello from task A3, world!')
  expect(finishedExecution.output.taskB1Output).toBe('Hello from task B1, world!')
  expect(finishedExecution.output.taskB2Output).toBe('Hello from task B2, world!')
  expect(finishedExecution.output.taskB3Output).toBe('Hello from task B3, world!')
  expect(finishedExecution.startedAt).toBeInstanceOf(Date)
  expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
  expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
    finishedExecution.startedAt.getTime(),
  )

  const concurrentFinishedExecutions = await Promise.all(
    concurrentHandles.map((handle) => handle.waitAndGetTaskFinishedExecution()),
  )
  for (const [i, execution] of concurrentFinishedExecutions.entries()) {
    expect(execution.status).toBe('completed')
    assert(execution.status === 'completed')
    expect(execution.taskId).toMatch(/^t\d+$/)
    expect(execution.executionId).toMatch(/^te_/)
    expect(execution.output).toBeDefined()
    expect(execution.output).toBe(`Hello from task T${i}, world!`)
    expect(execution.startedAt).toBeInstanceOf(Date)
    expect(execution.finishedAt).toBeInstanceOf(Date)
    expect(execution.finishedAt.getTime()).toBeGreaterThanOrEqual(execution.startedAt.getTime())
  }
}

export async function withTemporaryDirectory(fn: (dirPath: string) => Promise<void>) {
  const dirPath = await mkdtemp('.tmp_')
  try {
    await fn(dirPath)
  } finally {
    await rm(dirPath, { recursive: true })
  }
}

export async function withTemporaryFile(filename: string, fn: (file: string) => Promise<void>) {
  return withTemporaryDirectory(async (dirPath) => {
    const filePath = path.join(dirPath, filename)
    await fn(filePath)
  })
}

describe('index', () => {
  it('should complete with in memory storage', { timeout: 30_000 }, async () => {
    const storage = createInMemoryStorage({ enableDebug: false })
    await runStorageTest(storage)
  })

  it('should complete with sqlite storage', { timeout: 30_000 }, async () => {
    await withTemporaryFile('test.db', async (filePath) => {
      const table = createDurableTaskExecutionsSQLiteTable()
      const db = drizzleLibsql(`file:${filePath}`)
      const { apply } = await pushSQLiteSchema({ table }, db)
      await apply()

      const storage = createSQLiteDurableStorage(db, table)
      await runStorageTest(storage)
    })
  })

  it('should complete with pg storage', { timeout: 30_000 }, async () => {
    await withTemporaryDirectory(async (dirPath) => {
      const table = createDurableTaskExecutionsPgTable()
      const db = drizzlePglite(dirPath)
      const { apply } = await pushSchema({ table }, db)
      await apply()

      const storage = createPgDurableStorage(db, table)
      await runStorageTest(storage)
    })
  })
})
