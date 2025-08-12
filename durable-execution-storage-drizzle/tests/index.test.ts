import fs from 'node:fs/promises'
import { createRequire } from 'node:module'
import path from 'node:path'

import type {
  pushSchema as pushSchemaType,
  pushSQLiteSchema as pushSQLiteSchemaType,
} from 'drizzle-kit/api'
import { drizzle as drizzleLibsql } from 'drizzle-orm/libsql'
import { drizzle as drizzlePglite } from 'drizzle-orm/pglite'
import { DurableExecutor, InMemoryStorage, type Storage, type Task } from 'durable-execution'
import { describe, expect, it } from 'vitest'

import { sleep } from '@gpahal/std/promises'

import {
  createPgStorage,
  createSQLiteStorage,
  createTaskExecutionsPgTable,
  createTaskExecutionsSQLiteTable,
} from '../src'

const require = createRequire(import.meta.url)
const { pushSQLiteSchema, pushSchema } = require('drizzle-kit/api') as {
  pushSQLiteSchema: typeof pushSQLiteSchemaType
  pushSchema: typeof pushSchemaType
}

async function runStorageTest(storage: Storage, cleanup?: () => void | Promise<void>) {
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
      run: (ctx, { output, childrenTaskExecutionsOutputs }) => {
        return {
          taskAOutput: output,
          taskA1Output: childrenTaskExecutionsOutputs[0]!.output as string,
          taskA2Output: childrenTaskExecutionsOutputs[1]!.output as string,
          taskA3Output: childrenTaskExecutionsOutputs[2]!.output as string,
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
      run: (ctx, { output, childrenTaskExecutionsOutputs }) => {
        const taskAOutput = childrenTaskExecutionsOutputs[0]!.output as {
          taskAOutput: string
          taskA1Output: string
          taskA2Output: string
          taskA3Output: string
        }
        const taskBOutput = childrenTaskExecutionsOutputs[1]!.output as {
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

  const concurrentTasks: Array<Task<string, string>> = []
  for (let i = 0; i < 100; i++) {
    concurrentTasks.push(
      executor.task({
        id: `t${i}`,
        timeoutMs: 1000,
        run: async (ctx, input: string) => {
          await sleep(10 * Math.random())
          return `Hello from task T${i}, ${input}!`
        },
      }),
    )
  }

  const concurrentChildTask = executor.task({
    id: 'concurrent_child',
    timeoutMs: 1000,
    run: async (_, index: number) => {
      await sleep(1)
      return index
    },
  })

  const concurrentParentTask = executor.parentTask({
    id: 'concurrent_parent',
    timeoutMs: 1000,
    runParent: async () => {
      await sleep(1)
      return {
        output: undefined,
        childrenTasks: Array.from({ length: 250 }, (_, index) => ({
          task: concurrentChildTask,
          input: index,
        })),
      }
    },
  })

  const handle = await executor.enqueueTask(rootTask, { name: 'world' })
  const concurrentHandles = await Promise.all(
    concurrentTasks.map((task) => executor.enqueueTask(task, 'world')),
  )

  const finishedExecution = await handle.waitAndGetFinishedExecution()
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
    concurrentHandles.map((handle) => handle.waitAndGetFinishedExecution()),
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

  const retryTask = executor.task({
    id: 'retry',
    retryOptions: {
      maxAttempts: 3,
    },
    timeoutMs: 1000,
    run: (ctx) => {
      if (ctx.attempt < 2) {
        throw new Error('Failed')
      }
      return 'Success'
    },
  })

  const retryTaskHandle = await executor.enqueueTask(retryTask)
  const retryExecution = await retryTaskHandle.waitAndGetFinishedExecution()
  expect(retryExecution.status).toBe('completed')
  assert(retryExecution.status === 'completed')
  expect(retryExecution.taskId).toBe('retry')
  expect(retryExecution.executionId).toMatch(/^te_/)
  expect(retryExecution.output).toBe('Success')
  expect(retryExecution.retryAttempts).toBe(2)
  expect(retryExecution.startedAt).toBeInstanceOf(Date)
  expect(retryExecution.finishedAt).toBeInstanceOf(Date)
  expect(retryExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
    retryExecution.startedAt.getTime(),
  )

  const failingTask = executor.task({
    id: 'failing',
    timeoutMs: 1000,
    run: () => {
      throw new Error('Failed')
    },
  })

  const failingTaskHandle = await executor.enqueueTask(failingTask)
  const failingExecution = await failingTaskHandle.waitAndGetFinishedExecution()
  expect(failingExecution.status).toBe('failed')
  assert(failingExecution.status === 'failed')
  expect(failingExecution.taskId).toBe('failing')
  expect(failingExecution.executionId).toMatch(/^te_/)
  expect(failingExecution.error).toBeDefined()
  expect(failingExecution.error.message).toBe('Failed')
  expect(failingExecution.startedAt).toBeInstanceOf(Date)
  expect(failingExecution.finishedAt).toBeInstanceOf(Date)
  expect(failingExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
    failingExecution.startedAt.getTime(),
  )

  const parentTaskWithFailingChild = executor.parentTask({
    id: 'parentWithFailingChild',
    timeoutMs: 1000,
    runParent: () => {
      return {
        output: undefined,
        childrenTasks: [{ task: failingTask, input: undefined }],
      }
    },
  })

  const parentTaskWithFailingChildHandle = await executor.enqueueTask(
    parentTaskWithFailingChild,
    undefined,
  )
  const parentTaskWithFailingChildExecution =
    await parentTaskWithFailingChildHandle.waitAndGetFinishedExecution()
  expect(parentTaskWithFailingChildExecution.status).toBe('children_tasks_failed')
  assert(parentTaskWithFailingChildExecution.status === 'children_tasks_failed')
  expect(parentTaskWithFailingChildExecution.taskId).toBe('parentWithFailingChild')
  expect(parentTaskWithFailingChildExecution.childrenTaskExecutionsErrors).toBeDefined()
  expect(parentTaskWithFailingChildExecution.childrenTaskExecutionsErrors[0]!.error.message).toBe(
    'Failed',
  )
  expect(parentTaskWithFailingChildExecution.startedAt).toBeInstanceOf(Date)
  expect(parentTaskWithFailingChildExecution.finishedAt).toBeInstanceOf(Date)
  expect(parentTaskWithFailingChildExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
    parentTaskWithFailingChildExecution.startedAt.getTime(),
  )

  const parentTaskWithFailingFinalizeTask = executor.parentTask({
    id: 'parentWithFailingFinalizeTask',
    timeoutMs: 1000,
    runParent: () => {
      return {
        output: undefined,
        childrenTasks: [{ task: taskA1, input: { name: 'world' } }],
      }
    },
    finalizeTask: {
      id: 'parentWithFailingFinalizeTaskFinalize',
      timeoutMs: 1000,
      run: () => {
        throw new Error('Failed')
      },
    },
  })

  const parentTaskWithFailingFinalizeTaskHandle = await executor.enqueueTask(
    parentTaskWithFailingFinalizeTask,
    undefined,
  )
  const parentTaskWithFailingFinalizeTaskExecution =
    await parentTaskWithFailingFinalizeTaskHandle.waitAndGetFinishedExecution()
  expect(parentTaskWithFailingFinalizeTaskExecution.status).toBe('finalize_task_failed')
  assert(parentTaskWithFailingFinalizeTaskExecution.status === 'finalize_task_failed')
  expect(parentTaskWithFailingFinalizeTaskExecution.taskId).toBe('parentWithFailingFinalizeTask')
  expect(parentTaskWithFailingFinalizeTaskExecution.finalizeTaskExecutionError).toBeDefined()
  expect(parentTaskWithFailingFinalizeTaskExecution.finalizeTaskExecutionError.message).toBe(
    'Failed',
  )
  expect(parentTaskWithFailingFinalizeTaskExecution.startedAt).toBeInstanceOf(Date)
  expect(parentTaskWithFailingFinalizeTaskExecution.finishedAt).toBeInstanceOf(Date)
  expect(parentTaskWithFailingFinalizeTaskExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
    parentTaskWithFailingFinalizeTaskExecution.startedAt.getTime(),
  )

  const concurrentParentTaskHandle = await executor.enqueueTask(concurrentParentTask)
  const concurrentParentTaskExecution =
    await concurrentParentTaskHandle.waitAndGetFinishedExecution()
  expect(concurrentParentTaskExecution.status).toBe('completed')
  assert(concurrentParentTaskExecution.status === 'completed')
  expect(concurrentParentTaskExecution.taskId).toBe('concurrent_parent')
  expect(concurrentParentTaskExecution.output).toBeDefined()
  expect(concurrentParentTaskExecution.output.childrenTaskExecutionsOutputs).toHaveLength(250)
  for (const [
    i,
    childTaskOutput,
  ] of concurrentParentTaskExecution.output.childrenTaskExecutionsOutputs.entries()) {
    expect(childTaskOutput.output).toBeDefined()
    expect(childTaskOutput.output).toBe(i)
  }
  expect(concurrentParentTaskExecution.startedAt).toBeInstanceOf(Date)
}

export async function withTemporaryDirectory(fn: (dirPath: string) => Promise<void>) {
  const dirPath = await fs.mkdtemp('.tmp_')
  try {
    await fn(dirPath)
  } finally {
    await fs.rmdir(dirPath, { recursive: true })
  }
}

export async function withTemporaryFile(filename: string, fn: (file: string) => Promise<void>) {
  return withTemporaryDirectory(async (dirPath) => {
    const filePath = path.join(dirPath, filename)
    await fn(filePath)
  })
}

describe('index', () => {
  afterAll(async () => {
    const tmpDir = process.cwd()
    try {
      const files = await fs.readdir(tmpDir)
      for (const file of files) {
        if (file.startsWith('.tmp_')) {
          const fullPath = path.join(tmpDir, file)
          try {
            await fs.rmdir(fullPath, { recursive: true })
          } catch {
            // ignore errors
          }
        }
      }
    } catch {
      // ignore errors
    }
  })

  it('should complete with in memory storage', { timeout: 60_000 }, async () => {
    const storage = new InMemoryStorage({ enableDebug: false })
    await runStorageTest(storage)
  })

  it('should complete with pg storage', { timeout: 60_000 }, async () => {
    await withTemporaryDirectory(async (dirPath) => {
      const table = createTaskExecutionsPgTable()
      const db = drizzlePglite(dirPath)
      const { apply } = await pushSchema({ table }, db)
      await apply()

      const storage = createPgStorage(db, table)
      await runStorageTest(storage)
    })
  })

  it('should complete with sqlite storage', { timeout: 60_000 }, async () => {
    await withTemporaryFile('test.db', async (filePath) => {
      const table = createTaskExecutionsSQLiteTable()
      const db = drizzleLibsql(`file:${filePath}`)
      const { apply } = await pushSQLiteSchema({ table }, db)
      await apply()

      const storage = createSQLiteStorage(db, table)
      await runStorageTest(storage)
    })
  })
})
