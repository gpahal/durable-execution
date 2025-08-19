import fs from 'node:fs/promises'
import path from 'node:path'

import {
  ChildTask,
  DurableExecutor,
  FINISHED_TASK_EXECUTION_STATUSES,
  type ParentTaskExecutionSummary,
  type Task,
  type TaskExecutionsStorage,
  type TaskExecutionStorageGetByIdFilters,
  type TaskExecutionStorageValue,
  type TaskExecutionSummary,
  type TaskRetryOptions,
} from 'durable-execution'
import { customAlphabet } from 'nanoid'

import { sleep } from '@gpahal/std/promises'

/**
 * Runs a comprehensive test suite to validate a TaskExecutionsStorage implementation.
 *
 * This function executes an extensive set of tests that verify the correctness of a storage
 * implementation, including ACID compliance, concurrent access patterns, and all required
 * storage operations. It tests both high-level DurableExecutor functionality and low-level
 * storage operations.
 *
 * The test suite includes:
 * - Task execution lifecycle (ready → running → completed/failed)
 * - Parent-child task relationships and hierarchies
 * - Sequential task chains
 * - Retry mechanisms and error handling
 * - Concurrent task execution (250+ parallel tasks)
 * - Task expiration and timeout handling
 * - Promise cancellation flows
 * - Atomic transactions and batch operations
 * - Race condition handling
 *
 * @example
 * ```ts
 * import { InMemoryTaskExecutionsStorage } from 'durable-execution'
 * import { runStorageTest } from 'durable-execution-storage-test-utils'
 *
 * const storage = new InMemoryTaskExecutionsStorage()
 * await runStorageTest(storage)
 * ```
 *
 * @example
 * ```ts
 * // With cleanup for database storage
 * const storage = new CustomTaskExecutionsStorage({ url: process.env.DATABASE_URL! })
 * await runStorageTest(storage, async () => {
 *   await db.delete(taskExecutions)
 * })
 * ```
 *
 *
 * @param storage - The TaskExecutionsStorage implementation to test
 * @param cleanup - Optional cleanup function to run after tests complete (e.g., to remove test
 *   database)
 * @throws Will throw if any storage operation fails validation or doesn't meet ACID requirements
 */
export async function runStorageTest(
  storage: TaskExecutionsStorage,
  cleanup?: () => void | Promise<void>,
) {
  const executor = new DurableExecutor(storage, {
    logLevel: 'error',
    backgroundProcessIntraBatchSleepMs: 50,
  })
  executor.startBackgroundProcesses()

  try {
    try {
      await runDurableExecutorTest(executor, storage)
    } finally {
      await executor.shutdown()
    }

    await runStorageOperationsTest(storage)
  } finally {
    if (cleanup) {
      await cleanup()
    }
  }
}

async function runDurableExecutorTest(executor: DurableExecutor, storage: TaskExecutionsStorage) {
  const taskB1 = executor.task({
    id: 'b1',
    timeoutMs: 5000,
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
    timeoutMs: 5000,
    run: (ctx, input: { name: string; taskB1Output: string; taskB2Output: string }) => {
      return {
        taskB1Output: input.taskB1Output,
        taskB2Output: input.taskB2Output,
        taskB3Output: `Hello from task B3, ${input.name}!`,
      }
    },
  })
  const taskB = executor.sequentialTasks('b', taskB1, taskB2, taskB3)

  const taskA1 = executor.task({
    id: 'a1',
    timeoutMs: 5000,
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
    timeoutMs: 5000,
    run: (ctx, input: { name: string }) => {
      return `Hello from task A3, ${input.name}!`
    },
  })
  const taskA = executor.parentTask({
    id: 'a',
    timeoutMs: 5000,
    runParent: (ctx, input: { name: string }) => {
      return {
        output: `Hello from task A, ${input.name}!`,
        children: [
          new ChildTask(taskA1, { name: input.name }),
          new ChildTask(taskA2, { name: input.name }),
          new ChildTask(taskA3, { name: input.name }),
        ],
      }
    },
    finalize: {
      id: 'taskAFinalize',
      timeoutMs: 5000,
      run: (ctx, { output, children }) => {
        const child0 = children[0]!
        const child1 = children[1]!
        const child2 = children[2]!
        if (
          child0.status !== 'completed' ||
          child1.status !== 'completed' ||
          child2.status !== 'completed'
        ) {
          throw new Error('Failed')
        }

        return {
          taskAOutput: output,
          taskA1Output: child0.output as string,
          taskA2Output: child1.output as string,
          taskA3Output: child2.output as string,
        }
      },
    },
  })

  const rootTask = executor.parentTask({
    id: 'root',
    timeoutMs: 5000,
    runParent: (ctx, input: { name: string }) => {
      return {
        output: `Hello from root task, ${input.name}!`,
        children: [
          new ChildTask(taskA, { name: input.name }),
          new ChildTask(taskB, { name: input.name }),
        ],
      }
    },
    finalize: {
      id: 'rootFinalize',
      timeoutMs: 5000,
      run: (ctx, { output, children }) => {
        const child0 = children[0]!
        const child1 = children[1]!
        if (child0.status !== 'completed' || child1.status !== 'completed') {
          throw new Error('Failed')
        }

        const taskAOutput = child0.output as {
          taskAOutput: string
          taskA1Output: string
          taskA2Output: string
          taskA3Output: string
        }
        const taskBOutput = child1.output as {
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
  for (let i = 0; i < 250; i++) {
    concurrentTasks.push(
      executor.task({
        id: `t${i}`,
        timeoutMs: 5000,
        run: async (ctx, input: string) => {
          await sleep(10 * Math.random())
          return `Hello from task T${i}, ${input}!`
        },
      }),
    )
  }

  const retryTask = executor.task({
    id: 'retry',
    retryOptions: {
      maxAttempts: 3,
    },
    timeoutMs: 5000,
    run: (ctx) => {
      if (ctx.attempt < 2) {
        throw new Error('Failed')
      }
      return 'Success'
    },
  })

  const failingTask = executor.task({
    id: 'failing',
    timeoutMs: 5000,
    run: () => {
      throw new Error('Failed')
    },
  })

  const parentTaskWithFailingChild = executor.parentTask({
    id: 'parentWithFailingChild',
    timeoutMs: 5000,
    runParent: () => {
      return {
        output: undefined,
        children: [new ChildTask(failingTask)],
      }
    },
    finalize: {
      id: 'parentWithFailingChildFinalize',
      timeoutMs: 1000,
      run: (ctx, { children }) => {
        const child = children[0]!
        if (child.status !== 'completed') {
          throw new Error('Failed')
        }
        return 'success'
      },
    },
  })

  const parentTaskWithFailingFinalizeTask = executor.parentTask({
    id: 'parentWithFailingFinalizeTask',
    timeoutMs: 5000,
    runParent: () => {
      return {
        output: undefined,
        children: [new ChildTask(taskA1, { name: 'world' })],
      }
    },
    finalize: {
      id: 'parentWithFailingFinalizeTaskFinalize',
      timeoutMs: 1000,
      run: () => {
        throw new Error('Failed')
      },
    },
  })

  const concurrentChildTask = executor.task({
    id: 'concurrent_child',
    timeoutMs: 10_000,
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
        children: Array.from({ length: 500 }, (_, index) => ({
          task: concurrentChildTask,
          input: index,
        })),
      }
    },
  })

  let value: number | undefined
  setTimeout(() => {
    value = 10
  }, 1000)

  const pollTask = executor.task({
    id: 'poll',
    sleepMsBeforeRun: 100,
    timeoutMs: 1000,
    run: () => {
      return value == null
        ? {
            isDone: false,
          }
        : {
            isDone: true,
            output: value,
          }
    },
  })
  const pollingTask = executor.pollingTask('polling', pollTask, 20, 100)

  const sleepingTask = executor.sleepingTask<string>({
    id: 'test',
    timeoutMs: 300_000,
  })

  const parentTaskWithSleepingTask = executor.parentTask({
    id: 'parent',
    timeoutMs: 1000,
    runParent: () => {
      return {
        output: 'parent_output',
        children: [new ChildTask(sleepingTask, 'test_unique_id')],
      }
    },
    finalize: {
      id: 'finalizeTask',
      timeoutMs: 1000,
      run: (ctx, { children }) => {
        const child = children[0]!
        if (child.status !== 'completed') {
          throw new Error('Finalize task failed')
        }
        return child.output
      },
    },
  })

  let closeTestExecutionCount = 0
  const closeTestTask = executor.task({
    id: 'closeTestTask',
    timeoutMs: 1000,
    run: () => {
      closeTestExecutionCount++
      return 'test'
    },
  })

  const closeTestParentTask = executor.parentTask({
    id: 'closeTestParentTask',
    timeoutMs: 1000,
    runParent: () => {
      return { output: 'parent_output', children: [new ChildTask(closeTestTask)] }
    },
  })

  console.log('=> Enqueuing tasks')
  const rootTaskHandle = await executor.enqueueTask(rootTask, { name: 'world' })
  const concurrentTaskHandles = await Promise.all(
    concurrentTasks.map((task) => executor.enqueueTask(task, 'world')),
  )
  const retryTaskHandle = await executor.enqueueTask(retryTask)
  const failingTaskHandle = await executor.enqueueTask(failingTask)
  const parentTaskWithFailingChildHandle = await executor.enqueueTask(parentTaskWithFailingChild)
  const parentTaskWithFailingFinalizeTaskHandle = await executor.enqueueTask(
    parentTaskWithFailingFinalizeTask,
  )
  const concurrentParentTaskHandle = await executor.enqueueTask(concurrentParentTask)
  const pollingTaskHandle = await executor.enqueueTask(pollingTask)
  const parentTaskWithSleepingTaskHandle = await executor.enqueueTask(parentTaskWithSleepingTask)
  const closeTestTaskHandle = await executor.enqueueTask(closeTestParentTask)

  console.log('=> Waiting for root task')
  const finishedExecution = await rootTaskHandle.waitAndGetFinishedExecution({
    pollingIntervalMs: 50,
  })
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

  console.log('=> Waiting for concurrent tasks')
  const concurrentFinishedExecutions = await Promise.all(
    concurrentTaskHandles.map((handle) =>
      handle.waitAndGetFinishedExecution({
        pollingIntervalMs: 50,
      }),
    ),
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

  console.log('=> Waiting for retry task')
  const retryExecution = await retryTaskHandle.waitAndGetFinishedExecution({
    pollingIntervalMs: 50,
  })
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
  const failingExecution = await failingTaskHandle.waitAndGetFinishedExecution({
    pollingIntervalMs: 50,
  })
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

  console.log('=> Waiting for parent task with failing child')
  const parentTaskWithFailingChildExecution =
    await parentTaskWithFailingChildHandle.waitAndGetFinishedExecution({
      pollingIntervalMs: 50,
    })
  expect(parentTaskWithFailingChildExecution.status).toBe('finalize_failed')
  assert(parentTaskWithFailingChildExecution.status === 'finalize_failed')
  expect(parentTaskWithFailingChildExecution.taskId).toBe('parentWithFailingChild')
  expect(parentTaskWithFailingChildExecution.error).toBeDefined()
  expect(parentTaskWithFailingChildExecution.error.message).toBe('Failed')
  expect(parentTaskWithFailingChildExecution.startedAt).toBeInstanceOf(Date)
  expect(parentTaskWithFailingChildExecution.finishedAt).toBeInstanceOf(Date)
  expect(parentTaskWithFailingChildExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
    parentTaskWithFailingChildExecution.startedAt.getTime(),
  )

  console.log('=> Waiting for parent task with failing finalize task')
  const parentTaskWithFailingFinalizeTaskExecution =
    await parentTaskWithFailingFinalizeTaskHandle.waitAndGetFinishedExecution({
      pollingIntervalMs: 50,
    })
  expect(parentTaskWithFailingFinalizeTaskExecution.status).toBe('finalize_failed')
  assert(parentTaskWithFailingFinalizeTaskExecution.status === 'finalize_failed')
  expect(parentTaskWithFailingFinalizeTaskExecution.taskId).toBe('parentWithFailingFinalizeTask')
  expect(parentTaskWithFailingFinalizeTaskExecution.error).toBeDefined()
  expect(parentTaskWithFailingFinalizeTaskExecution.error.message).toBe('Failed')
  expect(parentTaskWithFailingFinalizeTaskExecution.startedAt).toBeInstanceOf(Date)
  expect(parentTaskWithFailingFinalizeTaskExecution.finishedAt).toBeInstanceOf(Date)
  expect(parentTaskWithFailingFinalizeTaskExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
    parentTaskWithFailingFinalizeTaskExecution.startedAt.getTime(),
  )

  console.log('=> Waiting for concurrent parent task')
  const concurrentParentTaskExecution =
    await concurrentParentTaskHandle.waitAndGetFinishedExecution({
      pollingIntervalMs: 50,
    })
  expect(concurrentParentTaskExecution.status).toBe('completed')
  assert(concurrentParentTaskExecution.status === 'completed')
  expect(concurrentParentTaskExecution.taskId).toBe('concurrent_parent')
  expect(concurrentParentTaskExecution.output).toBeDefined()
  expect(concurrentParentTaskExecution.output.children).toHaveLength(500)
  for (const [i, childTaskOutput] of concurrentParentTaskExecution.output.children.entries()) {
    expect(childTaskOutput.status).toBe('completed')
    assert(childTaskOutput.status === 'completed')
    expect(childTaskOutput.output).toBeDefined()
    expect(childTaskOutput.output).toBe(i)
  }
  expect(concurrentParentTaskExecution.startedAt).toBeInstanceOf(Date)

  console.log('=> Waiting for polling task')
  const pollingTaskExecution = await pollingTaskHandle.waitAndGetFinishedExecution({
    pollingIntervalMs: 250,
  })
  expect(pollingTaskExecution.status).toBe('completed')
  assert(pollingTaskExecution.status === 'completed')
  expect(pollingTaskExecution.taskId).toBe('polling')
  expect(pollingTaskExecution.output).toBeDefined()
  expect(pollingTaskExecution.output.isSuccess).toBe(true)
  assert(pollingTaskExecution.output.isSuccess)
  expect(pollingTaskExecution.output.output).toBe(10)
  expect(pollingTaskExecution.startedAt).toBeInstanceOf(Date)
  expect(pollingTaskExecution.finishedAt).toBeInstanceOf(Date)
  expect(pollingTaskExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
    pollingTaskExecution.startedAt.getTime(),
  )

  console.log('=> Waiting for parent task with sleeping task')
  while (true) {
    const execution = await parentTaskWithSleepingTaskHandle.getExecution()
    if (
      FINISHED_TASK_EXECUTION_STATUSES.includes(execution.status) ||
      execution.status === 'waiting_for_children'
    ) {
      break
    }
    await sleep(100)
  }

  await sleep(100)
  const sleepingTaskExecution = await executor.wakeupSleepingTaskExecution(
    sleepingTask,
    'test_unique_id',
    {
      status: 'completed',
      output: 'sleeping_task_output',
    },
  )
  expect(sleepingTaskExecution.status).toBe('completed')
  assert(sleepingTaskExecution.status === 'completed')
  expect(sleepingTaskExecution.output).toBe('sleeping_task_output')

  const parentTaskWithSleepingTaskFinishedExecution =
    await parentTaskWithSleepingTaskHandle.waitAndGetFinishedExecution()
  expect(parentTaskWithSleepingTaskFinishedExecution.status).toBe('completed')
  assert(parentTaskWithSleepingTaskFinishedExecution.status === 'completed')
  expect(parentTaskWithSleepingTaskFinishedExecution.output).toBe('sleeping_task_output')

  console.log('=> Waiting for close test task')
  const closeTestFinishedExecution = await closeTestTaskHandle.waitAndGetFinishedExecution()
  expect(closeTestExecutionCount).toBe(1)
  expect(closeTestFinishedExecution.status).toBe('completed')
  assert(closeTestFinishedExecution.status === 'completed')
  expect(closeTestFinishedExecution.output.output).toBe('parent_output')

  for (let i = 0; ; i++) {
    const executionStorageValue = await storage.getById(closeTestFinishedExecution.executionId, {})
    expect(executionStorageValue).toBeDefined()
    assert(executionStorageValue)
    expect(executionStorageValue.status).toBe('completed')
    expect(executionStorageValue.onChildrenFinishedProcessingStatus).toBe('processed')
    expect(executionStorageValue.onChildrenFinishedProcessingFinishedAt).toBeInstanceOf(Date)
    assert(executionStorageValue.onChildrenFinishedProcessingFinishedAt)

    if (executionStorageValue.closeStatus === 'closed') {
      expect(executionStorageValue.closedAt).toBeInstanceOf(Date)
      assert(executionStorageValue.closedAt)
      expect(executionStorageValue.closedAt.getTime()).toBeGreaterThanOrEqual(
        executionStorageValue.onChildrenFinishedProcessingFinishedAt.getTime(),
      )
      break
    }

    if (i >= 50) {
      throw new Error('closeTestParentTask not closed')
    }
    await sleep(100)
  }
}

async function runStorageOperationsTest(storage: TaskExecutionsStorage) {
  console.log('=> Running storage operations test')
  await storage.deleteAll()
  await testInsertMany(storage)
  await storage.deleteAll()
  await testGetById(storage)
  await storage.deleteAll()
  await testGetBySleepingTaskUniqueId(storage)
  await storage.deleteAll()
  await testUpdateById(storage)
  await storage.deleteAll()
  await testUpdateByIdAndInsertManyIfUpdated(storage)
  await storage.deleteAll()
  await testUpdateByStatusAndStartAtLessThanAndReturn(storage)
  await storage.deleteAll()
  await testUpdateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn(
    storage,
  )
  await storage.deleteAll()
  await testUpdateByCloseStatusAndReturn(storage)
  await storage.deleteAll()
  await testUpdateByExpiresAtLessThanAndReturn(storage)
  await storage.deleteAll()
  await testUpdateByOnChildrenFinishedProcessingExpiresAtLessThanAndReturn(storage)
  await storage.deleteAll()
  await testUpdateByCloseExpiresAtLessThanAndReturn(storage)
  await storage.deleteAll()
  await testUpdateByExecutorIdAndNeedsPromiseCancellationAndReturn(storage)
  await storage.deleteAll()
  await testGetByParentExecutionId(storage)
  await storage.deleteAll()
  await testUpdateByParentExecutionIdAndIsFinished(storage)
  await storage.deleteAll()
  await testUpdateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(storage)
  await storage.deleteAll()
}

async function testInsertMany(storage: TaskExecutionsStorage) {
  const executions = [
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue({
      root: {
        taskId: generateTaskId(),
        executionId: generateTaskExecutionId(),
      },
      parent: {
        taskId: generateTaskId(),
        executionId: generateTaskExecutionId(),
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
    }),
  ]
  executions[1]!.isSleepingTask = true
  executions[1]!.sleepingTaskUniqueId = 'test_unique_id'
  await storage.insertMany(executions)

  for (const execution of executions) {
    const insertedExecution = await storage.getById(execution.executionId, {})
    expect(insertedExecution).toBeDefined()
    assert(insertedExecution)
    expect(insertedExecution.root).toStrictEqual(execution.root)
    expect(insertedExecution.parent).toStrictEqual(execution.parent)
    expect(insertedExecution.taskId).toEqual(execution.taskId)
    expect(insertedExecution.executionId).toEqual(execution.executionId)
    expect(insertedExecution.retryOptions).toEqual(execution.retryOptions)
    expect(insertedExecution.sleepMsBeforeRun).toEqual(execution.sleepMsBeforeRun)
    expect(insertedExecution.timeoutMs).toEqual(execution.timeoutMs)
    expect(insertedExecution.input).toEqual(execution.input)
    expect(insertedExecution.executorId).toBeUndefined()
    expect(insertedExecution.status).toEqual('ready')
    expect(insertedExecution.isFinished).toEqual(false)
    expect(insertedExecution.runOutput).toBeUndefined()
    expect(insertedExecution.output).toBeUndefined()
    expect(insertedExecution.error).toBeUndefined()
    expect(insertedExecution.retryAttempts).toEqual(0)
    expect(insertedExecution.startAt).toBeInstanceOf(Date)
    expect(insertedExecution.startedAt).toBeUndefined()
    expect(insertedExecution.expiresAt).toBeUndefined()
    expect(insertedExecution.finishedAt).toBeUndefined()
    expect(insertedExecution.children).toBeUndefined()
    expect(insertedExecution.activeChildrenCount).toEqual(0)
    expect(insertedExecution.onChildrenFinishedProcessingStatus).toEqual('idle')
    expect(insertedExecution.onChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(insertedExecution.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(insertedExecution.closeStatus).toEqual('idle')
    expect(insertedExecution.closeExpiresAt).toBeUndefined()
    expect(insertedExecution.closedAt).toBeUndefined()
    expect(insertedExecution.needsPromiseCancellation).toEqual(false)
    expect(insertedExecution.createdAt).toBeInstanceOf(Date)
    expect(insertedExecution.updatedAt).toBeInstanceOf(Date)
    expectDateApproximatelyEqual(insertedExecution.createdAt, insertedExecution.updatedAt)
  }

  const duplicateExecution = createTaskExecutionStorageValue()
  duplicateExecution.executionId = executions[0]!.executionId
  await expect(storage.insertMany([duplicateExecution])).rejects.toThrow()

  const duplicateSleepingTaskExecution = createTaskExecutionStorageValue({
    isSleepingTask: true,
    sleepingTaskUniqueId: executions[1]!.sleepingTaskUniqueId,
  })
  await expect(storage.insertMany([duplicateSleepingTaskExecution])).rejects.toThrow()
}

async function testGetById(storage: TaskExecutionsStorage) {
  const execution1 = createTaskExecutionStorageValue()
  const execution2 = createTaskExecutionStorageValue()

  execution2.status = 'completed'
  execution2.isFinished = true
  execution2.output = 'output'
  execution2.retryAttempts = 1
  execution2.finishedAt = new Date()
  execution2.children = [
    {
      taskId: generateTaskId(),
      executionId: generateTaskExecutionId(),
    },
  ]
  execution2.activeChildrenCount = 0
  execution2.closeStatus = 'closing'
  execution2.closeExpiresAt = new Date()
  await storage.insertMany([execution1, execution2])

  const foundExecution1 = await storage.getById(execution1.executionId, {})
  expect(foundExecution1).toBeDefined()
  assert(foundExecution1)
  expect(foundExecution1.taskId).toBe(execution1.taskId)
  expect(foundExecution1.executionId).toEqual(execution1.executionId)
  expect(foundExecution1.status).toEqual('ready')

  const execution2MatchingFilters: Array<TaskExecutionStorageGetByIdFilters> = [
    {},
    { status: 'completed' },
    { status: 'completed', isFinished: true },
    { isFinished: true },
    { isSleepingTask: false },
    { isSleepingTask: false, status: 'completed' },
    { isSleepingTask: false, status: 'completed', isFinished: true },
    { isSleepingTask: false, isFinished: true },
  ]

  for (const filter of execution2MatchingFilters) {
    const foundExecution2 = await storage.getById(execution2.executionId, filter)
    expect(foundExecution2).toBeDefined()
    assert(foundExecution2)
    expect(foundExecution2.taskId).toBe(execution2.taskId)
    expect(foundExecution2.executionId).toEqual(execution2.executionId)
    expect(foundExecution2.status).toEqual('completed')
    expect(foundExecution2.isFinished).toEqual(true)
    expect(foundExecution2.output).toEqual('output')
    expect(foundExecution2.error).toBeUndefined()
    expect(foundExecution2.retryAttempts).toEqual(1)
    expectDateApproximatelyEqual(foundExecution2.finishedAt!, execution2.finishedAt)
    expect(foundExecution2.children).toBeDefined()
    assert(foundExecution2.children)
    expect(foundExecution2.children).toHaveLength(1)
    expect(foundExecution2.children[0]!.taskId).toEqual(execution2.children[0]!.taskId)
    expect(foundExecution2.children[0]!.executionId).toEqual(execution2.children[0]!.executionId)
    expect(foundExecution2.activeChildrenCount).toEqual(0)
    expect(foundExecution2.onChildrenFinishedProcessingStatus).toEqual('idle')
    expect(foundExecution2.onChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(foundExecution2.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(foundExecution2.closeStatus).toEqual('closing')
    expectDateApproximatelyEqual(foundExecution2.closeExpiresAt!, execution2.closeExpiresAt)
    expect(foundExecution2.closedAt).toBeUndefined()
  }

  const execution2NonFiltersFilters: Array<TaskExecutionStorageGetByIdFilters> = [
    { status: 'ready' },
    { status: 'completed', isFinished: false },
    { isFinished: false },
    { isSleepingTask: true },
    { isSleepingTask: true, status: 'completed' },
    { isSleepingTask: true, status: 'completed', isFinished: true },
    { isSleepingTask: true, isFinished: true },
  ]

  for (const filter of execution2NonFiltersFilters) {
    const foundExecution2 = await storage.getById(execution2.executionId, filter)
    expect(foundExecution2).toBeUndefined()
  }

  const nonFoundExecution = await storage.getById('invalid_execution_id', {})
  expect(nonFoundExecution).toBeUndefined()
}

async function testGetBySleepingTaskUniqueId(storage: TaskExecutionsStorage) {
  const execution1 = createTaskExecutionStorageValue()

  execution1.isSleepingTask = true
  execution1.sleepingTaskUniqueId = 'test_unique_id1'
  await storage.insertMany([execution1])

  const foundExecution1 = await storage.getBySleepingTaskUniqueId(execution1.sleepingTaskUniqueId)
  expect(foundExecution1).toBeDefined()
  assert(foundExecution1)
  expect(foundExecution1.taskId).toBe(execution1.taskId)
  expect(foundExecution1.executionId).toEqual(execution1.executionId)
  expect(foundExecution1.status).toEqual('ready')

  const nonFoundExecution = await storage.getBySleepingTaskUniqueId(
    'invalid_sleeping_task_unique_id',
  )
  expect(nonFoundExecution).toBeUndefined()
}

async function testUpdateById(storage: TaskExecutionsStorage) {
  const executionFilters: Array<TaskExecutionStorageGetByIdFilters> = [
    {},
    { status: 'ready' },
    { status: 'ready', isFinished: false },
    { isFinished: false },
    { isSleepingTask: false },
    { isSleepingTask: false, status: 'ready' },
    { isSleepingTask: false, status: 'ready', isFinished: false },
    { isSleepingTask: false, isFinished: false },
  ]

  for (const filter of executionFilters) {
    const execution = createTaskExecutionStorageValue()
    await storage.insertMany([execution])

    let now = new Date()
    const child = {
      taskId: generateTaskId(),
      executionId: generateTaskExecutionId(),
    }
    await storage.updateById(execution.executionId, filter, {
      executorId: 'executor_id',
      status: 'running',
      isFinished: true,
      runOutput: 'run_output',
      output: 'output',
      error: {
        message: 'error_message',
        errorType: 'cancelled',
        isRetryable: false,
        isInternal: false,
      },
      retryAttempts: 1,
      startedAt: now,
      expiresAt: now,
      finishedAt: now,
      children: [child],
      activeChildrenCount: 1,
      onChildrenFinishedProcessingStatus: 'processing',
      onChildrenFinishedProcessingExpiresAt: now,
      onChildrenFinishedProcessingFinishedAt: now,
      closeStatus: 'closing',
      closeExpiresAt: now,
      closedAt: now,
      needsPromiseCancellation: true,
      updatedAt: now,
    })

    const updatedExecution1 = await storage.getById(execution.executionId, {})
    expect(updatedExecution1).toBeDefined()
    assert(updatedExecution1)
    expect(updatedExecution1.executorId).toBe('executor_id')
    expect(updatedExecution1.status).toBe('running')
    expect(updatedExecution1.isFinished).toBe(true)
    expect(updatedExecution1.runOutput).toBe('run_output')
    expect(updatedExecution1.output).toBe('output')
    expect(updatedExecution1.error).toBeDefined()
    assert(updatedExecution1.error)
    expect(updatedExecution1.error.message).toBe('error_message')
    expect(updatedExecution1.error.errorType).toBe('cancelled')
    expect(updatedExecution1.error.isRetryable).toBe(false)
    expect(updatedExecution1.error.isInternal).toBe(false)
    expect(updatedExecution1.retryAttempts).toBe(1)
    expectDateApproximatelyEqual(updatedExecution1.startedAt!, now)
    expectDateApproximatelyEqual(updatedExecution1.expiresAt!, now)
    expectDateApproximatelyEqual(updatedExecution1.finishedAt!, now)
    expect(updatedExecution1.children).toBeDefined()
    assert(updatedExecution1.children)
    expect(updatedExecution1.children).toHaveLength(1)
    expect(updatedExecution1.children[0]!.taskId).toEqual(child.taskId)
    expect(updatedExecution1.children[0]!.executionId).toEqual(child.executionId)
    expect(updatedExecution1.activeChildrenCount).toBe(1)
    expect(updatedExecution1.onChildrenFinishedProcessingStatus).toBe('processing')
    expectDateApproximatelyEqual(updatedExecution1.onChildrenFinishedProcessingExpiresAt!, now)
    expectDateApproximatelyEqual(updatedExecution1.onChildrenFinishedProcessingFinishedAt!, now)
    expect(updatedExecution1.closeStatus).toBe('closing')
    expectDateApproximatelyEqual(updatedExecution1.closeExpiresAt!, now)
    expectDateApproximatelyEqual(updatedExecution1.closedAt!, now)
    expect(updatedExecution1.needsPromiseCancellation).toBe(true)
    expectDateApproximatelyEqual(updatedExecution1.updatedAt, now)

    now = new Date()
    await storage.updateById(
      execution.executionId,
      {},
      {
        unsetExecutorId: true,
        unsetRunOutput: true,
        unsetError: true,
        unsetExpiresAt: true,
        unsetOnChildrenFinishedProcessingExpiresAt: true,
        unsetCloseExpiresAt: true,
        needsPromiseCancellation: false,
        updatedAt: now,
      },
    )

    const updatedExecution2 = await storage.getById(execution.executionId, {})
    expect(updatedExecution2).toBeDefined()
    assert(updatedExecution2)
    expect(updatedExecution2.executorId).toBeUndefined()
    expect(updatedExecution2.runOutput).toBeUndefined()
    expect(updatedExecution2.error).toBeUndefined()
    expect(updatedExecution2.expiresAt).toBeUndefined()
    expect(updatedExecution2.onChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(updatedExecution2.closeExpiresAt).toBeUndefined()
    expect(updatedExecution2.needsPromiseCancellation).toBe(false)
    expectDateApproximatelyEqual(updatedExecution2.updatedAt, now)
  }
}

async function testUpdateByIdAndInsertManyIfUpdated(storage: TaskExecutionsStorage) {
  const matchingFilters: Array<TaskExecutionStorageGetByIdFilters> = [
    {},
    { status: 'ready' },
    { status: 'ready', isFinished: false },
    { isFinished: false },
    { isSleepingTask: false },
    { isSleepingTask: false, status: 'ready' },
    { isSleepingTask: false, status: 'ready', isFinished: false },
    { isSleepingTask: false, isFinished: false },
  ]

  for (const filter of matchingFilters) {
    const testExecution = createTaskExecutionStorageValue()
    const childrenExecutions = [
      createTaskExecutionStorageValue({
        parent: {
          taskId: testExecution.taskId,
          executionId: testExecution.executionId,
          indexInParentChildTaskExecutions: 0,
          isFinalizeTaskOfParentTask: false,
        },
      }),
      createTaskExecutionStorageValue({
        parent: {
          taskId: testExecution.taskId,
          executionId: testExecution.executionId,
          indexInParentChildTaskExecutions: 1,
          isFinalizeTaskOfParentTask: false,
        },
      }),
    ]

    await storage.insertMany([testExecution])

    const now = new Date()
    await storage.updateByIdAndInsertManyIfUpdated(
      testExecution.executionId,
      filter,
      {
        status: 'waiting_for_children',
        children: childrenExecutions.map((e) => ({ taskId: e.taskId, executionId: e.executionId })),
        activeChildrenCount: 2,
        updatedAt: now,
      },
      childrenExecutions,
    )

    const updatedExecution = await storage.getById(testExecution.executionId, {})
    expect(updatedExecution).toBeDefined()
    assert(updatedExecution)
    expect(updatedExecution.status).toBe('waiting_for_children')
    expect(updatedExecution.children).toBeDefined()
    assert(updatedExecution.children)
    expect(updatedExecution.children).toHaveLength(2)
    expect(updatedExecution.activeChildrenCount).toBe(2)

    for (const child of childrenExecutions) {
      const insertedChild = await storage.getById(child.executionId, {})
      expect(insertedChild).toBeDefined()
      assert(insertedChild)
      expect(insertedChild.taskId).toBe(child.taskId)
      expect(insertedChild.parent).toBeDefined()
      assert(insertedChild.parent)
      expect(insertedChild.parent.executionId).toBe(testExecution.executionId)
    }
  }

  const nonMatchingFilters: Array<TaskExecutionStorageGetByIdFilters> = [
    { status: 'running' },
    { status: 'completed' },
    { isFinished: true },
    { isSleepingTask: true },
    { isSleepingTask: true, status: 'ready' },
    { isSleepingTask: true, status: 'ready', isFinished: false },
    { isSleepingTask: true, isFinished: false },
  ]

  for (const filter of nonMatchingFilters) {
    const execution = createTaskExecutionStorageValue()
    await storage.insertMany([execution])

    const childExecution = createTaskExecutionStorageValue({
      parent: {
        taskId: execution.taskId,
        executionId: execution.executionId,
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
    })

    await storage.updateByIdAndInsertManyIfUpdated(
      execution.executionId,
      filter,
      {
        status: 'running',
        updatedAt: new Date(),
      },
      [childExecution],
    )

    const unchangedExecution = await storage.getById(execution.executionId, {})
    expect(unchangedExecution).toBeDefined()
    assert(unchangedExecution)
    expect(unchangedExecution.status).toBe('ready')

    const nonInsertedChild = await storage.getById(childExecution.executionId, {})
    expect(nonInsertedChild).toBeUndefined()
  }
}

async function testUpdateByStatusAndStartAtLessThanAndReturn(storage: TaskExecutionsStorage) {
  let now = new Date()
  const past = new Date(now.getTime() - 5000)
  const future = new Date(now.getTime() + 5000)

  const executions = [
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
  ]

  executions[0]!.startAt = past
  executions[1]!.startAt = future
  executions[2]!.startAt = now
  executions[3]!.status = 'running'
  executions[3]!.startAt = past
  executions[4]!.status = 'completed'
  executions[4]!.isFinished = true
  executions[4]!.startAt = past

  await storage.insertMany(executions)

  now = new Date(Date.now() + 1000)
  const updatedExecutions = await storage.updateByStatusAndStartAtLessThanAndReturn(
    'ready',
    new Date(future.getTime() + 1000),
    {
      status: 'running',
      executorId: 'test-executor',
      startedAt: now,
      updatedAt: now,
    },
    new Date(now.getTime() + 10_000),
    2,
  )

  expect(updatedExecutions).toHaveLength(2)
  expect(updatedExecutions[0]!.executionId).toBe(executions[0]!.executionId)
  expect(updatedExecutions[1]!.executionId).toBe(executions[2]!.executionId)
  expect(updatedExecutions[0]!.status).toBe('running')
  expect(updatedExecutions[1]!.status).toBe('running')
  expect(updatedExecutions[0]!.executorId).toBe('test-executor')
  expect(updatedExecutions[1]!.executorId).toBe('test-executor')

  const expiresAt = new Date(now.getTime() + updatedExecutions[0]!.timeoutMs + 10_000)
  expectDateApproximatelyEqual(updatedExecutions[0]!.expiresAt!, expiresAt)
  expectDateApproximatelyEqual(updatedExecutions[1]!.expiresAt!, expiresAt)

  const updatedExecution1 = await storage.getById(executions[0]!.executionId, {})
  const updatedExecution2 = await storage.getById(executions[2]!.executionId, {})
  expect(updatedExecution1?.status).toBe('running')
  expect(updatedExecution2?.status).toBe('running')

  const nonUpdatedExecution1 = await storage.getById(executions[1]!.executionId, {})
  const nonUpdatedExecution2 = await storage.getById(executions[3]!.executionId, {})
  const nonUpdatedExecution3 = await storage.getById(executions[4]!.executionId, {})
  expect(nonUpdatedExecution1?.status).toBe('ready')
  expect(nonUpdatedExecution2?.status).toBe('running')
  expect(nonUpdatedExecution3?.status).toBe('completed')

  const noMatchedExecutions = await storage.updateByStatusAndStartAtLessThanAndReturn(
    'failed',
    new Date(future.getTime() + 1000),
    { updatedAt: new Date() },
    new Date(now.getTime() + 10_000),
    10,
  )
  expect(noMatchedExecutions).toHaveLength(0)
}

async function testUpdateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn(
  storage: TaskExecutionsStorage,
) {
  let now = new Date()
  const past = new Date(now.getTime() - 1000)
  const future = new Date(now.getTime() + 1000)

  const executions = [
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
  ]

  executions[0]!.status = 'waiting_for_children'
  executions[0]!.onChildrenFinishedProcessingStatus = 'idle'
  executions[0]!.activeChildrenCount = 0
  executions[0]!.updatedAt = past

  executions[1]!.status = 'waiting_for_children'
  executions[1]!.onChildrenFinishedProcessingStatus = 'idle'
  executions[1]!.activeChildrenCount = 0
  executions[1]!.updatedAt = future

  executions[2]!.status = 'waiting_for_children'
  executions[2]!.onChildrenFinishedProcessingStatus = 'idle'
  executions[2]!.activeChildrenCount = 0
  executions[2]!.updatedAt = now

  executions[3]!.status = 'waiting_for_children'
  executions[3]!.onChildrenFinishedProcessingStatus = 'idle'
  executions[3]!.activeChildrenCount = 2

  executions[4]!.status = 'completed'
  executions[4]!.isFinished = true
  executions[4]!.onChildrenFinishedProcessingStatus = 'idle'
  executions[4]!.activeChildrenCount = 0

  await storage.insertMany(executions)

  now = new Date()
  const updatedExecutions =
    await storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn(
      'waiting_for_children',
      'idle',
      1,
      {
        onChildrenFinishedProcessingStatus: 'processing',
        onChildrenFinishedProcessingExpiresAt: new Date(now.getTime() + 10_000),
        updatedAt: now,
      },
      2,
    )

  expect(updatedExecutions).toHaveLength(2)
  expect(updatedExecutions[0]!.executionId).toBe(executions[0]!.executionId)
  expect(updatedExecutions[1]!.executionId).toBe(executions[2]!.executionId)
  expect(updatedExecutions[0]!.onChildrenFinishedProcessingStatus).toBe('processing')
  expect(updatedExecutions[1]!.onChildrenFinishedProcessingStatus).toBe('processing')

  const updatedExecution1 = await storage.getById(executions[0]!.executionId, {})
  const updatedExecution2 = await storage.getById(executions[2]!.executionId, {})
  expect(updatedExecution1?.onChildrenFinishedProcessingStatus).toBe('processing')
  expect(updatedExecution2?.onChildrenFinishedProcessingStatus).toBe('processing')
  expect(updatedExecution1?.onChildrenFinishedProcessingExpiresAt).toBeInstanceOf(Date)
  expect(updatedExecution2?.onChildrenFinishedProcessingExpiresAt).toBeInstanceOf(Date)

  const nonUpdatedExecution1 = await storage.getById(executions[1]!.executionId, {})
  const nonUpdatedExecution2 = await storage.getById(executions[3]!.executionId, {})
  const nonUpdatedExecution3 = await storage.getById(executions[4]!.executionId, {})
  expect(nonUpdatedExecution1?.onChildrenFinishedProcessingStatus).toBe('idle')
  expect(nonUpdatedExecution2?.onChildrenFinishedProcessingStatus).toBe('idle')
  expect(nonUpdatedExecution3?.onChildrenFinishedProcessingStatus).toBe('idle')
}

async function testUpdateByCloseStatusAndReturn(storage: TaskExecutionsStorage) {
  let now = new Date()
  const past = new Date(now.getTime() - 1000)
  const future = new Date(now.getTime() + 1000)

  const executions = [
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
  ]

  executions[0]!.closeStatus = 'ready'
  executions[0]!.updatedAt = past
  executions[1]!.closeStatus = 'ready'
  executions[1]!.updatedAt = future
  executions[2]!.closeStatus = 'ready'
  executions[2]!.updatedAt = now
  executions[3]!.closeStatus = 'closing'
  executions[3]!.updatedAt = past
  executions[4]!.closeStatus = 'closed'
  executions[4]!.updatedAt = past

  await storage.insertMany(executions)

  now = new Date()
  const updatedExecutions = await storage.updateByCloseStatusAndReturn(
    'ready',
    {
      closeStatus: 'closing',
      closeExpiresAt: new Date(now.getTime() + 10_000),
      updatedAt: now,
    },
    2,
  )

  expect(updatedExecutions).toHaveLength(2)
  expect(updatedExecutions[0]!.executionId).toBe(executions[0]!.executionId)
  expect(updatedExecutions[1]!.executionId).toBe(executions[2]!.executionId)
  expect(updatedExecutions[0]!.closeStatus).toBe('closing')
  expect(updatedExecutions[1]!.closeStatus).toBe('closing')

  const updatedExecution1 = await storage.getById(executions[0]!.executionId, {})
  const updatedExecution2 = await storage.getById(executions[2]!.executionId, {})
  expect(updatedExecution1?.closeStatus).toBe('closing')
  expect(updatedExecution2?.closeStatus).toBe('closing')
  expect(updatedExecution1?.closeExpiresAt).toBeInstanceOf(Date)
  expect(updatedExecution2?.closeExpiresAt).toBeInstanceOf(Date)

  const nonUpdatedExecution1 = await storage.getById(executions[1]!.executionId, {})
  const nonUpdatedExecution2 = await storage.getById(executions[3]!.executionId, {})
  const nonUpdatedExecution3 = await storage.getById(executions[4]!.executionId, {})
  expect(nonUpdatedExecution1?.closeStatus).toBe('ready')
  expect(nonUpdatedExecution2?.closeStatus).toBe('closing')
  expect(nonUpdatedExecution3?.closeStatus).toBe('closed')
}

async function testUpdateByExpiresAtLessThanAndReturn(storage: TaskExecutionsStorage) {
  let now = new Date()
  const past = new Date(now.getTime() - 5000)
  const future = new Date(now.getTime() + 5000)

  const executions = [
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
  ]

  executions[0]!.status = 'running'
  executions[0]!.expiresAt = past
  executions[1]!.status = 'running'
  executions[1]!.expiresAt = future
  executions[2]!.status = 'running'
  executions[2]!.expiresAt = now
  executions[3]!.status = 'completed'
  executions[3]!.isFinished = true
  executions[4]!.isSleepingTask = true
  executions[4]!.expiresAt = past

  await storage.insertMany(executions)

  now = new Date(Date.now() + 1000)
  let updatedExecutions = await storage.updateByIsSleepingTaskAndExpiresAtLessThanAndReturn(
    false,
    now,
    {
      status: 'ready',
      error: {
        message: 'Task expired',
        errorType: 'generic',
        isRetryable: true,
        isInternal: true,
      },
      startAt: now,
      updatedAt: now,
    },
    2,
  )

  expect(updatedExecutions).toHaveLength(2)
  expect(updatedExecutions[0]!.executionId).toBe(executions[0]!.executionId)
  expect(updatedExecutions[1]!.executionId).toBe(executions[2]!.executionId)
  expect(updatedExecutions[0]!.status).toBe('ready')
  expect(updatedExecutions[1]!.status).toBe('ready')
  expect(updatedExecutions[0]!.error).toBeDefined()
  assert(updatedExecutions[0]!.error)
  expect(updatedExecutions[0]!.error.message).toBe('Task expired')
  expect(updatedExecutions[0]!.startAt).toBeInstanceOf(Date)
  expect(updatedExecutions[1]!.error).toBeDefined()
  assert(updatedExecutions[1]!.error)
  expect(updatedExecutions[1]!.error.message).toBe('Task expired')
  expect(updatedExecutions[1]!.startAt).toBeInstanceOf(Date)

  const updatedExecution1 = await storage.getById(executions[0]!.executionId, {})
  const updatedExecution2 = await storage.getById(executions[2]!.executionId, {})
  expect(updatedExecution1?.status).toBe('ready')
  expect(updatedExecution2?.status).toBe('ready')
  expect(updatedExecution1?.startAt).toBeInstanceOf(Date)
  expect(updatedExecution2?.startAt).toBeInstanceOf(Date)

  const nonUpdatedExecution1 = await storage.getById(executions[1]!.executionId, {})
  const nonUpdatedExecution2 = await storage.getById(executions[3]!.executionId, {})
  const nonUpdatedExecution3 = await storage.getById(executions[4]!.executionId, {})
  expect(nonUpdatedExecution1?.status).toBe('running')
  expect(nonUpdatedExecution2?.status).toBe('completed')
  expect(nonUpdatedExecution3?.status).toBe('ready')

  now = new Date(Date.now() + 1000)
  updatedExecutions = await storage.updateByIsSleepingTaskAndExpiresAtLessThanAndReturn(
    true,
    now,
    {
      status: 'timed_out',
      error: {
        message: 'Task timed out',
        errorType: 'timed_out',
        isRetryable: true,
        isInternal: false,
      },
      updatedAt: now,
    },
    2,
  )

  expect(updatedExecutions).toHaveLength(1)
  expect(updatedExecutions[0]!.executionId).toBe(executions[4]!.executionId)
  expect(updatedExecutions[0]!.status).toBe('timed_out')
  expect(updatedExecutions[0]!.error).toBeDefined()
  assert(updatedExecutions[0]!.error)
  expect(updatedExecutions[0]!.error.message).toBe('Task timed out')
  expect(updatedExecutions[0]!.error.errorType).toBe('timed_out')
  expect(updatedExecutions[0]!.error.isRetryable).toBe(true)
  expect(updatedExecutions[0]!.error.isInternal).toBe(false)
}

async function testUpdateByOnChildrenFinishedProcessingExpiresAtLessThanAndReturn(
  storage: TaskExecutionsStorage,
) {
  let now = new Date()
  const past = new Date(now.getTime() - 5000)
  const future = new Date(now.getTime() + 5000)

  const executions = [
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
  ]

  executions[0]!.onChildrenFinishedProcessingStatus = 'processing'
  executions[0]!.onChildrenFinishedProcessingExpiresAt = past
  executions[1]!.onChildrenFinishedProcessingStatus = 'processing'
  executions[1]!.onChildrenFinishedProcessingExpiresAt = future
  executions[2]!.onChildrenFinishedProcessingStatus = 'processing'
  executions[2]!.onChildrenFinishedProcessingExpiresAt = now
  executions[3]!.onChildrenFinishedProcessingStatus = 'processed'
  executions[3]!.updatedAt = new Date(past.getTime() - 10_000)

  await storage.insertMany(executions)

  now = new Date(Date.now() + 1000)
  const updatedExecutions =
    await storage.updateByOnChildrenFinishedProcessingExpiresAtLessThanAndReturn(
      now,
      {
        onChildrenFinishedProcessingStatus: 'processed',
        unsetOnChildrenFinishedProcessingExpiresAt: true,
        updatedAt: now,
      },
      2,
    )

  expect(updatedExecutions).toHaveLength(2)
  expect(updatedExecutions[0]!.executionId).toBe(executions[0]!.executionId)
  expect(updatedExecutions[1]!.executionId).toBe(executions[2]!.executionId)
  expect(updatedExecutions[0]!.onChildrenFinishedProcessingStatus).toBe('processed')
  expect(updatedExecutions[1]!.onChildrenFinishedProcessingStatus).toBe('processed')

  const updatedExecution1 = await storage.getById(executions[0]!.executionId, {})
  const updatedExecution2 = await storage.getById(executions[2]!.executionId, {})
  expect(updatedExecution1?.onChildrenFinishedProcessingStatus).toBe('processed')
  expect(updatedExecution2?.onChildrenFinishedProcessingStatus).toBe('processed')
  expect(updatedExecution1?.onChildrenFinishedProcessingExpiresAt).toBeUndefined()
  expect(updatedExecution2?.onChildrenFinishedProcessingExpiresAt).toBeUndefined()

  const nonUpdatedExecution1 = await storage.getById(executions[1]!.executionId, {})
  const nonUpdatedExecution2 = await storage.getById(executions[3]!.executionId, {})
  expect(nonUpdatedExecution1?.onChildrenFinishedProcessingStatus).toBe('processing')
  expect(nonUpdatedExecution2?.onChildrenFinishedProcessingStatus).toBe('processed')
  expectDateApproximatelyEqual(nonUpdatedExecution1!.onChildrenFinishedProcessingExpiresAt!, future)
  expect(nonUpdatedExecution2?.updatedAt.getTime()).toBeLessThan(now.getTime())
}

async function testUpdateByCloseExpiresAtLessThanAndReturn(storage: TaskExecutionsStorage) {
  let now = new Date()
  const past = new Date(now.getTime() - 5000)
  const future = new Date(now.getTime() + 5000)

  const executions = [
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
  ]

  executions[0]!.closeStatus = 'closing'
  executions[0]!.closeExpiresAt = past
  executions[1]!.closeStatus = 'closing'
  executions[1]!.closeExpiresAt = future
  executions[2]!.closeStatus = 'closing'
  executions[2]!.closeExpiresAt = now
  executions[3]!.closeStatus = 'ready'
  executions[3]!.updatedAt = new Date(past.getTime() - 10_000)

  await storage.insertMany(executions)

  now = new Date(Date.now() + 1000)
  const updatedExecutions = await storage.updateByCloseExpiresAtLessThanAndReturn(
    now,
    {
      closeStatus: 'closed',
      unsetCloseExpiresAt: true,
      updatedAt: now,
    },
    2,
  )

  expect(updatedExecutions).toHaveLength(2)
  expect(updatedExecutions[0]!.executionId).toBe(executions[0]!.executionId)
  expect(updatedExecutions[1]!.executionId).toBe(executions[2]!.executionId)
  expect(updatedExecutions[0]!.closeStatus).toBe('closed')
  expect(updatedExecutions[1]!.closeStatus).toBe('closed')
  expect(updatedExecutions[0]!.closeExpiresAt).toBeUndefined()
  expect(updatedExecutions[1]!.closeExpiresAt).toBeUndefined()

  const updatedExecution1 = await storage.getById(executions[0]!.executionId, {})
  const updatedExecution2 = await storage.getById(executions[2]!.executionId, {})
  expect(updatedExecution1?.closeStatus).toBe('closed')
  expect(updatedExecution2?.closeStatus).toBe('closed')
  expect(updatedExecution1?.closeExpiresAt).toBeUndefined()
  expect(updatedExecution2?.closeExpiresAt).toBeUndefined()

  const nonUpdatedExecution1 = await storage.getById(executions[1]!.executionId, {})
  const nonUpdatedExecution2 = await storage.getById(executions[3]!.executionId, {})
  expect(nonUpdatedExecution1?.closeStatus).toBe('closing')
  expect(nonUpdatedExecution2?.closeStatus).toBe('ready')
  expectDateApproximatelyEqual(nonUpdatedExecution1!.closeExpiresAt!, future)
  expect(nonUpdatedExecution2?.updatedAt.getTime()).toBeLessThan(now.getTime())
}

async function testUpdateByExecutorIdAndNeedsPromiseCancellationAndReturn(
  storage: TaskExecutionsStorage,
) {
  let now = new Date()
  const past = new Date(now.getTime() - 1000)
  const future = new Date(now.getTime() + 1000)

  const executions = [
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
    createTaskExecutionStorageValue(),
  ]

  executions[0]!.executorId = 'executor1'
  executions[0]!.needsPromiseCancellation = true
  executions[0]!.updatedAt = past
  executions[1]!.executorId = 'executor2'
  executions[1]!.needsPromiseCancellation = true
  executions[1]!.updatedAt = past
  executions[2]!.executorId = 'executor1'
  executions[2]!.needsPromiseCancellation = true
  executions[2]!.updatedAt = future
  executions[3]!.executorId = 'executor1'
  executions[3]!.needsPromiseCancellation = true
  executions[3]!.updatedAt = now
  executions[4]!.executorId = 'executor1'
  executions[4]!.needsPromiseCancellation = false
  executions[4]!.updatedAt = past

  await storage.insertMany(executions)

  now = new Date()
  const updatedExecutions = await storage.updateByExecutorIdAndNeedsPromiseCancellationAndReturn(
    'executor1',
    true,
    {
      needsPromiseCancellation: false,
      updatedAt: now,
    },
    2,
  )

  expect(updatedExecutions).toHaveLength(2)
  expect(updatedExecutions[0]!.executionId).toBe(executions[0]!.executionId)
  expect(updatedExecutions[1]!.executionId).toBe(executions[3]!.executionId)
  expect(updatedExecutions[0]!.needsPromiseCancellation).toBe(false)
  expect(updatedExecutions[1]!.needsPromiseCancellation).toBe(false)

  const updatedExecution1 = await storage.getById(executions[0]!.executionId, {})
  const updatedExecution2 = await storage.getById(executions[3]!.executionId, {})
  expect(updatedExecution1?.needsPromiseCancellation).toBe(false)
  expect(updatedExecution2?.needsPromiseCancellation).toBe(false)

  const nonUpdatedExecution1 = await storage.getById(executions[1]!.executionId, {})
  const nonUpdatedExecution2 = await storage.getById(executions[2]!.executionId, {})
  const nonUpdatedExecution3 = await storage.getById(executions[4]!.executionId, {})
  expect(nonUpdatedExecution1?.needsPromiseCancellation).toBe(true)
  expect(nonUpdatedExecution2?.needsPromiseCancellation).toBe(true)
  expect(nonUpdatedExecution3?.needsPromiseCancellation).toBe(false)
}

async function testGetByParentExecutionId(storage: TaskExecutionsStorage) {
  const parentExecution = createTaskExecutionStorageValue()
  const childrenExecutions = [
    createTaskExecutionStorageValue({
      parent: {
        taskId: parentExecution.taskId,
        executionId: parentExecution.executionId,
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
    }),
    createTaskExecutionStorageValue({
      parent: {
        taskId: parentExecution.taskId,
        executionId: parentExecution.executionId,
        indexInParentChildTaskExecutions: 1,
        isFinalizeTaskOfParentTask: false,
      },
    }),
    createTaskExecutionStorageValue({
      parent: {
        taskId: parentExecution.taskId,
        executionId: parentExecution.executionId,
        indexInParentChildTaskExecutions: -1,
        isFinalizeTaskOfParentTask: true,
      },
    }),
  ]
  const unrelatedExecution1 = createTaskExecutionStorageValue()
  const unrelatedExecution2 = createTaskExecutionStorageValue({
    parent: {
      taskId: unrelatedExecution1.taskId,
      executionId: unrelatedExecution1.executionId,
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: true,
    },
  })

  await storage.insertMany([
    parentExecution,
    ...childrenExecutions,
    unrelatedExecution1,
    unrelatedExecution2,
  ])

  const foundChildrenExecutions = await storage.getByParentExecutionId(parentExecution.executionId)
  expect(foundChildrenExecutions).toHaveLength(3)

  const childrenTaskIds = childrenExecutions.map((c) => c.taskId).sort()
  const foundChildrenTaskIds = foundChildrenExecutions.map((c) => c.taskId).sort()
  expect(foundChildrenTaskIds).toEqual(childrenTaskIds)

  const childrenExecutionIds = childrenExecutions.map((c) => c.executionId).sort()
  const foundChildrenExecutionIds = foundChildrenExecutions.map((c) => c.executionId).sort()
  expect(foundChildrenExecutionIds).toEqual(childrenExecutionIds)

  for (const child of foundChildrenExecutions) {
    expect(child.parent).toBeDefined()
    assert(child.parent)
    expect(child.parent.executionId).toBe(parentExecution.executionId)
    expect(child.parent.taskId).toBe(parentExecution.taskId)
  }

  const nonFoundChildrenExecutions = await storage.getByParentExecutionId('non-existent-id')
  expect(nonFoundChildrenExecutions).toHaveLength(0)

  const emptyChildrenExecutions = await storage.getByParentExecutionId(
    unrelatedExecution2.executionId,
  )
  expect(emptyChildrenExecutions).toHaveLength(0)
}

async function testUpdateByParentExecutionIdAndIsFinished(storage: TaskExecutionsStorage) {
  const parentExecution = createTaskExecutionStorageValue()
  const childrenExecutions = [
    createTaskExecutionStorageValue({
      parent: {
        taskId: parentExecution.taskId,
        executionId: parentExecution.executionId,
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
    }),
    createTaskExecutionStorageValue({
      parent: {
        taskId: parentExecution.taskId,
        executionId: parentExecution.executionId,
        indexInParentChildTaskExecutions: 1,
        isFinalizeTaskOfParentTask: false,
      },
    }),
    createTaskExecutionStorageValue({
      parent: {
        taskId: parentExecution.taskId,
        executionId: parentExecution.executionId,
        indexInParentChildTaskExecutions: 1,
        isFinalizeTaskOfParentTask: false,
      },
    }),
  ]

  childrenExecutions[0]!.status = 'ready'
  childrenExecutions[0]!.isFinished = false
  childrenExecutions[1]!.status = 'completed'
  childrenExecutions[1]!.isFinished = true
  childrenExecutions[2]!.status = 'running'
  childrenExecutions[2]!.isFinished = false

  await storage.insertMany([parentExecution, ...childrenExecutions])

  let now = new Date()
  await storage.updateByParentExecutionIdAndIsFinished(parentExecution.executionId, false, {
    status: 'cancelled',
    isFinished: true,
    error: {
      message: 'Parent task cancelled',
      errorType: 'cancelled',
      isRetryable: false,
      isInternal: false,
    },
    finishedAt: now,
    updatedAt: now,
  })

  let childExecution1 = await storage.getById(childrenExecutions[0]!.executionId, {})
  let childExecution2 = await storage.getById(childrenExecutions[1]!.executionId, {})
  let childExecution3 = await storage.getById(childrenExecutions[2]!.executionId, {})

  expect(childExecution1?.status).toBe('cancelled')
  expect(childExecution1?.isFinished).toBe(true)
  expect(childExecution1?.error).toBeDefined()
  assert(childExecution1?.error)
  expect(childExecution1?.error.message).toBe('Parent task cancelled')

  expect(childExecution2?.status).toBe('completed')
  expect(childExecution2?.isFinished).toBe(true)

  expect(childExecution3?.status).toBe('cancelled')
  expect(childExecution3?.isFinished).toBe(true)
  expect(childExecution3?.error).toBeDefined()
  assert(childExecution3?.error)
  expect(childExecution3?.error.message).toBe('Parent task cancelled')

  const newChildTaskExecution = createTaskExecutionStorageValue({
    parent: {
      taskId: parentExecution.taskId,
      executionId: parentExecution.executionId,
      indexInParentChildTaskExecutions: 2,
      isFinalizeTaskOfParentTask: false,
    },
  })

  newChildTaskExecution.status = 'running'
  newChildTaskExecution.isFinished = false

  await storage.insertMany([newChildTaskExecution])

  now = new Date()
  await storage.updateByParentExecutionIdAndIsFinished(parentExecution.executionId, true, {
    closeStatus: 'ready',
    updatedAt: now,
  })

  childExecution1 = await storage.getById(childrenExecutions[0]!.executionId, {})
  childExecution2 = await storage.getById(childrenExecutions[1]!.executionId, {})
  childExecution3 = await storage.getById(childrenExecutions[2]!.executionId, {})
  const childTaskExecution4 = await storage.getById(newChildTaskExecution.executionId, {})

  expect(childExecution1?.closeStatus).toBe('ready')
  expect(childExecution2?.closeStatus).toBe('ready')
  expect(childExecution3?.closeStatus).toBe('ready')
  expect(childTaskExecution4?.closeStatus).toBe('idle')
}

async function testUpdateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
  storage: TaskExecutionsStorage,
) {
  let now = new Date()
  const past = new Date(now.getTime() - 1000)
  const future = new Date(now.getTime() + 1000)

  const parent1 = createTaskExecutionStorageValue()
  parent1.status = 'waiting_for_children'
  parent1.activeChildrenCount = 3

  const parent2 = createTaskExecutionStorageValue()
  parent2.status = 'waiting_for_children'
  parent2.activeChildrenCount = 2

  const childrenExecutions = [
    createTaskExecutionStorageValue({
      parent: {
        taskId: parent1.taskId,
        executionId: parent1.executionId,
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
    }),
    createTaskExecutionStorageValue({
      parent: {
        taskId: parent1.taskId,
        executionId: parent1.executionId,
        indexInParentChildTaskExecutions: 1,
        isFinalizeTaskOfParentTask: false,
      },
    }),
    createTaskExecutionStorageValue({
      parent: {
        taskId: parent2.taskId,
        executionId: parent2.executionId,
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
    }),
    createTaskExecutionStorageValue({
      parent: {
        taskId: parent2.taskId,
        executionId: parent2.executionId,
        indexInParentChildTaskExecutions: 1,
        isFinalizeTaskOfParentTask: false,
      },
    }),
  ]

  childrenExecutions[0]!.status = 'completed'
  childrenExecutions[0]!.isFinished = true
  childrenExecutions[0]!.updatedAt = past
  childrenExecutions[1]!.status = 'waiting_for_children'
  childrenExecutions[1]!.isFinished = false
  childrenExecutions[1]!.updatedAt = past
  childrenExecutions[2]!.status = 'completed'
  childrenExecutions[2]!.isFinished = true
  childrenExecutions[2]!.updatedAt = future
  childrenExecutions[3]!.status = 'failed'
  childrenExecutions[3]!.isFinished = true
  childrenExecutions[3]!.updatedAt = now

  await storage.insertMany([parent1, parent2, ...childrenExecutions])

  now = new Date()
  let updatedCount =
    await storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
      true,
      'idle',
      {
        closeStatus: 'ready',
        updatedAt: now,
      },
      2,
    )

  expect(updatedCount).toBe(2)

  const updatedChildExecution1 = await storage.getById(childrenExecutions[0]!.executionId, {})
  const updatedChildExecution2 = await storage.getById(childrenExecutions[3]!.executionId, {})
  expect(updatedChildExecution1?.closeStatus).toBe('ready')
  expect(updatedChildExecution2?.closeStatus).toBe('ready')

  const updatedParentExecution1 = await storage.getById(parent1.executionId, {})
  const updatedParentExecution2 = await storage.getById(parent2.executionId, {})
  expect(updatedParentExecution1?.activeChildrenCount).toBe(2)
  expect(updatedParentExecution2?.activeChildrenCount).toBe(1)

  const nonUpdatedChildExecution1 = await storage.getById(childrenExecutions[1]!.executionId, {})
  const nonUpdatedChildExecution2 = await storage.getById(childrenExecutions[2]!.executionId, {})
  expect(nonUpdatedChildExecution1?.closeStatus).toBe('idle')
  expect(nonUpdatedChildExecution2?.closeStatus).toBe('idle')

  now = new Date()
  updatedCount =
    await storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
      true,
      'idle',
      {
        closeStatus: 'ready',
        updatedAt: now,
      },
      10,
    )

  expect(updatedCount).toBe(1)

  const updatedChildExecution3 = await storage.getById(childrenExecutions[2]!.executionId, {})
  expect(updatedChildExecution3?.closeStatus).toBe('ready')

  const finalParentExecution1 = await storage.getById(parent1.executionId, {})
  const finalParentExecution2 = await storage.getById(parent2.executionId, {})
  expect(finalParentExecution1?.activeChildrenCount).toBe(2)
  expect(finalParentExecution2?.activeChildrenCount).toBe(0)

  now = new Date()
  updatedCount =
    await storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
      true,
      'idle',
      { updatedAt: new Date() },
      10,
    )
  expect(updatedCount).toBe(0)
}

function createTaskExecutionStorageValue({
  now,
  root,
  parent,
  taskId,
  executionId,
  isSleepingTask,
  sleepingTaskUniqueId,
  retryOptions,
  sleepMsBeforeRun,
  timeoutMs,
  input,
}: {
  now?: Date
  root?: TaskExecutionSummary
  parent?: ParentTaskExecutionSummary
  taskId?: string
  executionId?: string
  isSleepingTask?: boolean
  sleepingTaskUniqueId?: string
  retryOptions?: TaskRetryOptions
  sleepMsBeforeRun?: number
  timeoutMs?: number
  input?: string
} = {}): TaskExecutionStorageValue {
  now = now ?? new Date()
  taskId = taskId ?? generateTaskId()
  executionId = executionId ?? generateTaskExecutionId()
  isSleepingTask = isSleepingTask ?? false
  retryOptions = retryOptions ?? { maxAttempts: 0 }
  sleepMsBeforeRun = sleepMsBeforeRun ?? 0
  timeoutMs = timeoutMs ?? 1000
  input = input ?? ''
  return {
    root,
    parent,
    taskId,
    executionId,
    isSleepingTask,
    sleepingTaskUniqueId: isSleepingTask ? (sleepingTaskUniqueId ?? '') : undefined,
    retryOptions,
    sleepMsBeforeRun,
    timeoutMs,
    input,
    status: isSleepingTask ? 'running' : 'ready',
    isFinished: false,
    retryAttempts: 0,
    startAt: new Date(now.getTime() + sleepMsBeforeRun),
    activeChildrenCount: 0,
    onChildrenFinishedProcessingStatus: 'idle',
    closeStatus: 'idle',
    needsPromiseCancellation: false,
    createdAt: now,
    updatedAt: now,
  }
}

function expectDateApproximatelyEqual(date1: Date, date2: Date) {
  expect(Math.abs(date1.getTime() - date2.getTime())).toBeLessThanOrEqual(1000)
}

const STORAGE_BENCH_EXECUTOR_COUNT = 3

/**
 * Runs a benchmark suite to measure the performance of a TaskExecutionsStorage implementation.
 *
 * @example
 * ```ts
 * import { InMemoryTaskExecutionsStorage } from 'durable-execution'
 * import { runStorageBench } from 'durable-execution-storage-test-utils'
 *
 * const storage = new InMemoryTaskExecutionsStorage()
 * await runStorageBench("in memory", storage)
 * ```
 *
 * @example
 * ```ts
 * // With cleanup for database storage
 * const storage = new CustomTaskExecutionsStorage({ url: process.env.DATABASE_URL! })
 * await runStorageBench("custom", storage, async () => {
 *   await db.delete(taskExecutions)
 * })
 * ```
 *
 * @param name - Name of the storage implementation to test
 * @param storage - A TaskExecutionsStorage implementation to test
 * @param cleanup - Optional cleanup function to run after tests complete (e.g., to remove test
 *   database)
 */
export async function runStorageBench(
  name: string,
  storage: TaskExecutionsStorage,
  cleanup?: () => void | Promise<void>,
) {
  console.log('\n========================================')
  console.log(`Running benchmark for ${name} storage\n`)

  let activeExecutors: Array<DurableExecutor> = []
  let isShuttingDown = false

  const gracefulShutdown = (signal: string) => {
    if (isShuttingDown) {
      return
    }

    isShuttingDown = true
    console.log(`\n=> Received ${signal}, shutting down gracefully...`)
    const shutdownAsync = async () => {
      try {
        if (activeExecutors.length > 0) {
          console.log(`=> Shutting down ${activeExecutors.length} executor(s)...`)
          const shutdownPromises = activeExecutors.map((executor) => executor.shutdown())
          await Promise.all(shutdownPromises)
          console.log('=> Executors shut down successfully')
        }

        if (cleanup) {
          console.log('=> Running cleanup...')
          await cleanup()
          console.log('=> Cleanup completed')
        }
      } catch (error) {
        console.error('=> Error during graceful shutdown', error)
      }

      // eslint-disable-next-line unicorn/no-process-exit
      process.exit(0)
    }

    void shutdownAsync()
  }

  process.on('SIGINT', () => {
    gracefulShutdown('SIGINT')
  })
  process.on('SIGTERM', () => {
    gracefulShutdown('SIGTERM')
  })

  try {
    const durations: Array<number> = []
    for (let i = 0; i < 4; i++) {
      if (isShuttingDown) {
        break
      }

      const executors = Array.from(
        { length: STORAGE_BENCH_EXECUTOR_COUNT },
        () =>
          new DurableExecutor(storage, {
            logLevel: 'error',
          }),
      )
      activeExecutors = executors

      let parentTask: Task<number, string> | undefined
      for (const executor of executors) {
        const childTask = executor.task({
          id: 'child',
          timeoutMs: 60_000,
          run: async (
            ctx,
            input: {
              parentIndex: number
              childIndex: number
            },
          ) => {
            await sleep(1)
            return `child_output_${input.parentIndex}_${input.childIndex}`
          },
        })
        parentTask = executor.parentTask({
          id: 'parent',
          timeoutMs: 60_000,
          runParent: async (ctx, parentIndex: number) => {
            await sleep(1)
            return {
              output: 'parent_output',
              children: Array.from({ length: 100 }, (_, index) => ({
                task: childTask,
                input: {
                  parentIndex: parentIndex,
                  childIndex: index,
                },
              })),
            }
          },
          finalize: {
            id: 'finalize',
            timeoutMs: 60_000,
            run: (ctx, { output, children }) => {
              if (output !== 'parent_output') {
                throw new Error('Invalid parent output')
              }
              if (children.some((child) => child.status !== 'completed')) {
                const first3Errors = children
                  .filter((child) => child.status !== 'completed')
                  .slice(0, 3)
                  .map((child) => child.error)
                throw new Error(
                  `${children.filter((child) => child.status !== 'completed').length}/${children.length} children failed:\n${first3Errors.map((error) => `- ${error?.message}`).join('\n')}`,
                )
              }
              return 'finalize_output'
            },
          },
        })
      }

      if (!parentTask) {
        throw new Error('Parent task is not defined')
      }

      await storage.deleteAll()
      for (const executor of executors) {
        executor.startBackgroundProcesses()
      }

      const iterationName = i === 0 ? 'warmup iteration' : `iteration ${i}`
      console.log(`=> Running ${iterationName} for ${name} storage`)
      try {
        await storage.deleteAll()
        const startTime = performance.now()
        await runDurableExecutorBench(executors[0]!, parentTask)
        const endTime = performance.now()
        console.log(
          `=> Completed ${iterationName} for ${name} storage: ${(endTime - startTime).toFixed(2)}ms`,
        )
        if (i > 0) {
          durations.push(endTime - startTime)
        }
      } finally {
        const promises = executors.map((executor) => executor.shutdown())
        await Promise.all(promises)
        activeExecutors = []
      }
    }

    const mean = durations.reduce((a, b) => a + b, 0) / durations.length
    const min = Math.min(...durations)
    const max = Math.max(...durations)
    const median = durations.sort((a, b) => a - b)[Math.floor(durations.length / 2)]
    console.log(
      `\nBenchmark results for ${name} storage:\n    mean: ${mean}ms\n     min: ${min}ms\n     max: ${max}ms\n  median: ${median}ms`,
    )
  } catch (error) {
    console.error(`Error running benchmark for ${name} storage:`, error)
    throw error
  } finally {
    console.log('\n========================================\n')

    process.removeAllListeners('SIGINT')
    process.removeAllListeners('SIGTERM')
    if (cleanup) {
      console.log('=> Running cleanup...')
      await cleanup()
      console.log('=> Cleanup completed')
    }
  }
}

async function runDurableExecutorBench(
  executor: DurableExecutor,
  parentTask: Task<number, string>,
) {
  console.log('=> Enqueuing tasks')
  const handles = await Promise.all(
    Array.from({ length: 100 }, async (_, i) => {
      return await executor.enqueueTask(parentTask, i)
    }),
  )

  console.log('=> Waiting for tasks')
  for (const handle of handles) {
    const finalExecution = await handle.waitAndGetFinishedExecution()
    if (finalExecution.status !== 'completed') {
      console.error(
        `=> Final execution status is not completed: ${JSON.stringify(finalExecution.error)}`,
      )
      throw new Error('Final execution status is not completed')
    }
    if (finalExecution.output !== 'finalize_output') {
      console.error(
        `=> Final execution output is not finalize_output: ${JSON.stringify(finalExecution)}`,
      )
      throw new Error('Final execution output is not finalize_output')
    }
  }
  console.log('=> Completed tasks')
}

/**
 * Executes a function with a temporary directory that is automatically cleaned up.
 *
 * Creates a temporary directory with a unique name prefixed with '.tmp_' and ensures it's
 * removed after the function completes, even if an error occurs.
 *
 * @example
 * ```ts
 * await withTemporaryDirectory(async (tmpDir) => {
 *   const testFile = path.join(tmpDir, 'test.db')
 *   // Use the temporary directory...
 *   await fs.writeFile(testFile, 'test data')
 * })
 * // Directory is automatically cleaned up
 * ```
 *
 *
 * @param fn - Function to execute with the temporary directory path
 * @throws Re-throws any error from the provided function
 */
export async function withTemporaryDirectory(fn: (dirPath: string) => Promise<void>) {
  const dirPath = await fs.mkdtemp('.tmp_')
  try {
    await fn(dirPath)
  } finally {
    await fs.rm(dirPath, { recursive: true })
  }
}

/**
 * Executes a function with a temporary file path that is automatically cleaned up.
 *
 * Creates a temporary directory, constructs a file path with the given filename, and ensures the
 * directory is removed after the function completes. The file itself doesn't need to be created -
 * this just provides a safe file path.
 *
 * @example
 * ```ts
 * await withTemporaryFile('test.db', async (filePath) => {
 *   const storage = new CustomTaskExecutionsStorage(filePath)
 *   await runStorageTest(storage)
 * })
 * // Temporary directory and file are automatically cleaned up
 * ```
 *
 * @param filename - Name of the file within the temporary directory
 * @param fn - Function to execute with the temporary file path
 * @throws Re-throws any error from the provided function
 */
export async function withTemporaryFile(filename: string, fn: (file: string) => Promise<void>) {
  return withTemporaryDirectory(async (dirPath) => {
    const filePath = path.join(dirPath, filename)
    await fn(filePath)
  })
}

/**
 * Cleans up any remaining temporary files and directories created by this library.
 *
 * Searches the current working directory for files and directories starting with '.tmp_' and
 * removes them. This function is safe to call multiple times and ignores any errors during
 * cleanup.
 *
 * Useful for test cleanup hooks or when temporary files weren't properly cleaned up due to
 * unexpected process termination.
 *
 * @example
 * ```ts
 * // In a test suite
 * afterAll(async () => {
 *   await cleanupTemporaryFiles()
 * })
 * ```
 *
 * @example
 * ```ts
 * // Manual cleanup
 * await cleanupTemporaryFiles()
 * ```
 */
export async function cleanupTemporaryFiles() {
  const tmpDir = process.cwd()
  try {
    const files = await fs.readdir(tmpDir)
    for (const file of files) {
      if (file.startsWith('.tmp_')) {
        const fullPath = path.join(tmpDir, file)
        try {
          await fs.rm(fullPath, { recursive: true })
        } catch {
          // ignore errors
        }
      }
    }
  } catch {
    // ignore errors
  }
}

const _ALPHABET = '0123456789ABCDEFGHJKMNPQRSTUVWXYZabcdefghjkmnpqrstwxyz'

const generateId = customAlphabet(_ALPHABET, 24)

function generateTaskId(): string {
  return `t_${generateId(24)}`
}

function generateTaskExecutionId(): string {
  return `te_${generateId(24)}`
}
