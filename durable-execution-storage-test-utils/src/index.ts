import fs from 'node:fs/promises'
import path from 'node:path'

import {
  childTask,
  DurableExecutor,
  FINISHED_TASK_EXECUTION_STATUSES,
  type ParentTaskExecutionSummary,
  type Task,
  type TaskExecutionCloseStatus,
  type TaskExecutionOnChildrenFinishedProcessingStatus,
  type TaskExecutionsStorage,
  type TaskExecutionStatus,
  type TaskExecutionStorageGetByIdFilters,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
  type TaskExecutionSummary,
  type TaskRetryOptions,
} from 'durable-execution'
import { customAlphabet } from 'nanoid'

import { isObject } from '@gpahal/std/objects'
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
 * await runStorageTest(storage, {
 *   storageCleanup: async () => {
 *     await db.delete(taskExecutions)
 *   },
 * })
 * ```
 *
 * @param storage - The TaskExecutionsStorage implementation to test
 * @param options - Optional options
 * @param options.storageCleanup - Optional cleanup function to run after tests complete (e.g., to
 *   remove test database)
 * @param options.enableStorageBatching - Whether to enable storage batching.
 * @throws Will throw if any storage operation fails validation or doesn't meet ACID requirements
 */
export async function runStorageTest(
  storage: TaskExecutionsStorage,
  {
    storageCleanup,
    enableStorageBatching = false,
    storageBatchingBackgroundProcessIntraBatchSleepMs = 10,
  }: {
    storageCleanup?: () => void | Promise<void>
    enableStorageBatching?: boolean
    storageBatchingBackgroundProcessIntraBatchSleepMs?: number
  } = {},
) {
  const executor = new DurableExecutor(storage, {
    logLevel: 'error',
    backgroundProcessIntraBatchSleepMs: 50,
    enableStorageBatching,
    storageBatchingBackgroundProcessIntraBatchSleepMs,
  })
  executor.start()

  try {
    try {
      await runDurableExecutorTest(executor, storage)
    } finally {
      await executor.shutdown()
    }

    await runStorageOperationsTest(storage)
  } finally {
    if (storageCleanup) {
      await storageCleanup()
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
  const taskB = executor.sequentialTasks('b', [taskB1, taskB2, taskB3])

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
          childTask(taskA1, { name: input.name }),
          childTask(taskA2, { name: input.name }),
          childTask(taskA3, { name: input.name }),
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
        children: [childTask(taskA, { name: input.name }), childTask(taskB, { name: input.name })],
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
        children: [childTask(failingTask)],
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
        children: [childTask(taskA1, { name: 'world' })],
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
        children: [childTask(sleepingTask, 'test_unique_id')],
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
      return { output: 'parent_output', children: [childTask(closeTestTask)] }
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
  expect(finishedExecution.waitingForChildrenStartedAt).toBeInstanceOf(Date)
  expect(finishedExecution.waitingForFinalizeStartedAt).toBeInstanceOf(Date)
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
  expect(concurrentParentTaskExecution.waitingForChildrenStartedAt).toBeInstanceOf(Date)
  expect(concurrentParentTaskExecution.waitingForFinalizeStartedAt).toBeUndefined()

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
  expect(pollingTaskExecution.waitingForChildrenStartedAt).toBeInstanceOf(Date)
  expect(pollingTaskExecution.waitingForFinalizeStartedAt).toBeInstanceOf(Date)
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
  expect(sleepingTaskExecution.waitingForChildrenStartedAt).toBeUndefined()
  expect(sleepingTaskExecution.waitingForFinalizeStartedAt).toBeUndefined()

  const parentTaskWithSleepingTaskFinishedExecution =
    await parentTaskWithSleepingTaskHandle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
  expect(parentTaskWithSleepingTaskFinishedExecution.status).toBe('completed')
  assert(parentTaskWithSleepingTaskFinishedExecution.status === 'completed')
  expect(parentTaskWithSleepingTaskFinishedExecution.output).toBe('sleeping_task_output')

  console.log('=> Waiting for close test task')
  const closeTestFinishedExecution = await closeTestTaskHandle.waitAndGetFinishedExecution({
    pollingIntervalMs: 100,
  })
  expect(closeTestExecutionCount).toBe(1)
  expect(closeTestFinishedExecution.status).toBe('completed')
  assert(closeTestFinishedExecution.status === 'completed')
  expect(closeTestFinishedExecution.output.output).toBe('parent_output')

  for (let i = 0; ; i++) {
    const executionStorageValues = await storage.getManyById([
      { executionId: closeTestFinishedExecution.executionId, filters: {} },
    ])
    const executionStorageValue = executionStorageValues[0]!
    expect(executionStorageValue).toBeDefined()
    assert(executionStorageValue)
    expect(executionStorageValue.status).toBe('completed')
    expect(executionStorageValue.onChildrenFinishedProcessingStatus).toBe('processed')
    expect(executionStorageValue.onChildrenFinishedProcessingFinishedAt).toBeDefined()
    assert(executionStorageValue.onChildrenFinishedProcessingFinishedAt)
    expect(executionStorageValue.onChildrenFinishedProcessingFinishedAt).toBeGreaterThan(0)

    if (executionStorageValue.closeStatus === 'closed') {
      expect(executionStorageValue.closedAt).toBeDefined()
      assert(executionStorageValue.closedAt)
      expect(executionStorageValue.closedAt).toBeGreaterThan(
        executionStorageValue.onChildrenFinishedProcessingFinishedAt,
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
  await testGetManyById(storage)
  await storage.deleteAll()
  await testGetManyBySleepingTaskUniqueId(storage)
  await storage.deleteAll()
  await testUpdateManyById(storage)
  await storage.deleteAll()
  await testUpdateManyByIdAndInsertChildrenIfUpdated(storage)
  await storage.deleteAll()
  await testUpdateByStatusAndStartAtLessThanAndReturn(storage)
  await storage.deleteAll()
  await testUpdateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
    storage,
  )
  await storage.deleteAll()
  await testUpdateByCloseStatusAndReturn(storage)
  await storage.deleteAll()
  await testUpdateByStatusAndIsSleepingTaskAndExpiresAtLessThan(storage)
  await storage.deleteAll()
  await testUpdateByOnChildrenFinishedProcessingExpiresAtLessThan(storage)
  await storage.deleteAll()
  await testUpdateByCloseExpiresAtLessThan(storage)
  await storage.deleteAll()
  await testUpdateByExecutorIdAndNeedsPromiseCancellationAndReturn(storage)
  await storage.deleteAll()
  await testGetManyByParentExecutionId(storage)
  await storage.deleteAll()
  await testUpdateManyByParentExecutionIdAndIsFinished(storage)
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
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
    }),
  ]
  executions[1]!.isSleepingTask = true
  executions[1]!.sleepingTaskUniqueId = 'test_unique_id'
  await storage.insertMany(executions)

  for (const execution of executions) {
    const insertedExecution = (
      await storage.getManyById([{ executionId: execution.executionId, filters: {} }])
    )[0]!
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
    expect(insertedExecution.startAt).toBeGreaterThan(0)
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
    expect(insertedExecution.createdAt).toBeGreaterThan(0)
    expect(insertedExecution.updatedAt).toBeGreaterThan(0)
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

async function testGetManyById(storage: TaskExecutionsStorage) {
  const execution1 = createTaskExecutionStorageValue()
  const execution2 = createTaskExecutionStorageValue()

  execution2.status = 'completed'
  execution2.isFinished = true
  execution2.output = 'output'
  execution2.retryAttempts = 1
  execution2.finishedAt = Date.now()
  execution2.children = [
    {
      taskId: generateTaskId(),
      executionId: generateTaskExecutionId(),
    },
  ]
  execution2.activeChildrenCount = 0
  execution2.closeStatus = 'closing'
  execution2.closeExpiresAt = Date.now()
  await storage.insertMany([execution1, execution2])

  const foundExecution1 = (
    await storage.getManyById([{ executionId: execution1.executionId, filters: {} }])
  )[0]!
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
    const foundExecution2 = (
      await storage.getManyById([{ executionId: execution2.executionId, filters: filter }])
    )[0]!
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
    const foundExecution2 = (
      await storage.getManyById([{ executionId: execution2.executionId, filters: filter }])
    )[0]
    expect(foundExecution2).toBeUndefined()
  }

  const nonFoundExecution = (
    await storage.getManyById([{ executionId: 'invalid_execution_id', filters: {} }])
  )[0]
  expect(nonFoundExecution).toBeUndefined()

  const multipleExecutions = await storage.getManyById([
    { executionId: execution1.executionId, filters: {} },
    { executionId: execution2.executionId, filters: {} },
  ])
  expect(multipleExecutions).toHaveLength(2)
  expect(multipleExecutions[0]!.taskId).toBe(execution1.taskId)
  expect(multipleExecutions[1]!.taskId).toBe(execution2.taskId)
}

async function testGetManyBySleepingTaskUniqueId(storage: TaskExecutionsStorage) {
  const execution1 = createTaskExecutionStorageValue()
  const execution2 = createTaskExecutionStorageValue()

  execution1.isSleepingTask = true
  execution1.sleepingTaskUniqueId = 'test_unique_id1'
  execution2.isSleepingTask = true
  execution2.sleepingTaskUniqueId = 'test_unique_id2'
  await storage.insertMany([execution1, execution2])

  const foundExecution1 = (
    await storage.getManyBySleepingTaskUniqueId([
      { sleepingTaskUniqueId: execution1.sleepingTaskUniqueId },
    ])
  )[0]!
  expect(foundExecution1).toBeDefined()
  assert(foundExecution1)
  expect(foundExecution1.taskId).toBe(execution1.taskId)
  expect(foundExecution1.executionId).toEqual(execution1.executionId)
  expect(foundExecution1.status).toEqual('ready')

  const nonFoundExecution = (
    await storage.getManyBySleepingTaskUniqueId([
      { sleepingTaskUniqueId: 'invalid_sleeping_task_unique_id' },
    ])
  )[0]
  expect(nonFoundExecution).toBeUndefined()

  const multipleExecutions = await storage.getManyBySleepingTaskUniqueId([
    { sleepingTaskUniqueId: execution1.sleepingTaskUniqueId },
    { sleepingTaskUniqueId: execution2.sleepingTaskUniqueId },
  ])
  expect(multipleExecutions).toHaveLength(2)
  expect(multipleExecutions[0]!.taskId).toBe(execution1.taskId)
  expect(multipleExecutions[1]!.taskId).toBe(execution2.taskId)
}

async function testUpdateManyById(storage: TaskExecutionsStorage) {
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

    let now = Date.now()
    const child = {
      taskId: generateTaskId(),
      executionId: generateTaskExecutionId(),
    }
    await storage.updateManyById([
      {
        executionId: execution.executionId,
        filters: filter,
        update: {
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
        },
      },
    ])

    const updatedExecution1 = (
      await storage.getManyById([{ executionId: execution.executionId, filters: {} }])
    )[0]!
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

    now = Date.now()
    await storage.updateManyById([
      {
        executionId: execution.executionId,
        filters: {},
        update: {
          needsPromiseCancellation: false,
          updatedAt: now,
          unset: {
            executorId: true,
            runOutput: true,
            error: true,
            expiresAt: true,
            onChildrenFinishedProcessingExpiresAt: true,
            closeExpiresAt: true,
          },
        },
      },
    ])

    const updatedExecution2 = (
      await storage.getManyById([{ executionId: execution.executionId, filters: {} }])
    )[0]!
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

    const execution1 = createTaskExecutionStorageValue()
    const execution2 = createTaskExecutionStorageValue()
    await storage.insertMany([execution1, execution2])

    now = Date.now()
    await storage.updateManyById([
      {
        executionId: execution1.executionId,
        filters: {},
        update: {
          status: 'cancelled',
          updatedAt: now,
        },
      },
      {
        executionId: execution2.executionId,
        filters: {},
        update: {
          status: 'cancelled',
          updatedAt: now,
        },
      },
    ])

    const updatedExecution3 = (
      await storage.getManyById([{ executionId: execution1.executionId, filters: {} }])
    )[0]!
    const updatedExecution4 = (
      await storage.getManyById([{ executionId: execution2.executionId, filters: {} }])
    )[0]!
    expect(updatedExecution3).toBeDefined()
    assert(updatedExecution3)
    expect(updatedExecution4).toBeDefined()
    assert(updatedExecution4)
    expect(updatedExecution3.status).toBe('cancelled')
    expect(updatedExecution4.status).toBe('cancelled')
  }
}

async function testUpdateManyByIdAndInsertChildrenIfUpdated(storage: TaskExecutionsStorage) {
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
          indexInParentChildren: 0,
          isOnlyChildOfParent: false,
          isFinalizeOfParent: false,
        },
      }),
      createTaskExecutionStorageValue({
        parent: {
          taskId: testExecution.taskId,
          executionId: testExecution.executionId,
          indexInParentChildren: 1,
          isOnlyChildOfParent: false,
          isFinalizeOfParent: false,
        },
      }),
    ]

    await storage.insertMany([testExecution])

    const now = Date.now()
    await storage.updateManyByIdAndInsertChildrenIfUpdated([
      {
        executionId: testExecution.executionId,
        filters: filter,
        update: {
          status: 'waiting_for_children',
          children: childrenExecutions.map((e) => ({
            taskId: e.taskId,
            executionId: e.executionId,
          })),
          activeChildrenCount: 2,
          updatedAt: now,
        },
        childrenTaskExecutionsToInsertIfAnyUpdated: childrenExecutions,
      },
    ])

    const updatedExecution = (
      await storage.getManyById([{ executionId: testExecution.executionId, filters: {} }])
    )[0]!
    expect(updatedExecution).toBeDefined()
    assert(updatedExecution)
    expect(updatedExecution.status).toBe('waiting_for_children')
    expect(updatedExecution.children).toBeDefined()
    assert(updatedExecution.children)
    expect(updatedExecution.children).toHaveLength(2)
    expect(updatedExecution.activeChildrenCount).toBe(2)

    for (const child of childrenExecutions) {
      const insertedChild = (
        await storage.getManyById([{ executionId: child.executionId, filters: {} }])
      )[0]!
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
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
    })

    await storage.updateManyByIdAndInsertChildrenIfUpdated([
      {
        executionId: execution.executionId,
        filters: filter,
        update: {
          status: 'running',
          updatedAt: Date.now(),
        },
        childrenTaskExecutionsToInsertIfAnyUpdated: [childExecution],
      },
    ])

    const unchangedExecution = (
      await storage.getManyById([{ executionId: execution.executionId, filters: {} }])
    )[0]!
    expect(unchangedExecution).toBeDefined()
    assert(unchangedExecution)
    expect(unchangedExecution.status).toBe('ready')

    const nonInsertedChild = (
      await storage.getManyById([{ executionId: childExecution.executionId, filters: {} }])
    )[0]!
    expect(nonInsertedChild).toBeUndefined()
  }

  const execution1 = createTaskExecutionStorageValue()
  const execution2 = createTaskExecutionStorageValue()
  await storage.insertMany([execution1, execution2])

  const childExecution1 = createTaskExecutionStorageValue({
    parent: {
      taskId: execution1.taskId,
      executionId: execution1.executionId,
      indexInParentChildren: 0,
      isOnlyChildOfParent: false,
      isFinalizeOfParent: false,
    },
  })

  const childExecution2 = createTaskExecutionStorageValue({
    parent: {
      taskId: execution2.taskId,
      executionId: execution2.executionId,
      indexInParentChildren: 0,
      isOnlyChildOfParent: false,
      isFinalizeOfParent: false,
    },
  })

  await storage.updateManyByIdAndInsertChildrenIfUpdated([
    {
      executionId: execution1.executionId,
      filters: {},
      update: {
        status: 'running',
        updatedAt: Date.now(),
      },
      childrenTaskExecutionsToInsertIfAnyUpdated: [childExecution1],
    },
  ])

  await storage.updateManyByIdAndInsertChildrenIfUpdated([
    {
      executionId: execution2.executionId,
      filters: {},
      update: {
        status: 'running',
        updatedAt: Date.now(),
      },
      childrenTaskExecutionsToInsertIfAnyUpdated: [childExecution2],
    },
  ])

  const updatedExecution1 = (
    await storage.getManyById([{ executionId: execution1.executionId, filters: {} }])
  )[0]!
  const updatedExecution2 = (
    await storage.getManyById([{ executionId: execution2.executionId, filters: {} }])
  )[0]!
  expect(updatedExecution1).toBeDefined()
  assert(updatedExecution1)
  expect(updatedExecution2).toBeDefined()
  assert(updatedExecution2)
  expect(updatedExecution1.status).toBe('running')
  expect(updatedExecution2.status).toBe('running')

  const insertedChildExecution1 = (
    await storage.getManyById([{ executionId: childExecution1.executionId, filters: {} }])
  )[0]!
  expect(insertedChildExecution1).toBeDefined()
  assert(insertedChildExecution1)
  expect(insertedChildExecution1.taskId).toBe(childExecution1.taskId)
  expect(insertedChildExecution1.parent).toBeDefined()
  assert(insertedChildExecution1.parent)
  expect(insertedChildExecution1.parent.executionId).toBe(execution1.executionId)

  const insertedChildExecution2 = (
    await storage.getManyById([{ executionId: childExecution2.executionId, filters: {} }])
  )[0]!
  expect(insertedChildExecution2).toBeDefined()
  assert(insertedChildExecution2)
  expect(insertedChildExecution2.taskId).toBe(childExecution2.taskId)
  expect(insertedChildExecution2.parent).toBeDefined()
  assert(insertedChildExecution2.parent)
  expect(insertedChildExecution2.parent.executionId).toBe(execution2.executionId)
}

async function testUpdateByStatusAndStartAtLessThanAndReturn(storage: TaskExecutionsStorage) {
  let now = Date.now()
  const past = now - 5000
  const future = now + 5000

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

  now = Date.now() + 1000
  const updatedExecutions = await storage.updateByStatusAndStartAtLessThanAndReturn({
    status: 'ready',
    startAtLessThan: future + 1000,
    update: {
      status: 'running',
      executorId: 'test-executor',
      startedAt: now,
      updatedAt: now,
    },
    updateExpiresAtWithStartedAt: now + 10_000,
    limit: 2,
  })

  expect(updatedExecutions).toHaveLength(2)
  expect(updatedExecutions[0]!.executionId).toBe(executions[0]!.executionId)
  expect(updatedExecutions[1]!.executionId).toBe(executions[2]!.executionId)
  expect(updatedExecutions[0]!.status).toBe('running')
  expect(updatedExecutions[1]!.status).toBe('running')
  expect(updatedExecutions[0]!.executorId).toBe('test-executor')
  expect(updatedExecutions[1]!.executorId).toBe('test-executor')

  const expiresAt = now + updatedExecutions[0]!.timeoutMs + 10_000
  expectDateApproximatelyEqual(updatedExecutions[0]!.expiresAt!, expiresAt)
  expectDateApproximatelyEqual(updatedExecutions[1]!.expiresAt!, expiresAt)

  const updatedExecution1 = (
    await storage.getManyById([{ executionId: executions[0]!.executionId, filters: {} }])
  )[0]!
  const updatedExecution2 = (
    await storage.getManyById([{ executionId: executions[2]!.executionId, filters: {} }])
  )[0]!
  expect(updatedExecution1?.status).toBe('running')
  expect(updatedExecution2?.status).toBe('running')

  const nonUpdatedExecution1 = (
    await storage.getManyById([{ executionId: executions[1]!.executionId, filters: {} }])
  )[0]!
  const nonUpdatedExecution2 = (
    await storage.getManyById([{ executionId: executions[3]!.executionId, filters: {} }])
  )[0]!
  const nonUpdatedExecution3 = (
    await storage.getManyById([{ executionId: executions[4]!.executionId, filters: {} }])
  )[0]!
  expect(nonUpdatedExecution1?.status).toBe('ready')
  expect(nonUpdatedExecution2?.status).toBe('running')
  expect(nonUpdatedExecution3?.status).toBe('completed')

  const noMatchedExecutions = await storage.updateByStatusAndStartAtLessThanAndReturn({
    status: 'failed',
    startAtLessThan: future + 1000,
    update: { updatedAt: now },
    updateExpiresAtWithStartedAt: now + 10_000,
    limit: 10,
  })
  expect(noMatchedExecutions).toHaveLength(0)
}

async function testUpdateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
  storage: TaskExecutionsStorage,
) {
  let now = Date.now()
  const past = now - 1000
  const future = now + 1000

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

  await storage.insertMany([executions[0]!])
  await sleep(1)
  await storage.insertMany([executions[2]!])
  await sleep(1)
  await storage.insertMany([executions[1]!, executions[3]!, executions[4]!])

  await sleep(500)
  now = Date.now()
  const updatedExecutions =
    await storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
      {
        status: 'waiting_for_children',
        onChildrenFinishedProcessingStatus: 'idle',
        update: {
          onChildrenFinishedProcessingStatus: 'processing',
          onChildrenFinishedProcessingExpiresAt: now + 10_000,
          updatedAt: now,
        },
        limit: 2,
      },
    )

  expect(updatedExecutions).toHaveLength(2)
  expect(updatedExecutions[0]!.executionId).toBe(executions[0]!.executionId)
  expect(updatedExecutions[1]!.executionId).toBe(executions[2]!.executionId)
  expect(updatedExecutions[0]!.onChildrenFinishedProcessingStatus).toBe('processing')
  expect(updatedExecutions[1]!.onChildrenFinishedProcessingStatus).toBe('processing')

  const updatedExecution1 = (
    await storage.getManyById([{ executionId: executions[0]!.executionId, filters: {} }])
  )[0]!
  const updatedExecution2 = (
    await storage.getManyById([{ executionId: executions[2]!.executionId, filters: {} }])
  )[0]!
  expect(updatedExecution1?.onChildrenFinishedProcessingStatus).toBe('processing')
  expect(updatedExecution2?.onChildrenFinishedProcessingStatus).toBe('processing')
  expect(updatedExecution1?.onChildrenFinishedProcessingExpiresAt).toBeGreaterThan(0)
  expect(updatedExecution2?.onChildrenFinishedProcessingExpiresAt).toBeGreaterThan(0)

  const nonUpdatedExecution1 = (
    await storage.getManyById([{ executionId: executions[1]!.executionId, filters: {} }])
  )[0]!
  const nonUpdatedExecution2 = (
    await storage.getManyById([{ executionId: executions[3]!.executionId, filters: {} }])
  )[0]!
  const nonUpdatedExecution3 = (
    await storage.getManyById([{ executionId: executions[4]!.executionId, filters: {} }])
  )[0]!
  expect(nonUpdatedExecution1?.onChildrenFinishedProcessingStatus).toBe('idle')
  expect(nonUpdatedExecution2?.onChildrenFinishedProcessingStatus).toBe('idle')
  expect(nonUpdatedExecution3?.onChildrenFinishedProcessingStatus).toBe('idle')
}

async function testUpdateByCloseStatusAndReturn(storage: TaskExecutionsStorage) {
  let now = Date.now()
  const past = now - 1000
  const future = now + 1000

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

  await storage.insertMany([executions[0]!, executions[3]!, executions[4]!])
  await sleep(1)
  await storage.insertMany([executions[2]!])
  await sleep(1)
  await storage.insertMany([executions[1]!])

  await sleep(500)
  now = Date.now()
  const updatedExecutions = await storage.updateByCloseStatusAndReturn({
    closeStatus: 'ready',
    update: {
      closeStatus: 'closing',
      closeExpiresAt: now + 10_000,
      updatedAt: now,
    },
    limit: 2,
  })

  expect(updatedExecutions).toHaveLength(2)
  expect(updatedExecutions[0]!.executionId).toBe(executions[0]!.executionId)
  expect(updatedExecutions[1]!.executionId).toBe(executions[2]!.executionId)
  expect(updatedExecutions[0]!.closeStatus).toBe('closing')
  expect(updatedExecutions[1]!.closeStatus).toBe('closing')

  const updatedExecution1 = (
    await storage.getManyById([{ executionId: executions[0]!.executionId, filters: {} }])
  )[0]!
  const updatedExecution2 = (
    await storage.getManyById([{ executionId: executions[2]!.executionId, filters: {} }])
  )[0]!
  expect(updatedExecution1?.closeStatus).toBe('closing')
  expect(updatedExecution2?.closeStatus).toBe('closing')
  expect(updatedExecution1?.closeExpiresAt).toBeGreaterThan(0)
  expect(updatedExecution2?.closeExpiresAt).toBeGreaterThan(0)

  const nonUpdatedExecution1 = (
    await storage.getManyById([{ executionId: executions[1]!.executionId, filters: {} }])
  )[0]!
  const nonUpdatedExecution2 = (
    await storage.getManyById([{ executionId: executions[3]!.executionId, filters: {} }])
  )[0]!
  const nonUpdatedExecution3 = (
    await storage.getManyById([{ executionId: executions[4]!.executionId, filters: {} }])
  )[0]!
  expect(nonUpdatedExecution1?.closeStatus).toBe('ready')
  expect(nonUpdatedExecution2?.closeStatus).toBe('closing')
  expect(nonUpdatedExecution3?.closeStatus).toBe('closed')
}

async function testUpdateByStatusAndIsSleepingTaskAndExpiresAtLessThan(
  storage: TaskExecutionsStorage,
) {
  let now = Date.now()
  const past = now - 5000
  const future = now + 5000

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
  executions[4]!.status = 'running'
  executions[4]!.isSleepingTask = true
  executions[4]!.expiresAt = past

  await storage.insertMany([executions[0]!, executions[4]!])
  await sleep(1)
  await storage.insertMany([executions[2]!, executions[3]!])
  await sleep(1)
  await storage.insertMany([executions[1]!])

  await sleep(500)
  now = Date.now() + 1000
  let updatedCount = await storage.updateByStatusAndIsSleepingTaskAndExpiresAtLessThan({
    status: 'running',
    isSleepingTask: false,
    expiresAtLessThan: now,
    update: {
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
    limit: 2,
  })

  expect(updatedCount).toBe(2)
  const updatedExecution1 = (
    await storage.getManyById([{ executionId: executions[0]!.executionId, filters: {} }])
  )[0]!
  const updatedExecution2 = (
    await storage.getManyById([{ executionId: executions[2]!.executionId, filters: {} }])
  )[0]!
  expect(updatedExecution1.executionId).toBe(executions[0]!.executionId)
  expect(updatedExecution2.executionId).toBe(executions[2]!.executionId)
  expect(updatedExecution1.status).toBe('ready')
  expect(updatedExecution2.status).toBe('ready')
  expect(updatedExecution1.error).toBeDefined()
  assert(updatedExecution1.error)
  expect(updatedExecution1.error.message).toBe('Task expired')
  expect(updatedExecution1.startAt).toBeGreaterThan(0)
  expect(updatedExecution2.error).toBeDefined()
  assert(updatedExecution2.error)
  expect(updatedExecution2.error.message).toBe('Task expired')
  expect(updatedExecution2.startAt).toBeGreaterThan(0)

  expect(updatedExecution1?.status).toBe('ready')
  expect(updatedExecution2?.status).toBe('ready')
  expect(updatedExecution1?.startAt).toBeGreaterThan(0)
  expect(updatedExecution2?.startAt).toBeGreaterThan(0)

  const nonUpdatedExecution1 = (
    await storage.getManyById([{ executionId: executions[1]!.executionId, filters: {} }])
  )[0]!
  const nonUpdatedExecution2 = (
    await storage.getManyById([{ executionId: executions[3]!.executionId, filters: {} }])
  )[0]!
  const nonUpdatedExecution3 = (
    await storage.getManyById([{ executionId: executions[4]!.executionId, filters: {} }])
  )[0]!
  expect(nonUpdatedExecution1?.status).toBe('running')
  expect(nonUpdatedExecution2?.status).toBe('completed')
  expect(nonUpdatedExecution3?.status).toBe('running')

  now = Date.now() + 1000
  updatedCount = await storage.updateByStatusAndIsSleepingTaskAndExpiresAtLessThan({
    status: 'running',
    isSleepingTask: true,
    expiresAtLessThan: now,
    update: {
      status: 'timed_out',
      error: {
        message: 'Task timed out',
        errorType: 'timed_out',
        isRetryable: true,
        isInternal: false,
      },
      updatedAt: now,
    },
    limit: 2,
  })

  expect(updatedCount).toBe(1)
  const updatedExecution3 = (
    await storage.getManyById([{ executionId: executions[4]!.executionId, filters: {} }])
  )[0]!
  expect(updatedExecution3.executionId).toBe(executions[4]!.executionId)
  expect(updatedExecution3.status).toBe('timed_out')
  expect(updatedExecution3.error).toBeDefined()
  assert(updatedExecution3.error)
  expect(updatedExecution3.error.message).toBe('Task timed out')
  expect(updatedExecution3.error.errorType).toBe('timed_out')
  expect(updatedExecution3.error.isRetryable).toBe(true)
  expect(updatedExecution3.error.isInternal).toBe(false)
}

async function testUpdateByOnChildrenFinishedProcessingExpiresAtLessThan(
  storage: TaskExecutionsStorage,
) {
  let now = Date.now() - 1000
  const past = now - 5000
  const future = now + 5000

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
  executions[3]!.updatedAt = past - 10_000

  await storage.insertMany([executions[3]!])
  await sleep(1)
  await storage.insertMany([executions[0]!])
  await sleep(1)
  await storage.insertMany([executions[2]!])
  await sleep(1)
  await storage.insertMany([executions[1]!])

  await sleep(500)
  now = Date.now() + 1000
  const updatedCount = await storage.updateByOnChildrenFinishedProcessingExpiresAtLessThan({
    onChildrenFinishedProcessingExpiresAtLessThan: now,
    update: {
      onChildrenFinishedProcessingStatus: 'processed',
      unset: {
        onChildrenFinishedProcessingExpiresAt: true,
      },
      updatedAt: now,
    },
    limit: 2,
  })

  expect(updatedCount).toBe(2)
  const updatedExecution1 = (
    await storage.getManyById([{ executionId: executions[0]!.executionId, filters: {} }])
  )[0]!
  const updatedExecution2 = (
    await storage.getManyById([{ executionId: executions[2]!.executionId, filters: {} }])
  )[0]!
  expect(updatedExecution1.executionId).toBe(executions[0]!.executionId)
  expect(updatedExecution2.executionId).toBe(executions[2]!.executionId)
  expect(updatedExecution1.onChildrenFinishedProcessingStatus).toBe('processed')
  expect(updatedExecution2.onChildrenFinishedProcessingStatus).toBe('processed')
  expect(updatedExecution1.onChildrenFinishedProcessingExpiresAt).toBeUndefined()
  expect(updatedExecution2.onChildrenFinishedProcessingExpiresAt).toBeUndefined()

  const nonUpdatedExecution1 = (
    await storage.getManyById([{ executionId: executions[1]!.executionId, filters: {} }])
  )[0]!
  const nonUpdatedExecution2 = (
    await storage.getManyById([{ executionId: executions[3]!.executionId, filters: {} }])
  )[0]!
  expect(nonUpdatedExecution1.onChildrenFinishedProcessingStatus).toBe('processing')
  expect(nonUpdatedExecution2.onChildrenFinishedProcessingStatus).toBe('processed')
  expectDateApproximatelyEqual(nonUpdatedExecution1.onChildrenFinishedProcessingExpiresAt!, future)
  expect(nonUpdatedExecution2.updatedAt).toBeLessThan(now)
}

async function testUpdateByCloseExpiresAtLessThan(storage: TaskExecutionsStorage) {
  let now = Date.now()
  const past = now - 5000
  const future = now + 5000

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
  executions[3]!.updatedAt = past - 10_000

  await storage.insertMany([executions[3]!])
  await sleep(1)
  await storage.insertMany([executions[0]!])
  await sleep(1)
  await storage.insertMany([executions[2]!])
  await sleep(1)
  await storage.insertMany([executions[1]!])

  await sleep(500)
  now = Date.now() + 1000
  const updatedCount = await storage.updateByCloseExpiresAtLessThan({
    closeExpiresAtLessThan: now,
    update: {
      closeStatus: 'closed',
      unset: {
        closeExpiresAt: true,
      },
      updatedAt: now,
    },
    limit: 2,
  })

  expect(updatedCount).toBe(2)
  const updatedExecution1 = (
    await storage.getManyById([{ executionId: executions[0]!.executionId, filters: {} }])
  )[0]!
  const updatedExecution2 = (
    await storage.getManyById([{ executionId: executions[2]!.executionId, filters: {} }])
  )[0]!
  expect(updatedExecution1.closeStatus).toBe('closed')
  expect(updatedExecution2.closeStatus).toBe('closed')
  expect(updatedExecution1.closeExpiresAt).toBeUndefined()
  expect(updatedExecution2.closeExpiresAt).toBeUndefined()

  const nonUpdatedExecution1 = (
    await storage.getManyById([{ executionId: executions[1]!.executionId, filters: {} }])
  )[0]!
  const nonUpdatedExecution2 = (
    await storage.getManyById([{ executionId: executions[3]!.executionId, filters: {} }])
  )[0]!
  expect(nonUpdatedExecution1?.closeStatus).toBe('closing')
  expect(nonUpdatedExecution2?.closeStatus).toBe('ready')
  expectDateApproximatelyEqual(nonUpdatedExecution1.closeExpiresAt!, future)
  expect(nonUpdatedExecution2?.updatedAt).toBeLessThan(now)
}

async function testUpdateByExecutorIdAndNeedsPromiseCancellationAndReturn(
  storage: TaskExecutionsStorage,
) {
  let now = Date.now()
  const past = now - 1000
  const future = now + 1000

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

  await storage.insertMany([executions[0]!, executions[1]!, executions[4]!])
  await sleep(1)
  await storage.insertMany([executions[3]!])
  await sleep(1)
  await storage.insertMany([executions[2]!])

  await sleep(500)
  now = Date.now()
  const updatedExecutions = await storage.updateByExecutorIdAndNeedsPromiseCancellationAndReturn({
    executorId: 'executor1',
    needsPromiseCancellation: true,
    update: {
      needsPromiseCancellation: false,
      updatedAt: now,
    },
    limit: 2,
  })

  expect(updatedExecutions).toHaveLength(2)
  expect(updatedExecutions[0]!.executionId).toBe(executions[0]!.executionId)
  expect(updatedExecutions[1]!.executionId).toBe(executions[3]!.executionId)
  expect(updatedExecutions[0]!.needsPromiseCancellation).toBe(false)
  expect(updatedExecutions[1]!.needsPromiseCancellation).toBe(false)

  const updatedExecution1 = (
    await storage.getManyById([{ executionId: executions[0]!.executionId, filters: {} }])
  )[0]!
  const updatedExecution2 = (
    await storage.getManyById([{ executionId: executions[3]!.executionId, filters: {} }])
  )[0]!
  expect(updatedExecution1?.needsPromiseCancellation).toBe(false)
  expect(updatedExecution2?.needsPromiseCancellation).toBe(false)

  const nonUpdatedExecution1 = (
    await storage.getManyById([{ executionId: executions[1]!.executionId, filters: {} }])
  )[0]!
  const nonUpdatedExecution2 = (
    await storage.getManyById([{ executionId: executions[2]!.executionId, filters: {} }])
  )[0]!
  const nonUpdatedExecution3 = (
    await storage.getManyById([{ executionId: executions[4]!.executionId, filters: {} }])
  )[0]!
  expect(nonUpdatedExecution1?.needsPromiseCancellation).toBe(true)
  expect(nonUpdatedExecution2?.needsPromiseCancellation).toBe(true)
  expect(nonUpdatedExecution3?.needsPromiseCancellation).toBe(false)
}

async function testGetManyByParentExecutionId(storage: TaskExecutionsStorage) {
  const parentExecution1 = createTaskExecutionStorageValue()
  const parentExecution2 = createTaskExecutionStorageValue()
  const childrenExecutions1 = [
    createTaskExecutionStorageValue({
      parent: {
        taskId: parentExecution1.taskId,
        executionId: parentExecution1.executionId,
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
    }),
    createTaskExecutionStorageValue({
      parent: {
        taskId: parentExecution1.taskId,
        executionId: parentExecution1.executionId,
        indexInParentChildren: 1,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
    }),
    createTaskExecutionStorageValue({
      parent: {
        taskId: parentExecution1.taskId,
        executionId: parentExecution1.executionId,
        indexInParentChildren: -1,
        isOnlyChildOfParent: true,
        isFinalizeOfParent: true,
      },
    }),
  ]
  const childrenExecutions2 = [
    createTaskExecutionStorageValue({
      parent: {
        taskId: parentExecution2.taskId,
        executionId: parentExecution2.executionId,
        indexInParentChildren: 0,
        isOnlyChildOfParent: true,
        isFinalizeOfParent: false,
      },
    }),
  ]
  const unrelatedExecution1 = createTaskExecutionStorageValue()
  const unrelatedExecution2 = createTaskExecutionStorageValue({
    parent: {
      taskId: unrelatedExecution1.taskId,
      executionId: unrelatedExecution1.executionId,
      indexInParentChildren: 0,
      isOnlyChildOfParent: true,
      isFinalizeOfParent: true,
    },
  })

  await storage.insertMany([
    parentExecution1,
    parentExecution2,
    ...childrenExecutions1,
    ...childrenExecutions2,
    unrelatedExecution1,
    unrelatedExecution2,
  ])

  const foundChildrenExecutions = (
    await storage.getManyByParentExecutionId([{ parentExecutionId: parentExecution1.executionId }])
  )[0]!
  expect(foundChildrenExecutions).toHaveLength(3)

  const childrenTaskIds = childrenExecutions1.map((c) => c.taskId).sort()
  const foundChildrenTaskIds = foundChildrenExecutions.map((c) => c.taskId).sort()
  expect(foundChildrenTaskIds).toEqual(childrenTaskIds)

  const childrenExecutionIds = childrenExecutions1.map((c) => c.executionId).sort()
  const foundChildrenExecutionIds = foundChildrenExecutions.map((c) => c.executionId).sort()
  expect(foundChildrenExecutionIds).toEqual(childrenExecutionIds)

  for (const child of foundChildrenExecutions) {
    expect(child.parent).toBeDefined()
    assert(child.parent)
    expect(child.parent.executionId).toBe(parentExecution1.executionId)
    expect(child.parent.taskId).toBe(parentExecution1.taskId)
  }

  const nonFoundChildrenExecutions = (
    await storage.getManyByParentExecutionId([{ parentExecutionId: 'non-existent-id' }])
  )[0]!
  expect(nonFoundChildrenExecutions).toHaveLength(0)

  const emptyChildrenExecutions = (
    await storage.getManyByParentExecutionId([
      { parentExecutionId: unrelatedExecution2.executionId },
    ])
  )[0]!
  expect(emptyChildrenExecutions).toHaveLength(0)

  const foundChildrenExecutionsArr = await storage.getManyByParentExecutionId([
    { parentExecutionId: parentExecution1.executionId },
    { parentExecutionId: parentExecution2.executionId },
  ])
  expect(foundChildrenExecutionsArr.length).toEqual(2)
  expect(foundChildrenExecutionsArr[0]?.length).toEqual(childrenExecutions1.length)
  expect(foundChildrenExecutionsArr[1]?.length).toEqual(childrenExecutions2.length)
}

async function testUpdateManyByParentExecutionIdAndIsFinished(storage: TaskExecutionsStorage) {
  const parentExecution = createTaskExecutionStorageValue()
  const childrenExecutions = [
    createTaskExecutionStorageValue({
      parent: {
        taskId: parentExecution.taskId,
        executionId: parentExecution.executionId,
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
    }),
    createTaskExecutionStorageValue({
      parent: {
        taskId: parentExecution.taskId,
        executionId: parentExecution.executionId,
        indexInParentChildren: 1,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
    }),
    createTaskExecutionStorageValue({
      parent: {
        taskId: parentExecution.taskId,
        executionId: parentExecution.executionId,
        indexInParentChildren: 1,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
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

  let now = Date.now()
  await storage.updateManyByParentExecutionIdAndIsFinished([
    {
      parentExecutionId: parentExecution.executionId,
      isFinished: false,
      update: {
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
      },
    },
  ])

  let childExecution1 = (
    await storage.getManyById([{ executionId: childrenExecutions[0]!.executionId, filters: {} }])
  )[0]!
  let childExecution2 = (
    await storage.getManyById([{ executionId: childrenExecutions[1]!.executionId, filters: {} }])
  )[0]!
  let childExecution3 = (
    await storage.getManyById([{ executionId: childrenExecutions[2]!.executionId, filters: {} }])
  )[0]!

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
      indexInParentChildren: 2,
      isOnlyChildOfParent: false,
      isFinalizeOfParent: false,
    },
  })

  newChildTaskExecution.status = 'running'
  newChildTaskExecution.isFinished = false

  await storage.insertMany([newChildTaskExecution])

  now = Date.now()
  await storage.updateManyByParentExecutionIdAndIsFinished([
    {
      parentExecutionId: parentExecution.executionId,
      isFinished: true,
      update: {
        closeStatus: 'ready',
        updatedAt: now,
      },
    },
  ])

  childExecution1 = (
    await storage.getManyById([{ executionId: childrenExecutions[0]!.executionId, filters: {} }])
  )[0]!
  childExecution2 = (
    await storage.getManyById([{ executionId: childrenExecutions[1]!.executionId, filters: {} }])
  )[0]!
  childExecution3 = (
    await storage.getManyById([{ executionId: childrenExecutions[2]!.executionId, filters: {} }])
  )[0]!
  const childTaskExecution4 = (
    await storage.getManyById([{ executionId: newChildTaskExecution.executionId, filters: {} }])
  )[0]!

  expect(childExecution1.closeStatus).toBe('ready')
  expect(childExecution2.closeStatus).toBe('ready')
  expect(childExecution3.closeStatus).toBe('ready')
  expect(childTaskExecution4.closeStatus).toBe('idle')
}

async function testUpdateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
  storage: TaskExecutionsStorage,
) {
  let now = Date.now()
  const past = now - 1000
  const future = now + 1000

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
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
    }),
    createTaskExecutionStorageValue({
      parent: {
        taskId: parent1.taskId,
        executionId: parent1.executionId,
        indexInParentChildren: 1,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
    }),
    createTaskExecutionStorageValue({
      parent: {
        taskId: parent2.taskId,
        executionId: parent2.executionId,
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
    }),
    createTaskExecutionStorageValue({
      parent: {
        taskId: parent2.taskId,
        executionId: parent2.executionId,
        indexInParentChildren: 1,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
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

  await storage.insertMany([parent1, parent2])
  await storage.updateManyByIdAndInsertChildrenIfUpdated([
    {
      executionId: parent1.executionId,
      filters: {},
      update: {
        updatedAt: now,
      },
      childrenTaskExecutionsToInsertIfAnyUpdated: [childrenExecutions[0]!, childrenExecutions[1]!],
    },
  ])
  await sleep(1)
  await storage.updateManyByIdAndInsertChildrenIfUpdated([
    {
      executionId: parent2.executionId,
      filters: {},
      update: {
        updatedAt: now,
      },
      childrenTaskExecutionsToInsertIfAnyUpdated: [childrenExecutions[3]!],
    },
  ])
  await sleep(1)
  await storage.updateManyByIdAndInsertChildrenIfUpdated([
    {
      executionId: parent2.executionId,
      filters: {},
      update: {
        updatedAt: now,
      },
      childrenTaskExecutionsToInsertIfAnyUpdated: [childrenExecutions[2]!],
    },
  ])

  await sleep(500)
  now = Date.now()
  let updatedCount =
    await storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus({
      isFinished: true,
      closeStatus: 'idle',
      update: {
        closeStatus: 'ready',
        updatedAt: now,
      },
      limit: 2,
    })

  expect(updatedCount).toBe(2)

  const updatedChildExecution1 = (
    await storage.getManyById([{ executionId: childrenExecutions[0]!.executionId, filters: {} }])
  )[0]!
  const updatedChildExecution2 = (
    await storage.getManyById([{ executionId: childrenExecutions[3]!.executionId, filters: {} }])
  )[0]!
  expect(updatedChildExecution1.closeStatus).toBe('ready')
  expect(updatedChildExecution2.closeStatus).toBe('ready')

  const updatedParentExecution1 = (
    await storage.getManyById([{ executionId: parent1.executionId, filters: {} }])
  )[0]!
  const updatedParentExecution2 = (
    await storage.getManyById([{ executionId: parent2.executionId, filters: {} }])
  )[0]!
  expect(updatedParentExecution1.activeChildrenCount).toBe(2)
  expect(updatedParentExecution2.activeChildrenCount).toBe(1)

  const nonUpdatedChildExecution1 = (
    await storage.getManyById([{ executionId: childrenExecutions[1]!.executionId, filters: {} }])
  )[0]!
  const nonUpdatedChildExecution2 = (
    await storage.getManyById([{ executionId: childrenExecutions[2]!.executionId, filters: {} }])
  )[0]!
  expect(nonUpdatedChildExecution1.closeStatus).toBe('idle')
  expect(nonUpdatedChildExecution2.closeStatus).toBe('idle')

  now = Date.now()
  updatedCount =
    await storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus({
      isFinished: true,
      closeStatus: 'idle',
      update: {
        closeStatus: 'ready',
        updatedAt: now,
      },
      limit: 10,
    })

  expect(updatedCount).toBe(1)

  const updatedChildExecution3 = (
    await storage.getManyById([{ executionId: childrenExecutions[2]!.executionId, filters: {} }])
  )[0]!
  expect(updatedChildExecution3.closeStatus).toBe('ready')

  const finalParentExecution1 = (
    await storage.getManyById([{ executionId: parent1.executionId, filters: {} }])
  )[0]!
  const finalParentExecution2 = (
    await storage.getManyById([{ executionId: parent2.executionId, filters: {} }])
  )[0]!
  expect(finalParentExecution1.activeChildrenCount).toBe(2)
  expect(finalParentExecution2.activeChildrenCount).toBe(0)

  now = Date.now()
  updatedCount =
    await storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus({
      isFinished: true,
      closeStatus: 'idle',
      update: { updatedAt: now },
      limit: 10,
    })
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
  areChildrenSequential,
  input,
}: {
  now?: number
  root?: TaskExecutionSummary
  parent?: ParentTaskExecutionSummary
  taskId?: string
  executionId?: string
  isSleepingTask?: boolean
  sleepingTaskUniqueId?: string
  retryOptions?: TaskRetryOptions
  sleepMsBeforeRun?: number
  timeoutMs?: number
  areChildrenSequential?: boolean
  input?: string
} = {}): TaskExecutionStorageValue {
  now = now ?? Date.now()
  taskId = taskId ?? generateTaskId()
  executionId = executionId ?? generateTaskExecutionId()
  isSleepingTask = sleepingTaskUniqueId != null
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
    sleepingTaskUniqueId,
    retryOptions,
    sleepMsBeforeRun,
    timeoutMs,
    areChildrenSequential: areChildrenSequential ?? false,
    input,
    status: isSleepingTask ? 'running' : 'ready',
    isFinished: false,
    retryAttempts: 0,
    startAt: now + sleepMsBeforeRun,
    activeChildrenCount: 0,
    onChildrenFinishedProcessingStatus: 'idle',
    closeStatus: 'idle',
    needsPromiseCancellation: false,
    createdAt: now,
    updatedAt: now,
  }
}

function expectDateApproximatelyEqual(date1: number, date2: number) {
  expect(Math.abs(date1 - date2)).toBeLessThanOrEqual(1500)
}

class TaskExecutionsStorageWrapper implements TaskExecutionsStorage {
  private readonly storage: TaskExecutionsStorage
  private readonly storageSlowdownMs: number

  constructor(storage: TaskExecutionsStorage, storageSlowdownMs: number) {
    this.storage = storage
    this.storageSlowdownMs = storageSlowdownMs
  }

  async insertMany(executions: ReadonlyArray<TaskExecutionStorageValue>): Promise<void> {
    await sleep(this.storageSlowdownMs)
    await this.storage.insertMany(executions)
  }

  async getManyById(
    requests: ReadonlyArray<{ executionId: string; filters?: TaskExecutionStorageGetByIdFilters }>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    await sleep(this.storageSlowdownMs)
    return this.storage.getManyById(requests)
  }

  async getManyBySleepingTaskUniqueId(
    requests: ReadonlyArray<{ sleepingTaskUniqueId: string }>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    await sleep(this.storageSlowdownMs)
    return this.storage.getManyBySleepingTaskUniqueId(requests)
  }

  async updateManyById(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
    }>,
  ): Promise<void> {
    await sleep(this.storageSlowdownMs)
    await this.storage.updateManyById(requests)
  }

  async updateManyByIdAndInsertChildrenIfUpdated(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
      childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
    }>,
  ): Promise<void> {
    await sleep(this.storageSlowdownMs)
    await this.storage.updateManyByIdAndInsertChildrenIfUpdated(requests)
  }

  async updateByStatusAndStartAtLessThanAndReturn(request: {
    status: TaskExecutionStatus
    startAtLessThan: number
    update: TaskExecutionStorageUpdate
    updateExpiresAtWithStartedAt: number
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    await sleep(this.storageSlowdownMs)
    return this.storage.updateByStatusAndStartAtLessThanAndReturn(request)
  }

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(request: {
    status: TaskExecutionStatus
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    await sleep(this.storageSlowdownMs)
    return this.storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
      request,
    )
  }

  async updateByCloseStatusAndReturn(request: {
    closeStatus: TaskExecutionCloseStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    await sleep(this.storageSlowdownMs)
    return this.storage.updateByCloseStatusAndReturn(request)
  }

  async updateByStatusAndIsSleepingTaskAndExpiresAtLessThan(request: {
    status: TaskExecutionStatus
    isSleepingTask: boolean
    expiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    await sleep(this.storageSlowdownMs)
    return this.storage.updateByStatusAndIsSleepingTaskAndExpiresAtLessThan(request)
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThan(request: {
    onChildrenFinishedProcessingExpiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    await sleep(this.storageSlowdownMs)
    return this.storage.updateByOnChildrenFinishedProcessingExpiresAtLessThan(request)
  }

  async updateByCloseExpiresAtLessThan(request: {
    closeExpiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    await sleep(this.storageSlowdownMs)
    return this.storage.updateByCloseExpiresAtLessThan(request)
  }

  async updateByExecutorIdAndNeedsPromiseCancellationAndReturn(request: {
    executorId: string
    needsPromiseCancellation: boolean
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    await sleep(this.storageSlowdownMs)
    return this.storage.updateByExecutorIdAndNeedsPromiseCancellationAndReturn(request)
  }

  async getManyByParentExecutionId(
    requests: ReadonlyArray<{ parentExecutionId: string }>,
  ): Promise<Array<Array<TaskExecutionStorageValue>>> {
    await sleep(this.storageSlowdownMs)
    return this.storage.getManyByParentExecutionId(requests)
  }

  async updateManyByParentExecutionIdAndIsFinished(
    requests: ReadonlyArray<{
      parentExecutionId: string
      isFinished: boolean
      update: TaskExecutionStorageUpdate
    }>,
  ): Promise<void> {
    await sleep(this.storageSlowdownMs)
    await this.storage.updateManyByParentExecutionIdAndIsFinished(requests)
  }

  async updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(request: {
    isFinished: boolean
    closeStatus: TaskExecutionCloseStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    await sleep(this.storageSlowdownMs)
    return this.storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
      request,
    )
  }

  async deleteById(request: { executionId: string }): Promise<void> {
    await this.storage.deleteById(request)
  }

  async deleteAll(): Promise<void> {
    await this.storage.deleteAll()
  }
}

/**
 * Runs a benchmark suite to measure the performance of a TaskExecutionsStorage implementation.
 *
 * @example
 * ```ts
 * import { InMemoryTaskExecutionsStorage } from 'durable-execution'
 * import { runStorageBench } from 'durable-execution-storage-test-utils'
 *
 * await runStorageBench("in memory", () => new InMemoryTaskExecutionsStorage())
 * ```
 *
 * @example
 * ```ts
 * // With cleanup for database storage
 * await runStorageBench(
 *   "custom",
 *   () => new CustomTaskExecutionsStorage({ url: process.env.DATABASE_URL! }),
 *   {
 *     storageCleanup: async (storage) => {
 *       await storage.delete(taskExecutions)
 *     },
 *   },
 * )
 * ```
 *
 * @param name - Name of the storage implementation to test
 * @param getStorage - A function that returns a TaskExecutionsStorage implementation to test
 * @param options - Optional options object
 * @param options.storageCleanup - Optional cleanup function to run after tests complete (e.g., to
 *   remove test database)
 * @param options.storageSlowdownMs - Optional slowdown factor for storage operations.
 * @param options.executorsCount - Number of executors to run in parallel
 * @param options.backgroundProcessesCount - Number of background processes to run in parallel
 * @param options.enableStorageBatching - Whether to enable storage batching. If not provided,
 *   defaults to false.
 * @param options.storageBatchingBackgroundProcessIntraBatchSleepMs - The sleep duration between
 *   batches of storage operations. Only applicable if storage batching is enabled. If not
 *   provided, defaults to 10ms.
 * @param options.totalIterations - Number of iterations to run the benchmark for. If not provided,
 *   defaults to 2.
 * @param options.childTasksCount - Number of child tasks to run in parallel
 * @param options.parentTasksCount - Number of parent tasks to run in parallel
 * @param options.sequentialTasksCount - Number of sequential tasks to run in parallel
 * @param options.pollingTasksCount - Number of polling tasks to run in parallel
 */
export async function runStorageBench<TStorage extends TaskExecutionsStorage>(
  name: string,
  getStorage: (index: number) => TStorage | Promise<TStorage>,
  {
    storageCleanup,
    storageSlowdownMs = 0,
    executorsCount = 1,
    backgroundProcessesCount = 3,
    enableStorageBatching = false,
    storageBatchingBackgroundProcessIntraBatchSleepMs = 10,
    totalIterations = 2,
    childTasksCount = 50,
    parentTasksCount = 100,
    sequentialTasksCount = 100,
    pollingTasksCount = 100,
  }: {
    storageCleanup?: (storage: TStorage) => void | Promise<void>
    storageSlowdownMs?: number
    executorsCount?: number
    backgroundProcessesCount?: number
    enableStorageBatching?: boolean
    storageBatchingBackgroundProcessIntraBatchSleepMs?: number
    totalIterations?: number
    childTasksCount?: number
    parentTasksCount?: number
    sequentialTasksCount?: number
    pollingTasksCount?: number
  } = {},
) {
  if (executorsCount < 1) {
    throw new Error('Executors count must be at least 1')
  }
  if (totalIterations < 1) {
    throw new Error('Total iterations must be at least 1')
  }

  console.log('\n========================================')
  console.log(`Running benchmark for ${name} storage\n`)

  let activeStorages: Array<TStorage> = []
  let activeExecutors: Array<DurableExecutor> = []
  let isShuttingDown = false

  const gracefulShutdown = (signal: string) => {
    if (isShuttingDown) {
      return
    }

    isShuttingDown = true
    console.log(`\n=> Received ${signal}, shutting down gracefully`)
    const shutdownAsync = async () => {
      try {
        if (activeExecutors.length > 0) {
          console.log(`=> Shutting down executors`)
          await Promise.all(activeExecutors.map((executor) => executor.shutdown()))
          console.log('=> Executors shut down successfully')
        }

        if (storageCleanup && activeStorages.length > 0) {
          console.log('=> Running storage cleanup')
          for (const storage of activeStorages) {
            await storageCleanup(storage)
          }
          console.log('=> Storage cleanup completed')
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
    for (let i = 0; i < 1 + totalIterations; i++) {
      if (isShuttingDown) {
        break
      }

      const storages = await Promise.all(
        Array.from({ length: executorsCount }, async (_, index) => await getStorage(index)),
      )
      activeStorages = storages

      for (const storage of storages) {
        await storage.deleteAll()
      }

      const finalStorages =
        storageSlowdownMs && storageSlowdownMs > 0
          ? storages.map((storage) => new TaskExecutionsStorageWrapper(storage, storageSlowdownMs))
          : (storages as Array<TaskExecutionsStorage>)

      const executors = Array.from(
        { length: executorsCount },
        (_, idx) =>
          new DurableExecutor(finalStorages[idx]!, {
            logLevel: 'error',
            enableStorageBatching,
            enableStorageStats: true,
            storageBatchingBackgroundProcessIntraBatchSleepMs,
          }),
      )
      activeExecutors = executors

      let parentTask: Task<number, string>
      let sequentialTask: Task<string, string>
      let pollingTask: Task<string, { isSuccess: false } | { isSuccess: true; output: string }>
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
              children: Array.from({ length: childTasksCount }, (_, index) => ({
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

        const singleTask = executor.task({
          id: 'single',
          timeoutMs: 60_000,
          run: async (ctx, input: string) => {
            await sleep(1)
            return `single_output_${input}`
          },
        })
        sequentialTask = executor.sequentialTasks('sequential', [
          singleTask,
          singleTask,
          singleTask,
          singleTask,
        ])

        let value = 0
        setTimeout(() => {
          value = 1
        }, 1000)

        const pollTask = executor.task({
          id: 'poll',
          timeoutMs: 60_000,
          run: async (ctx, input: string) => {
            await sleep(1)
            if (value === 0) {
              return { isDone: false } as { isDone: false }
            }
            return { isDone: true, output: `poll_output_${input}` } as {
              isDone: true
              output: string
            }
          },
        })
        pollingTask = executor.pollingTask('polling', pollTask, 1000, 100)
      }

      for (const executor of executors) {
        for (let i = 0; i < backgroundProcessesCount; i++) {
          executor.start()
        }
      }

      await sleep(2500)
      const iterationName = i === 0 ? 'warmup iteration' : `iteration ${i}`
      console.log(`=> Running ${iterationName} for ${name} storage`)
      try {
        for (const storage of storages) {
          await storage.deleteAll()
        }
        const startTime = performance.now()
        await runDurableExecutorBench(
          executors[0]!,
          parentTask!,
          sequentialTask!,
          pollingTask!,
          parentTasksCount,
          sequentialTasksCount,
          pollingTasksCount,
        )
        const endTime = performance.now()

        const finalCallsDurationsStats = new Map<
          string,
          { callsCount: number; meanDurationMs: number }
        >()
        for (const [i, executor] of executors.entries()) {
          const callsDurationsStats = executor.getStorageCallsDurationsStats()
          for (const [key, value] of callsDurationsStats.entries()) {
            if (!finalCallsDurationsStats.has(key)) {
              finalCallsDurationsStats.set(key, { callsCount: 0, meanDurationMs: 0 })
            }
            const finalCallsDurationsStatsValue = finalCallsDurationsStats.get(key)!
            const prevCallsCount = finalCallsDurationsStatsValue.callsCount
            const prevMeanDurationMs = finalCallsDurationsStatsValue.meanDurationMs
            finalCallsDurationsStatsValue.callsCount = prevCallsCount + value.callsCount
            finalCallsDurationsStatsValue.meanDurationMs =
              (prevMeanDurationMs * prevCallsCount + value.meanDurationMs * value.callsCount) /
              (prevCallsCount + value.callsCount)
          }

          const perSecondCallsCountsStats = executor.getStoragePerSecondCallsCountsStats()
          console.log(
            `=> Per second call counts stats for ${name} storage for executor ${i + 1}:\n      total: ${perSecondCallsCountsStats.totalCallsCount}\n       mean: ${perSecondCallsCountsStats.meanCallsCount}\n        max: ${perSecondCallsCountsStats.maxCallsCount}`,
          )
        }

        const timingStatsArr = [...finalCallsDurationsStats.entries()].map(([key, value]) => ({
          key,
          callsCount: value.callsCount,
          meanDurationMs: value.meanDurationMs,
        }))
        timingStatsArr.sort((a, b) => b.callsCount - a.callsCount)
        console.log(`=> Timing stats for ${name} storage:`)
        for (const timingStat of timingStatsArr) {
          console.log(
            `  ${timingStat.key}:\n    [${timingStat.callsCount}] ${timingStat.meanDurationMs.toFixed(2)}ms`,
          )
        }

        console.log(
          `=> Completed ${iterationName} for ${name} storage: ${(endTime - startTime).toFixed(2)}ms`,
        )

        if (i > 0) {
          durations.push(endTime - startTime)
        }
      } finally {
        console.log('=> Shutting down executors')
        await Promise.all(executors.map((executor) => executor.shutdown()))
        console.log('=> Executors shut down successfully')
        if (storageCleanup && activeStorages.length > 0) {
          console.log('=> Running storages cleanup')
          for (const storage of storages) {
            await storageCleanup(storage)
          }
          console.log('=> Storages cleanup completed')
        }
        activeExecutors = []
      }
    }

    const mean = durations.reduce((a, b) => a + b, 0) / durations.length
    const min = Math.min(...durations)
    const max = Math.max(...durations)
    console.log(
      `\nBenchmark results for ${name} storage:\n    mean: ${mean.toFixed(2)}ms\n     min: ${min.toFixed(2)}ms\n     max: ${max.toFixed(2)}ms`,
    )
  } catch (error) {
    console.error(`Error running benchmark for ${name} storage:`, error)
    throw error
  } finally {
    console.log('\n========================================\n')

    process.removeAllListeners('SIGINT')
    process.removeAllListeners('SIGTERM')
  }
}

async function runDurableExecutorBench(
  executor: DurableExecutor,
  parentTask: Task<number, string>,
  sequentialTask: Task<string, string>,
  pollingTask: Task<string, { isSuccess: false } | { isSuccess: true; output: string }>,
  parentTasksCount: number,
  sequentialTasksCount: number,
  pollingTasksCount: number,
) {
  console.log('=> Enqueuing tasks')
  const handles = await Promise.all([
    ...Array.from({ length: parentTasksCount }, async (_, i) => {
      return await executor.enqueueTask(parentTask, i)
    }),
    ...Array.from({ length: sequentialTasksCount }, async () => {
      return await executor.enqueueTask(sequentialTask, 'test')
    }),
    ...Array.from({ length: pollingTasksCount }, async () => {
      return await executor.enqueueTask(pollingTask, 'test')
    }),
  ])

  const timings: {
    parent: Array<number>
    sequential: Array<number>
    polling: Array<number>
  } = {
    parent: [],
    sequential: [],
    polling: [],
  }

  console.log('=> Waiting for tasks')
  let finishedCount = 0
  for (const handle of handles) {
    const finalExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 50,
    })
    if (finalExecution.status !== 'completed') {
      console.error(
        `=> Final execution status is not completed: ${JSON.stringify(finalExecution.error)}`,
      )
      throw new Error('Final execution status is not completed')
    }

    const durationMs = finalExecution.finishedAt.getTime() - finalExecution.startedAt.getTime()
    if (finalExecution.output === 'finalize_output') {
      timings.parent.push(durationMs)
    } else if (
      finalExecution.output === 'single_output_single_output_single_output_single_output_test'
    ) {
      timings.sequential.push(durationMs)
    } else if (
      isObject(finalExecution.output) &&
      finalExecution.output.isSuccess &&
      finalExecution.output.output === 'poll_output_test'
    ) {
      timings.polling.push(durationMs)
    } else {
      console.error(`=> Final execution output is incorrect: ${JSON.stringify(finalExecution)}`)
      throw new Error('Final execution output is not finalize_output')
    }

    finishedCount++
    if (finishedCount % 50 === 0) {
      console.log(
        `=> ${finishedCount}/${parentTasksCount + sequentialTasksCount + pollingTasksCount} tasks finished`,
      )
    }
  }

  const parentTimingMeanMs = timings.parent.reduce((a, b) => a + b, 0) / timings.parent.length
  const sequentialTimingMeanMs =
    timings.sequential.reduce((a, b) => a + b, 0) / timings.sequential.length
  const pollingTimingMeanMs = timings.polling.reduce((a, b) => a + b, 0) / timings.polling.length

  console.log(`=> Parent timing mean: ${parentTimingMeanMs.toFixed(2)}ms`)
  console.log(`=> Sequential timing mean: ${sequentialTimingMeanMs.toFixed(2)}ms`)
  console.log(`=> Polling timing mean: ${pollingTimingMeanMs.toFixed(2)}ms`)
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
    try {
      await fs.rm(dirPath, { recursive: true })
    } catch {
      // ignore errors
    }
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
