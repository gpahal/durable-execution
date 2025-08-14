import fs from 'node:fs/promises'
import path from 'node:path'

import {
  DurableExecutor,
  type Task,
  type TaskExecutionsStorage,
  type TaskExecutionStorageGetByIdsFilters,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
  type TaskRetryOptions,
} from 'durable-execution'

import { sleep } from '@gpahal/std/promises'

export async function runStorageTest(
  storage: TaskExecutionsStorage,
  cleanup?: () => void | Promise<void>,
) {
  const executor = new DurableExecutor(storage)
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
  const taskB = executor.sequentialTasks(taskB1, taskB2, taskB3)

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
          { task: taskA1, input: { name: input.name } },
          { task: taskA2, input: { name: input.name } },
          { task: taskA3, input: { name: input.name } },
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
          { task: taskA, input: { name: input.name } },
          { task: taskB, input: { name: input.name } },
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
        children: [{ task: failingTask }],
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
        children: [{ task: taskA1, input: { name: 'world' } }],
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

  let closeTestExecutionCount = 0
  const closeTesttask = executor.task({
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
      return { output: 'parent_output', children: [{ task: closeTesttask }] }
    },
  })

  const handle = await executor.enqueueTask(closeTestParentTask)

  const closeTestFinishedExecution = await handle.waitAndGetFinishedExecution()
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
  const now = new Date()

  const parentExecution = createTaskExecutionStorageValue({
    now,
    taskId: 'storage-test-task-parent',
    executionId: 'te_storage_test_parent',
    retryOptions: { maxAttempts: 3 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ test: 'data-parent' }),
  })
  parentExecution.status = 'waiting_for_children'
  parentExecution.children = [
    { taskId: 'storage-test-task-1', executionId: 'te_storage_test_1' },
    { taskId: 'storage-test-task-2', executionId: 'te_storage_test_2' },
  ]
  parentExecution.activeChildrenCount = 2
  parentExecution.onChildrenFinishedProcessingStatus = 'idle'

  const execution1 = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'storage-test-task-parent', executionId: 'te_storage_test_parent' },
    parent: {
      taskId: 'storage-test-task-parent',
      executionId: 'te_storage_test_parent',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    },
    taskId: 'storage-test-task-1',
    executionId: 'te_storage_test_1',
    retryOptions: { maxAttempts: 3 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ test: 'data1' }),
  })

  const execution2 = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'storage-test-task-parent', executionId: 'te_storage_test_parent' },
    parent: {
      taskId: 'storage-test-task-parent',
      executionId: 'te_storage_test_parent',
      indexInParentChildTaskExecutions: 1,
      isFinalizeTaskOfParentTask: false,
    },
    taskId: 'storage-test-task-2',
    executionId: 'te_storage_test_2',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 100,
    timeoutMs: 10_000,
    input: JSON.stringify({ test: 'data2' }),
  })

  const execution3 = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'root-task', executionId: 'te_root' },
    parent: {
      taskId: 'parent-task',
      executionId: 'te_parent',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    },
    taskId: 'storage-test-task-3',
    executionId: 'te_storage_test_3',
    retryOptions: { maxAttempts: 5 },
    sleepMsBeforeRun: 200,
    timeoutMs: 15_000,
    input: JSON.stringify({ test: 'data3' }),
  })

  const multiParent = createTaskExecutionStorageValue({
    now,
    taskId: 'multi-parent',
    executionId: 'te_multi_parent',
    retryOptions: { maxAttempts: 2 },
    sleepMsBeforeRun: 0,
    timeoutMs: 8000,
    input: JSON.stringify({ multi: true }),
  })
  multiParent.status = 'waiting_for_children'
  multiParent.children = [
    { taskId: 'child-a', executionId: 'te_child_a' },
    { taskId: 'child-b', executionId: 'te_child_b' },
    { taskId: 'child-c', executionId: 'te_child_c' },
    { taskId: 'child-d', executionId: 'te_child_d' },
  ]
  multiParent.activeChildrenCount = 4
  multiParent.onChildrenFinishedProcessingStatus = 'idle'

  const childA = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'multi-parent', executionId: 'te_multi_parent' },
    parent: {
      taskId: 'multi-parent',
      executionId: 'te_multi_parent',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    },
    taskId: 'child-a',
    executionId: 'te_child_a',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 3000,
    input: JSON.stringify({ child: 'a' }),
  })

  const childB = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'multi-parent', executionId: 'te_multi_parent' },
    parent: {
      taskId: 'multi-parent',
      executionId: 'te_multi_parent',
      indexInParentChildTaskExecutions: 1,
      isFinalizeTaskOfParentTask: false,
    },
    taskId: 'child-b',
    executionId: 'te_child_b',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 3000,
    input: JSON.stringify({ child: 'b' }),
  })

  const childC = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'multi-parent', executionId: 'te_multi_parent' },
    parent: {
      taskId: 'multi-parent',
      executionId: 'te_multi_parent',
      indexInParentChildTaskExecutions: 2,
      isFinalizeTaskOfParentTask: false,
    },
    taskId: 'child-c',
    executionId: 'te_child_c',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 3000,
    input: JSON.stringify({ child: 'c' }),
  })

  const childD = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'multi-parent', executionId: 'te_multi_parent' },
    parent: {
      taskId: 'multi-parent',
      executionId: 'te_multi_parent',
      indexInParentChildTaskExecutions: 3,
      isFinalizeTaskOfParentTask: false,
    },
    taskId: 'child-d',
    executionId: 'te_child_d',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 3000,
    input: JSON.stringify({ child: 'd' }),
  })

  const finalizeParent = createTaskExecutionStorageValue({
    now,
    taskId: 'finalize-parent',
    executionId: 'te_finalize_parent',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ finalize: 'parent' }),
  })
  finalizeParent.status = 'waiting_for_children'
  finalizeParent.children = [{ taskId: 'finalize-child', executionId: 'te_finalize_child' }]
  finalizeParent.activeChildrenCount = 1
  finalizeParent.onChildrenFinishedProcessingStatus = 'idle'

  const finalizeChild = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'finalize-parent', executionId: 'te_finalize_parent' },
    parent: {
      taskId: 'finalize-parent',
      executionId: 'te_finalize_parent',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    },
    taskId: 'finalize-child',
    executionId: 'te_finalize_child',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 3000,
    input: JSON.stringify({ finalize: 'child' }),
  })

  const finalizeTask = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'finalize-parent', executionId: 'te_finalize_parent' },
    parent: {
      taskId: 'finalize-parent',
      executionId: 'te_finalize_parent',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: true,
    },
    taskId: 'finalize-task',
    executionId: 'te_finalize_task',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 3000,
    input: JSON.stringify({ finalize: 'task' }),
  })

  const retryExecution = createTaskExecutionStorageValue({
    now,
    taskId: 'retry-task',
    executionId: 'te_retry',
    retryOptions: { maxAttempts: 5, baseDelayMs: 100, delayMultiplier: 2 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ retry: true }),
  })

  const timedOutExecution = createTaskExecutionStorageValue({
    now,
    taskId: 'timeout-task',
    executionId: 'te_timeout',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 1000,
    input: JSON.stringify({ timeout: true }),
  })
  timedOutExecution.status = 'running'
  timedOutExecution.startedAt = now
  timedOutExecution.expiresAt = new Date(now.getTime() - 1000)

  await storage.insert([
    parentExecution,
    execution1,
    execution2,
    execution3,
    multiParent,
    childA,
    childB,
    childC,
    childD,
    finalizeParent,
    finalizeChild,
    finalizeTask,
    retryExecution,
    timedOutExecution,
  ])

  const retrieved = await storage.getByIds([
    'te_storage_test_1',
    'te_storage_test_2',
    'te_storage_test_3',
  ])
  expect(retrieved).toHaveLength(3)
  expect(retrieved[0]).toBeDefined()
  expect(retrieved[0]!.executionId).toBe('te_storage_test_1')
  expect(retrieved[0]!.taskId).toBe('storage-test-task-1')
  expect(retrieved[1]).toBeDefined()
  expect(retrieved[1]!.executionId).toBe('te_storage_test_2')
  expect(retrieved[2]).toBeDefined()
  expect(retrieved[2]!.executionId).toBe('te_storage_test_3')
  expect(retrieved[2]!.root).toEqual({ taskId: 'root-task', executionId: 'te_root' })
  expect(retrieved[2]!.parent).toEqual({
    taskId: 'parent-task',
    executionId: 'te_parent',
    indexInParentChildTaskExecutions: 0,
    isFinalizeTaskOfParentTask: false,
  })

  const partialRetrieved = await storage.getByIds([
    'te_storage_test_1',
    'te_nonexistent',
    'te_storage_test_3',
  ])
  expect(partialRetrieved).toHaveLength(3)
  expect(partialRetrieved[0]).toBeDefined()
  expect(partialRetrieved[0]!.executionId).toBe('te_storage_test_1')
  expect(partialRetrieved[1]).toBeUndefined()
  expect(partialRetrieved[2]).toBeDefined()
  expect(partialRetrieved[2]!.executionId).toBe('te_storage_test_3')

  const filters: TaskExecutionStorageGetByIdsFilters = { statuses: ['ready'] }
  const singleExecution = await storage.getById('te_storage_test_1', filters)
  expect(singleExecution).toBeDefined()
  expect(singleExecution!.executionId).toBe('te_storage_test_1')
  expect(singleExecution!.status).toBe('ready')

  const noMatchFilter: TaskExecutionStorageGetByIdsFilters = { statuses: ['running'] }
  const noMatch = await storage.getById('te_storage_test_1', noMatchFilter)
  expect(noMatch).toBeUndefined()

  const updatedAt = new Date(now.getTime() + 1000)
  const basicUpdate: TaskExecutionStorageUpdate = {
    status: 'running',
    startedAt: updatedAt,
    expiresAt: new Date(updatedAt.getTime() + 5000),
    retryAttempts: 1,
    runOutput: JSON.stringify({ step: 'started' }),
    updatedAt: updatedAt,
  }

  await storage.updateById('te_storage_test_1', {}, basicUpdate)

  const updatedExecution = await storage.getById('te_storage_test_1', {})
  expect(updatedExecution).toBeDefined()
  expect(updatedExecution!.status).toBe('running')
  expectDateApproximatelyEqual(updatedExecution!.startedAt!, updatedAt)
  expectDateApproximatelyEqual(updatedExecution!.expiresAt!, new Date(updatedAt.getTime() + 5000))
  expect(updatedExecution!.retryAttempts).toBe(1)
  expect(updatedExecution!.runOutput).toBe(JSON.stringify({ step: 'started' }))
  expectDateApproximatelyEqual(updatedExecution!.updatedAt, updatedAt)

  const unsetTime = new Date(updatedAt.getTime() + 1000)
  const completedUpdate: TaskExecutionStorageUpdate = {
    status: 'completed',
    output: JSON.stringify({ result: 'success' }),
    unsetRunOutput: true,
    finishedAt: unsetTime,
    decrementParentActiveChildrenCount: true,
    unsetExpiresAt: true,
    unsetError: true,
    updatedAt: unsetTime,
  }

  await storage.updateById('te_storage_test_1', {}, completedUpdate)

  const completedExecution = await storage.getById('te_storage_test_1', {})
  expect(completedExecution).toBeDefined()
  expect(completedExecution!.status).toBe('completed')
  expect(completedExecution!.output).toBe(JSON.stringify({ result: 'success' }))
  expect(completedExecution!.runOutput).toBeUndefined()
  expectDateApproximatelyEqual(completedExecution!.finishedAt!, unsetTime)
  expect(completedExecution!.expiresAt).toBeUndefined()
  expect(completedExecution!.error).toBeUndefined()

  const parentExecution1 = await storage.getById('te_storage_test_parent', {})
  expect(parentExecution1).toBeDefined()
  expect(parentExecution1!.status).toBe('waiting_for_children')
  expect(parentExecution1!.children).toHaveLength(2)
  expect(parentExecution1!.children![0]!.taskId).toBe('storage-test-task-1')
  expect(parentExecution1!.children![1]!.taskId).toBe('storage-test-task-2')
  expect(parentExecution1!.activeChildrenCount).toBe(1)

  const failedUpdate: TaskExecutionStorageUpdate = {
    status: 'failed',
    error: {
      errorType: 'generic',
      message: 'Test failure',
      isRetryable: false,
      isInternal: false,
    },
    unsetRunOutput: true,
    finishedAt: unsetTime,
    decrementParentActiveChildrenCount: true,
    unsetExpiresAt: true,
    updatedAt: unsetTime,
  }

  await storage.updateById('te_storage_test_2', {}, failedUpdate)

  const failedExecution = await storage.getById('te_storage_test_2', {})
  expect(failedExecution).toBeDefined()
  expect(failedExecution!.status).toBe('failed')
  expect(failedExecution!.error).toBeDefined()
  expect(failedExecution!.error!.errorType).toBe('generic')
  expect(failedExecution!.error!.message).toBe('Test failure')
  expect(failedExecution!.error!.isRetryable).toBe(false)
  expect(failedExecution!.error!.isInternal).toBe(false)
  expect(failedExecution!.runOutput).toBeUndefined()
  expectDateApproximatelyEqual(failedExecution!.finishedAt!, unsetTime)

  const parentExecution2 = await storage.getById('te_storage_test_parent', {})
  expect(parentExecution2).toBeDefined()
  expect(parentExecution2!.status).toBe('waiting_for_children')
  expect(parentExecution2!.children).toHaveLength(2)
  expect(parentExecution2!.children![0]!.taskId).toBe('storage-test-task-1')
  expect(parentExecution2!.children![1]!.taskId).toBe('storage-test-task-2')
  expect(parentExecution2!.activeChildrenCount).toBe(0)

  const childrenUpdate: TaskExecutionStorageUpdate = {
    status: 'waiting_for_children',
    children: [
      { taskId: 'child1', executionId: 'te_child1' },
      { taskId: 'child2', executionId: 'te_child2' },
    ],
    activeChildrenCount: 2,
    onChildrenFinishedProcessingStatus: 'idle',
    startedAt: updatedAt,
    expiresAt: new Date(updatedAt.getTime() + 10_000),
    updatedAt: updatedAt,
  }

  await storage.updateById('te_storage_test_3', {}, childrenUpdate)

  const childrenExecution = await storage.getById('te_storage_test_3', {})
  expect(childrenExecution).toBeDefined()
  expect(childrenExecution!.status).toBe('waiting_for_children')
  expect(childrenExecution!.children).toHaveLength(2)
  expect(childrenExecution!.children![0]!.taskId).toBe('child1')
  expect(childrenExecution!.children![1]!.taskId).toBe('child2')
  expect(childrenExecution!.activeChildrenCount).toBe(2)
  expect(childrenExecution!.onChildrenFinishedProcessingStatus).toBe('idle')

  const finalizeUpdate: TaskExecutionStorageUpdate = {
    status: 'waiting_for_finalize',
    finalize: { taskId: 'finalize-task', executionId: 'te_finalize' },
    onChildrenFinishedProcessingStatus: 'processed',
    onChildrenFinishedProcessingFinishedAt: updatedAt,
    unsetOnChildrenFinishedProcessingExpiresAt: true,
    updatedAt: updatedAt,
  }

  await storage.updateById('te_storage_test_3', {}, finalizeUpdate)

  const finalizeExecution = await storage.getById('te_storage_test_3', {})
  expect(finalizeExecution).toBeDefined()
  expect(finalizeExecution!.status).toBe('waiting_for_finalize')
  expect(finalizeExecution!.finalize).toBeDefined()
  expect(finalizeExecution!.finalize!.taskId).toBe('finalize-task')
  expect(finalizeExecution!.onChildrenFinishedProcessingStatus).toBe('processed')
  expectDateApproximatelyEqual(
    finalizeExecution!.onChildrenFinishedProcessingFinishedAt!,
    updatedAt,
  )
  expect(finalizeExecution!.onChildrenFinishedProcessingExpiresAt).toBeUndefined()

  const closeUpdate: TaskExecutionStorageUpdate = {
    closeStatus: 'closing',
    closeExpiresAt: new Date(updatedAt.getTime() + 5000),
    updatedAt: updatedAt,
  }

  await storage.updateById('te_storage_test_1', {}, closeUpdate)

  const closingExecution = await storage.getById('te_storage_test_1', {})
  expect(closingExecution).toBeDefined()
  expect(closingExecution!.closeStatus).toBe('closing')
  expectDateApproximatelyEqual(
    closingExecution!.closeExpiresAt!,
    new Date(updatedAt.getTime() + 5000),
  )

  const closedUpdate: TaskExecutionStorageUpdate = {
    closeStatus: 'closed',
    closedAt: updatedAt,
    unsetCloseExpiresAt: true,
    updatedAt: updatedAt,
  }

  await storage.updateById('te_storage_test_1', {}, closedUpdate)

  const closedExecution = await storage.getById('te_storage_test_1', {})
  expect(closedExecution).toBeDefined()
  expect(closedExecution!.closeStatus).toBe('closed')
  expectDateApproximatelyEqual(closedExecution!.closedAt!, updatedAt)
  expect(closedExecution!.closeExpiresAt).toBeUndefined()

  const cancellationUpdate: TaskExecutionStorageUpdate = {
    needsPromiseCancellation: true,
    updatedAt: updatedAt,
  }

  await storage.updateById('te_storage_test_2', {}, cancellationUpdate)

  const cancellationExecution = await storage.getById('te_storage_test_2', {})
  expect(cancellationExecution).toBeDefined()
  expect(cancellationExecution!.needsPromiseCancellation).toBe(true)

  const cancellationResults = await storage.getByNeedsPromiseCancellationAndIds(
    true,
    ['te_storage_test_1', 'te_storage_test_2', 'te_storage_test_3'],
    10,
  )
  expect(cancellationResults).toHaveLength(1)
  expect(cancellationResults[0]!.executionId).toBe('te_storage_test_2')
  expect(cancellationResults[0]!.needsPromiseCancellation).toBe(true)

  await storage.updateByIdsAndStatuses(['te_storage_test_2'], ['failed'], {
    needsPromiseCancellation: false,
    updatedAt: updatedAt,
  })

  const batchUpdatedExecution = await storage.getById('te_storage_test_2', {})
  expect(batchUpdatedExecution).toBeDefined()
  expect(batchUpdatedExecution!.needsPromiseCancellation).toBe(false)

  const newExecution = createTaskExecutionStorageValue({
    now: updatedAt,
    taskId: 'inserted-task',
    executionId: 'te_inserted',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ inserted: true }),
  })

  await storage.updateByIdAndInsertIfUpdated(
    'te_storage_test_3',
    {},
    {
      status: 'completed',
      output: JSON.stringify({ batch: 'success' }),
      updatedAt: updatedAt,
    },
    [newExecution],
  )

  const batchWithInsertExecution = await storage.getById('te_storage_test_3', {})
  expect(batchWithInsertExecution).toBeDefined()
  expect(batchWithInsertExecution!.status).toBe('completed')
  expect(batchWithInsertExecution!.output).toBe(JSON.stringify({ batch: 'success' }))

  const insertedExecution = await storage.getById('te_inserted', {})
  expect(insertedExecution).toBeDefined()
  expect(insertedExecution!.taskId).toBe('inserted-task')
  expect(insertedExecution!.status).toBe('ready')

  const futureDate = new Date(now.getTime() + 100_000)
  const startAtResults = await storage.updateByStatusAndStartAtLessThanAndReturn(
    'ready',
    futureDate,
    { retryAttempts: 10, updatedAt: updatedAt },
    5,
  )
  expect(startAtResults.length).toBeGreaterThanOrEqual(0)

  const statusFilter: TaskExecutionStorageGetByIdsFilters = {
    statuses: ['completed', 'failed'],
  }
  const statusFilteredExecution = await storage.getById('te_storage_test_1', statusFilter)
  expect(statusFilteredExecution).toBeDefined()
  expect(statusFilteredExecution!.status).toBe('completed')

  await testStatusTransitions(storage, now)
  await testParentChildRelationships(storage, now)
  await testUnsetOperations(storage, now)
  await testRetryAndTimeoutScenarios(storage, now)
  await testBatchOperationsAndFilters(storage, now)
  await testConcurrentUpdates(storage, now)
  await testErrorHandlingAndEdgeCases(storage, now)
  await testOnChildrenFinishedProcessing(storage, now)
}

async function testStatusTransitions(storage: TaskExecutionsStorage, now: Date) {
  const execution = createTaskExecutionStorageValue({
    now,
    taskId: 'status-test',
    executionId: 'te_status',
    retryOptions: { maxAttempts: 3 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ status: 'test' }),
  })
  await storage.insert([execution])

  const transitions = [
    { status: 'ready' as const, startAt: new Date(now.getTime() + 1000) },
    { status: 'running' as const, startedAt: now, expiresAt: new Date(now.getTime() + 5000) },
    { status: 'completed' as const, output: JSON.stringify({ done: true }) },
  ]

  for (const transition of transitions) {
    await storage.updateById('te_status', {}, { ...transition, updatedAt: now })
    const updated = await storage.getById('te_status', {})
    expect(updated).toBeDefined()
    expect(updated!.status).toBe(transition.status)
    if (transition.startAt) {
      expectDateApproximatelyEqual(updated!.startAt, transition.startAt)
    }
    if (transition.startedAt) {
      expectDateApproximatelyEqual(updated!.startedAt!, transition.startedAt)
    }
    if (transition.expiresAt) {
      expectDateApproximatelyEqual(updated!.expiresAt!, transition.expiresAt)
    }
    if (transition.output) {
      expect(updated!.output).toBe(transition.output)
    }
  }

  const cancellableExecution = createTaskExecutionStorageValue({
    now,
    taskId: 'cancel-test',
    executionId: 'te_cancel',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ cancel: 'test' }),
  })
  await storage.insert([cancellableExecution])

  await storage.updateById(
    'te_cancel',
    {},
    {
      status: 'running',
      startedAt: now,
      expiresAt: new Date(now.getTime() + 5000),
      updatedAt: now,
    },
  )

  await storage.updateById(
    'te_cancel',
    {},
    {
      status: 'cancelled',
      finishedAt: now,
      unsetExpiresAt: true,
      updatedAt: now,
    },
  )

  const cancelledExecution = await storage.getById('te_cancel', {})
  expect(cancelledExecution).toBeDefined()
  expect(cancelledExecution!.status).toBe('cancelled')
  expectDateApproximatelyEqual(cancelledExecution!.finishedAt!, now)
  expect(cancelledExecution!.expiresAt).toBeUndefined()

  const finalizeFailExecution = createTaskExecutionStorageValue({
    now,
    taskId: 'finalize-fail-test',
    executionId: 'te_finalize_fail',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ finalizeFail: 'test' }),
  })
  await storage.insert([finalizeFailExecution])

  await storage.updateById(
    'te_finalize_fail',
    {},
    {
      status: 'finalize_failed',
      error: {
        errorType: 'generic',
        message: 'Finalize task failed',
        isRetryable: false,
        isInternal: false,
      },
      finishedAt: now,
      updatedAt: now,
    },
  )

  const finalizeFailedExecution = await storage.getById('te_finalize_fail', {})
  expect(finalizeFailedExecution).toBeDefined()
  expect(finalizeFailedExecution!.status).toBe('finalize_failed')
  expect(finalizeFailedExecution!.error).toBeDefined()
  expect(finalizeFailedExecution!.error!.errorType).toBe('generic')
  expect(finalizeFailedExecution!.error!.message).toBe('Finalize task failed')
  expect(finalizeFailedExecution!.error!.isRetryable).toBe(false)
  expect(finalizeFailedExecution!.error!.isInternal).toBe(false)
}

async function testParentChildRelationships(storage: TaskExecutionsStorage, now: Date) {
  const parent = createTaskExecutionStorageValue({
    now,
    taskId: 'parent-test',
    executionId: 'te_parent_test',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ parent: 'test' }),
  })
  parent.status = 'waiting_for_children'
  parent.children = [
    { taskId: 'child1', executionId: 'te_child1_test' },
    { taskId: 'child2', executionId: 'te_child2_test' },
    { taskId: 'child3', executionId: 'te_child3_test' },
  ]
  parent.activeChildrenCount = 3
  parent.onChildrenFinishedProcessingStatus = 'idle'

  const child1 = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'parent-test', executionId: 'te_parent_test' },
    parent: {
      taskId: 'parent-test',
      executionId: 'te_parent_test',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    },
    taskId: 'child1',
    executionId: 'te_child1_test',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 3000,
    input: JSON.stringify({ child: 1 }),
  })

  const child2 = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'parent-test', executionId: 'te_parent_test' },
    parent: {
      taskId: 'parent-test',
      executionId: 'te_parent_test',
      indexInParentChildTaskExecutions: 1,
      isFinalizeTaskOfParentTask: false,
    },
    taskId: 'child2',
    executionId: 'te_child2_test',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 3000,
    input: JSON.stringify({ child: 2 }),
  })

  const child3 = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'parent-test', executionId: 'te_parent_test' },
    parent: {
      taskId: 'parent-test',
      executionId: 'te_parent_test',
      indexInParentChildTaskExecutions: 2,
      isFinalizeTaskOfParentTask: false,
    },
    taskId: 'child3',
    executionId: 'te_child3_test',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 3000,
    input: JSON.stringify({ child: 3 }),
  })

  await storage.insert([parent, child1, child2, child3])

  await storage.updateById(
    'te_child1_test',
    {},
    {
      status: 'completed',
      output: JSON.stringify({ result: 'child1 done' }),
      finishedAt: now,
      decrementParentActiveChildrenCount: true,
      updatedAt: now,
    },
  )

  const parentAfterChild1 = await storage.getById('te_parent_test', {})
  expect(parentAfterChild1).toBeDefined()
  expect(parentAfterChild1!.activeChildrenCount).toBe(2)

  await storage.updateById(
    'te_child2_test',
    {},
    {
      status: 'failed',
      error: {
        errorType: 'generic',
        message: 'Child 2 failed',
        isRetryable: false,
        isInternal: false,
      },
      finishedAt: now,
      decrementParentActiveChildrenCount: true,
      updatedAt: now,
    },
  )

  const parentAfterChild2 = await storage.getById('te_parent_test', {})
  expect(parentAfterChild2).toBeDefined()
  expect(parentAfterChild2!.activeChildrenCount).toBe(1)

  await storage.updateById(
    'te_child3_test',
    {},
    {
      status: 'cancelled',
      finishedAt: now,
      decrementParentActiveChildrenCount: true,
      updatedAt: now,
    },
  )

  const parentAfterAllChildren = await storage.getById('te_parent_test', {})
  expect(parentAfterAllChildren).toBeDefined()
  expect(parentAfterAllChildren!.activeChildrenCount).toBe(0)

  const nestedParent = createTaskExecutionStorageValue({
    now,
    taskId: 'nested-parent',
    executionId: 'te_nested_parent',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ nested: 'parent' }),
  })
  nestedParent.status = 'waiting_for_children'
  nestedParent.children = [{ taskId: 'nested-child-parent', executionId: 'te_nested_child_parent' }]
  nestedParent.activeChildrenCount = 1
  nestedParent.onChildrenFinishedProcessingStatus = 'idle'

  const nestedChildParent = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'nested-parent', executionId: 'te_nested_parent' },
    parent: {
      taskId: 'nested-parent',
      executionId: 'te_nested_parent',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    },
    taskId: 'nested-child-parent',
    executionId: 'te_nested_child_parent',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ nested: 'child-parent' }),
  })
  nestedChildParent.status = 'waiting_for_children'
  nestedChildParent.children = [
    { taskId: 'nested-grandchild', executionId: 'te_nested_grandchild' },
  ]
  nestedChildParent.activeChildrenCount = 1
  nestedChildParent.onChildrenFinishedProcessingStatus = 'idle'

  const nestedGrandchild = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'nested-parent', executionId: 'te_nested_parent' },
    parent: {
      taskId: 'nested-child-parent',
      executionId: 'te_nested_child_parent',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    },
    taskId: 'nested-grandchild',
    executionId: 'te_nested_grandchild',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 3000,
    input: JSON.stringify({ nested: 'grandchild' }),
  })

  await storage.insert([nestedParent, nestedChildParent, nestedGrandchild])

  await storage.updateById(
    'te_nested_grandchild',
    {},
    {
      status: 'completed',
      output: JSON.stringify({ result: 'grandchild done' }),
      finishedAt: now,
      decrementParentActiveChildrenCount: true,
      updatedAt: now,
    },
  )

  const childParentAfterGrandchild = await storage.getById('te_nested_child_parent', {})
  expect(childParentAfterGrandchild).toBeDefined()
  expect(childParentAfterGrandchild!.activeChildrenCount).toBe(0)

  const topParentAfterGrandchild = await storage.getById('te_nested_parent', {})
  expect(topParentAfterGrandchild).toBeDefined()
  expect(topParentAfterGrandchild!.activeChildrenCount).toBe(1)

  await storage.updateById(
    'te_nested_child_parent',
    {},
    {
      status: 'completed',
      output: JSON.stringify({ result: 'child-parent done' }),
      finishedAt: now,
      decrementParentActiveChildrenCount: true,
      updatedAt: now,
    },
  )

  const topParentAfterAll = await storage.getById('te_nested_parent', {})
  expect(topParentAfterAll).toBeDefined()
  expect(topParentAfterAll!.activeChildrenCount).toBe(0)
}

async function testUnsetOperations(storage: TaskExecutionsStorage, now: Date) {
  const execution = createTaskExecutionStorageValue({
    now,
    taskId: 'unset-test',
    executionId: 'te_unset',
    retryOptions: { maxAttempts: 3 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ unset: 'test' }),
  })
  execution.status = 'running'
  execution.startedAt = now
  execution.expiresAt = new Date(now.getTime() + 5000)
  execution.runOutput = JSON.stringify({ running: true })
  execution.error = {
    errorType: 'generic',
    message: 'Initial error',
    isRetryable: true,
    isInternal: false,
  }
  await storage.insert([execution])

  await storage.updateById(
    'te_unset',
    {},
    {
      status: 'ready',
      unsetRunOutput: true,
      unsetError: true,
      unsetExpiresAt: true,
      updatedAt: now,
    },
  )

  const unsetExecution = await storage.getById('te_unset', {})
  expect(unsetExecution).toBeDefined()
  expect(unsetExecution!.status).toBe('ready')
  expect(unsetExecution!.runOutput).toBeUndefined()
  expect(unsetExecution!.error).toBeUndefined()
  expect(unsetExecution!.expiresAt).toBeUndefined()

  const processingExecution = createTaskExecutionStorageValue({
    now,
    taskId: 'processing-unset-test',
    executionId: 'te_processing_unset',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ processingUnset: 'test' }),
  })
  processingExecution.status = 'waiting_for_children'
  processingExecution.onChildrenFinishedProcessingStatus = 'processing'
  processingExecution.onChildrenFinishedProcessingExpiresAt = new Date(now.getTime() + 5000)
  await storage.insert([processingExecution])

  await storage.updateById(
    'te_processing_unset',
    {},
    {
      onChildrenFinishedProcessingStatus: 'processed',
      onChildrenFinishedProcessingFinishedAt: now,
      unsetOnChildrenFinishedProcessingExpiresAt: true,
      updatedAt: now,
    },
  )

  const processedExecution = await storage.getById('te_processing_unset', {})
  expect(processedExecution).toBeDefined()
  expect(processedExecution!.onChildrenFinishedProcessingStatus).toBe('processed')
  expectDateApproximatelyEqual(processedExecution!.onChildrenFinishedProcessingFinishedAt!, now)
  expect(processedExecution!.onChildrenFinishedProcessingExpiresAt).toBeUndefined()

  const closeExecution = createTaskExecutionStorageValue({
    now,
    taskId: 'close-unset-test',
    executionId: 'te_close_unset',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ closeUnset: 'test' }),
  })
  closeExecution.closeStatus = 'closing'
  closeExecution.closeExpiresAt = new Date(now.getTime() + 5000)
  await storage.insert([closeExecution])

  await storage.updateById(
    'te_close_unset',
    {},
    {
      closeStatus: 'closed',
      closedAt: now,
      unsetCloseExpiresAt: true,
      updatedAt: now,
    },
  )

  const closedExecution = await storage.getById('te_close_unset', {})
  expect(closedExecution).toBeDefined()
  expect(closedExecution!.closeStatus).toBe('closed')
  expectDateApproximatelyEqual(closedExecution!.closedAt!, now)
  expect(closedExecution!.closeExpiresAt).toBeUndefined()
}

async function testRetryAndTimeoutScenarios(storage: TaskExecutionsStorage, now: Date) {
  const retryExecution = createTaskExecutionStorageValue({
    now,
    taskId: 'retry-scenario',
    executionId: 'te_retry_scenario',
    retryOptions: { maxAttempts: 5, baseDelayMs: 100, delayMultiplier: 2 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ retry: 'scenario' }),
  })
  await storage.insert([retryExecution])

  for (let attempt = 1; attempt <= 4; attempt++) {
    await storage.updateById(
      'te_retry_scenario',
      {},
      {
        status: 'running',
        retryAttempts: attempt,
        startedAt: now,
        expiresAt: new Date(now.getTime() + 5000),
        updatedAt: now,
      },
    )

    const runningExecution = await storage.getById('te_retry_scenario', {})
    expect(runningExecution).toBeDefined()
    expect(runningExecution!.status).toBe('running')
    expect(runningExecution!.retryAttempts).toBe(attempt)

    if (attempt < 4) {
      const retryDelay = 100 * Math.pow(2, attempt - 1)
      await storage.updateById(
        'te_retry_scenario',
        {},
        {
          status: 'ready',
          error: {
            errorType: 'generic',
            message: `Attempt ${attempt} failed`,
            isRetryable: true,
            isInternal: false,
          },
          startAt: new Date(now.getTime() + retryDelay),
          unsetExpiresAt: true,
          updatedAt: now,
        },
      )

      const retryReadyExecution = await storage.getById('te_retry_scenario', {})
      expect(retryReadyExecution).toBeDefined()
      expect(retryReadyExecution!.status).toBe('ready')
      expectDateApproximatelyEqual(
        retryReadyExecution!.startAt,
        new Date(now.getTime() + retryDelay),
      )
    }
  }

  await storage.updateById(
    'te_retry_scenario',
    {},
    {
      status: 'completed',
      output: JSON.stringify({ result: 'success after retries' }),
      finishedAt: now,
      unsetError: true,
      unsetExpiresAt: true,
      updatedAt: now,
    },
  )

  const completedRetryExecution = await storage.getById('te_retry_scenario', {})
  expect(completedRetryExecution).toBeDefined()
  expect(completedRetryExecution!.status).toBe('completed')
  expect(completedRetryExecution!.retryAttempts).toBe(4)
  expect(completedRetryExecution!.output).toBe(JSON.stringify({ result: 'success after retries' }))

  const timeoutExecution = createTaskExecutionStorageValue({
    now,
    taskId: 'timeout-scenario',
    executionId: 'te_timeout_scenario',
    retryOptions: { maxAttempts: 2 },
    sleepMsBeforeRun: 0,
    timeoutMs: 1000,
    input: JSON.stringify({ timeout: 'scenario' }),
  })
  timeoutExecution.status = 'running'
  timeoutExecution.startedAt = now
  timeoutExecution.expiresAt = new Date(now.getTime() - 1000)
  await storage.insert([timeoutExecution])

  await storage.updateById(
    'te_timeout_scenario',
    {},
    {
      status: 'failed',
      error: {
        errorType: 'timed_out',
        message: 'Task execution timed out',
        isRetryable: true,
        isInternal: false,
      },
      finishedAt: now,
      unsetExpiresAt: true,
      updatedAt: now,
    },
  )

  const timedOutExecution = await storage.getById('te_timeout_scenario', {})
  expect(timedOutExecution).toBeDefined()
  expect(timedOutExecution!.status).toBe('failed')
  expect(timedOutExecution!.error).toBeDefined()
  expect(timedOutExecution!.error!.errorType).toBe('timed_out')
  expect(timedOutExecution!.error!.message).toBe('Task execution timed out')
  expect(timedOutExecution!.error!.isRetryable).toBe(true)
  expect(timedOutExecution!.error!.isInternal).toBe(false)
  expectDateApproximatelyEqual(timedOutExecution!.finishedAt!, now)
}

async function testBatchOperationsAndFilters(storage: TaskExecutionsStorage, now: Date) {
  const batchExecutions = []
  for (let i = 0; i < 5; i++) {
    const execution = createTaskExecutionStorageValue({
      now,
      taskId: `batch-task-${i}`,
      executionId: `te_batch_${i}`,
      retryOptions: { maxAttempts: 1 },
      sleepMsBeforeRun: i * 100,
      timeoutMs: 5000,
      input: JSON.stringify({ batch: i }),
    })
    if (i % 2 === 0) {
      execution.status = 'ready'
    } else {
      execution.status = 'running'
      execution.startedAt = now
      execution.expiresAt = new Date(now.getTime() + 5000)
    }
    batchExecutions.push(execution)
  }
  await storage.insert(batchExecutions)

  const batchIds = batchExecutions.map((e) => e.executionId)
  const allExecutions = await storage.getByIds(batchIds)

  const readyCount = allExecutions.filter((e) => e !== undefined && e.status === 'ready').length
  expect(readyCount).toBe(3)

  const runningCount = allExecutions.filter((e) => e !== undefined && e.status === 'running').length
  expect(runningCount).toBe(2)

  await storage.updateByIdsAndStatuses(['te_batch_1', 'te_batch_3'], ['running'], {
    status: 'completed',
    output: JSON.stringify({ batch: 'completed' }),
    finishedAt: now,
    unsetExpiresAt: true,
    updatedAt: now,
  })

  const updatedBatch1 = await storage.getById('te_batch_1', {})
  expect(updatedBatch1).toBeDefined()
  expect(updatedBatch1!.status).toBe('completed')

  const updatedBatch3 = await storage.getById('te_batch_3', {})
  expect(updatedBatch3).toBeDefined()
  expect(updatedBatch3!.status).toBe('completed')

  const allExecutionsUpdated = await storage.getByIds(batchIds)

  const multiStatusCount = allExecutionsUpdated.filter(
    (e) => e !== undefined && (e.status === 'ready' || e.status === 'completed'),
  ).length
  expect(multiStatusCount).toBe(5)

  const needsCancellationExecutions = []
  for (let i = 0; i < 3; i++) {
    const execution = createTaskExecutionStorageValue({
      now,
      taskId: `cancellation-task-${i}`,
      executionId: `te_cancellation_${i}`,
      retryOptions: { maxAttempts: 1 },
      sleepMsBeforeRun: 0,
      timeoutMs: 5000,
      input: JSON.stringify({ cancellation: i }),
    })
    execution.needsPromiseCancellation = i < 2
    needsCancellationExecutions.push(execution)
  }
  await storage.insert(needsCancellationExecutions)

  const cancellationResults = await storage.getByNeedsPromiseCancellationAndIds(
    true,
    needsCancellationExecutions.map((e) => e.executionId),
    10,
  )
  expect(cancellationResults).toHaveLength(2)
  expect(cancellationResults[0]!.needsPromiseCancellation).toBe(true)
  expect(cancellationResults[1]!.needsPromiseCancellation).toBe(true)

  const noCancellationResults = await storage.getByNeedsPromiseCancellationAndIds(
    false,
    needsCancellationExecutions.map((e) => e.executionId),
    10,
  )
  expect(noCancellationResults).toHaveLength(1)
  expect(noCancellationResults[0]!.needsPromiseCancellation).toBe(false)
}

async function testConcurrentUpdates(storage: TaskExecutionsStorage, now: Date) {
  const concurrentParent = createTaskExecutionStorageValue({
    now,
    taskId: 'concurrent-parent',
    executionId: 'te_concurrent_parent',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ concurrent: 'parent' }),
  })
  concurrentParent.status = 'waiting_for_children'
  concurrentParent.children = []
  concurrentParent.activeChildrenCount = 10
  concurrentParent.onChildrenFinishedProcessingStatus = 'idle'

  const concurrentChildren = []
  for (let i = 0; i < 10; i++) {
    const child = createTaskExecutionStorageValue({
      now,
      root: { taskId: 'concurrent-parent', executionId: 'te_concurrent_parent' },
      parent: {
        taskId: 'concurrent-parent',
        executionId: 'te_concurrent_parent',
        indexInParentChildTaskExecutions: i,
        isFinalizeTaskOfParentTask: false,
      },
      taskId: `concurrent-child-${i}`,
      executionId: `te_concurrent_child_${i}`,
      retryOptions: { maxAttempts: 1 },
      sleepMsBeforeRun: 0,
      timeoutMs: 3000,
      input: JSON.stringify({ concurrent: 'child', index: i }),
    })
    concurrentChildren.push(child)
    concurrentParent.children.push({
      taskId: `concurrent-child-${i}`,
      executionId: `te_concurrent_child_${i}`,
    })
  }

  await storage.insert([concurrentParent, ...concurrentChildren])

  const updatePromises = concurrentChildren.map((child, index) =>
    storage.updateById(
      child.executionId,
      {},
      {
        status: index % 2 === 0 ? 'completed' : 'failed',
        output: index % 2 === 0 ? JSON.stringify({ result: `child ${index} done` }) : undefined,
        error:
          index % 2 === 1
            ? {
                errorType: 'generic',
                message: `Child ${index} failed`,
                isRetryable: false,
                isInternal: false,
              }
            : undefined,
        finishedAt: now,
        decrementParentActiveChildrenCount: true,
        updatedAt: now,
      },
    ),
  )

  await Promise.all(updatePromises)

  const parentAfterConcurrentUpdates = await storage.getById('te_concurrent_parent', {})
  expect(parentAfterConcurrentUpdates).toBeDefined()
  expect(parentAfterConcurrentUpdates!.activeChildrenCount).toBe(0)

  for (let i = 0; i < 10; i++) {
    const child = await storage.getById(`te_concurrent_child_${i}`, {})
    expect(child).toBeDefined()
    if (i % 2 === 0) {
      expect(child!.status).toBe('completed')
      expect(child!.output).toBe(JSON.stringify({ result: `child ${i} done` }))
    } else {
      expect(child!.status).toBe('failed')
      expect(child!.error).toBeDefined()
      expect(child!.error!.message).toBe(`Child ${i} failed`)
    }
  }

  const raceConditionExecution = createTaskExecutionStorageValue({
    now,
    taskId: 'race-condition',
    executionId: 'te_race_condition',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ race: 'condition' }),
  })
  await storage.insert([raceConditionExecution])

  const conflictingUpdates = [
    storage.updateByIdsAndStatuses(['te_race_condition'], ['ready'], {
      status: 'running',
      startedAt: now,
      expiresAt: new Date(now.getTime() + 5000),
      updatedAt: now,
    }),
    storage.updateByIdsAndStatuses(['te_race_condition'], ['ready'], {
      status: 'cancelled',
      finishedAt: now,
      updatedAt: now,
    }),
  ]

  await Promise.all(conflictingUpdates)

  const raceConditionResult = await storage.getById('te_race_condition', {})
  expect(raceConditionResult).toBeDefined()
  expect(['running', 'cancelled', 'ready']).toContain(raceConditionResult!.status)
}

async function testErrorHandlingAndEdgeCases(storage: TaskExecutionsStorage, now: Date) {
  const errorTypes = [
    {
      errorType: 'generic' as const,
      message: 'Generic error',
      isRetryable: false,
      isInternal: false,
    },
    {
      errorType: 'timed_out' as const,
      message: 'Timeout error',
      isRetryable: true,
      isInternal: false,
    },
    {
      errorType: 'not_found' as const,
      message: 'Not found error',
      isRetryable: false,
      isInternal: false,
    },
    {
      errorType: 'cancelled' as const,
      message: 'Cancelled error',
      isRetryable: false,
      isInternal: false,
    },
    {
      errorType: 'generic' as const,
      message: 'Internal error',
      isRetryable: false,
      isInternal: true,
    },
  ]

  for (const [index, errorType] of errorTypes.entries()) {
    const execution = createTaskExecutionStorageValue({
      now,
      taskId: `error-type-${index}`,
      executionId: `te_error_type_${index}`,
      retryOptions: { maxAttempts: 1 },
      sleepMsBeforeRun: 0,
      timeoutMs: 5000,
      input: JSON.stringify({ errorType: errorType.errorType }),
    })
    await storage.insert([execution])

    await storage.updateById(
      `te_error_type_${index}`,
      {},
      {
        status: 'failed',
        error: errorType,
        finishedAt: now,
        updatedAt: now,
      },
    )

    const errorExecution = await storage.getById(`te_error_type_${index}`, {})
    expect(errorExecution).toBeDefined()
    expect(errorExecution!.status).toBe('failed')
    expect(errorExecution!.error).toBeDefined()
    expect(errorExecution!.error!.errorType).toBe(errorType.errorType)
    expect(errorExecution!.error!.message).toBe(errorType.message)
    expect(errorExecution!.error!.isRetryable).toBe(errorType.isRetryable)
    expect(errorExecution!.error!.isInternal).toBe(errorType.isInternal)
  }

  const nullExecution = createTaskExecutionStorageValue({
    now,
    taskId: 'null-test',
    executionId: 'te_null',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify(null),
  })
  await storage.insert([nullExecution])

  await storage.updateById(
    'te_null',
    {},
    {
      status: 'completed',
      output: JSON.stringify(null),
      finishedAt: now,
      updatedAt: now,
    },
  )

  const nullResultExecution = await storage.getById('te_null', {})
  expect(nullResultExecution).toBeDefined()
  expect(nullResultExecution!.status).toBe('completed')
  expect(nullResultExecution!.output).toBe(JSON.stringify(null))

  const largeExecution = createTaskExecutionStorageValue({
    now,
    taskId: 'large-data',
    executionId: 'te_large',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ data: 'x'.repeat(10_000) }),
  })
  await storage.insert([largeExecution])

  await storage.updateById(
    'te_large',
    {},
    {
      status: 'completed',
      output: JSON.stringify({ result: 'y'.repeat(10_000) }),
      finishedAt: now,
      updatedAt: now,
    },
  )

  const largeResultExecution = await storage.getById('te_large', {})
  expect(largeResultExecution).toBeDefined()
  expect(largeResultExecution!.status).toBe('completed')
  expect(largeResultExecution!.output).toContain('y'.repeat(100))

  await storage.updateById(
    'te_nonexistent_update',
    {},
    {
      status: 'completed',
      updatedAt: now,
    },
  )

  const emptyFilterResult = await storage.getByIds(['te_nonexistent_1', 'te_nonexistent_2'])
  expect(emptyFilterResult).toHaveLength(2)
  expect(emptyFilterResult[0]).toBeUndefined()
  expect(emptyFilterResult[1]).toBeUndefined()
}

async function testOnChildrenFinishedProcessing(storage: TaskExecutionsStorage, now: Date) {
  const processingParent = createTaskExecutionStorageValue({
    now,
    taskId: 'processing-parent',
    executionId: 'te_processing_parent',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 5000,
    input: JSON.stringify({ processing: 'parent' }),
  })
  processingParent.status = 'waiting_for_children'
  processingParent.children = [{ taskId: 'processing-child', executionId: 'te_processing_child' }]
  processingParent.activeChildrenCount = 1
  processingParent.onChildrenFinishedProcessingStatus = 'idle'

  const processingChild = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'processing-parent', executionId: 'te_processing_parent' },
    parent: {
      taskId: 'processing-parent',
      executionId: 'te_processing_parent',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    },
    taskId: 'processing-child',
    executionId: 'te_processing_child',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 3000,
    input: JSON.stringify({ processing: 'child' }),
  })

  await storage.insert([processingParent, processingChild])

  await storage.updateById(
    'te_processing_child',
    {},
    {
      status: 'completed',
      output: JSON.stringify({ result: 'child done' }),
      finishedAt: now,
      decrementParentActiveChildrenCount: true,
      updatedAt: now,
    },
  )

  const parentAfterChildComplete = await storage.getById('te_processing_parent', {})
  expect(parentAfterChildComplete).toBeDefined()
  expect(parentAfterChildComplete!.activeChildrenCount).toBe(0)

  await storage.updateById(
    'te_processing_parent',
    {},
    {
      onChildrenFinishedProcessingStatus: 'processing',
      onChildrenFinishedProcessingExpiresAt: new Date(now.getTime() + 5000),
      updatedAt: now,
    },
  )

  const parentProcessing = await storage.getById('te_processing_parent', {})
  expect(parentProcessing).toBeDefined()
  expect(parentProcessing!.onChildrenFinishedProcessingStatus).toBe('processing')
  expectDateApproximatelyEqual(
    parentProcessing!.onChildrenFinishedProcessingExpiresAt!,
    new Date(now.getTime() + 5000),
  )

  await storage.updateById(
    'te_processing_parent',
    {},
    {
      onChildrenFinishedProcessingStatus: 'processed',
      onChildrenFinishedProcessingFinishedAt: now,
      unsetOnChildrenFinishedProcessingExpiresAt: true,
      updatedAt: now,
    },
  )

  const parentProcessed = await storage.getById('te_processing_parent', {})
  expect(parentProcessed).toBeDefined()
  expect(parentProcessed!.onChildrenFinishedProcessingStatus).toBe('processed')
  expectDateApproximatelyEqual(parentProcessed!.onChildrenFinishedProcessingFinishedAt!, now)
  expect(parentProcessed!.onChildrenFinishedProcessingExpiresAt).toBeUndefined()

  await storage.updateById(
    'te_processing_parent',
    {},
    {
      status: 'waiting_for_finalize',
      finalize: { taskId: 'processing-finalize', executionId: 'te_processing_finalize' },
      updatedAt: now,
    },
  )

  const parentWaitingForFinalize = await storage.getById('te_processing_parent', {})
  expect(parentWaitingForFinalize).toBeDefined()
  expect(parentWaitingForFinalize!.status).toBe('waiting_for_finalize')
  expect(parentWaitingForFinalize!.finalize).toBeDefined()
  expect(parentWaitingForFinalize!.finalize!.taskId).toBe('processing-finalize')

  const processingFinalizeTask = createTaskExecutionStorageValue({
    now,
    root: { taskId: 'processing-parent', executionId: 'te_processing_parent' },
    parent: {
      taskId: 'processing-parent',
      executionId: 'te_processing_parent',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: true,
    },
    taskId: 'processing-finalize',
    executionId: 'te_processing_finalize',
    retryOptions: { maxAttempts: 1 },
    sleepMsBeforeRun: 0,
    timeoutMs: 3000,
    input: JSON.stringify({ finalize: true }),
  })

  await storage.insert([processingFinalizeTask])

  await storage.updateById(
    'te_processing_finalize',
    {},
    {
      status: 'completed',
      output: JSON.stringify({ finalized: true }),
      finishedAt: now,
      updatedAt: now,
    },
  )

  await storage.updateById(
    'te_processing_parent',
    {},
    {
      status: 'completed',
      output: JSON.stringify({ parent: 'completed' }),
      finishedAt: now,
      updatedAt: now,
    },
  )

  const parentCompleted = await storage.getById('te_processing_parent', {})
  expect(parentCompleted).toBeDefined()
  expect(parentCompleted!.status).toBe('completed')
  expect(parentCompleted!.output).toBe(JSON.stringify({ parent: 'completed' }))
}

export function createTaskExecutionStorageValue({
  now,
  root,
  parent,
  taskId,
  executionId,
  retryOptions,
  sleepMsBeforeRun,
  timeoutMs,
  input,
}: {
  now: Date
  root?: {
    taskId: string
    executionId: string
  }
  parent?: {
    taskId: string
    executionId: string
    indexInParentChildTaskExecutions: number
    isFinalizeTaskOfParentTask: boolean
  }
  taskId: string
  executionId: string
  retryOptions: TaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
  input: string
}): TaskExecutionStorageValue {
  return {
    root,
    parent,
    taskId,
    executionId,
    retryOptions,
    sleepMsBeforeRun,
    timeoutMs,
    input,
    status: 'ready',
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

export async function withTemporaryDirectory(fn: (dirPath: string) => Promise<void>) {
  const dirPath = await fs.mkdtemp('.tmp_')
  try {
    await fn(dirPath)
  } finally {
    await fs.rm(dirPath, { recursive: true })
  }
}

export async function withTemporaryFile(filename: string, fn: (file: string) => Promise<void>) {
  return withTemporaryDirectory(async (dirPath) => {
    const filePath = path.join(dirPath, filename)
    await fn(filePath)
  })
}

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
