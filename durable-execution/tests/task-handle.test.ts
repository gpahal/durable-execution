import { sleep } from '@gpahal/std/promises'

import {
  createCancelSignal,
  createTimeoutCancelSignal,
  DurableExecutor,
  InMemoryTaskExecutionsStorage,
} from '../src'

describe('taskHandle', () => {
  let storage: InMemoryTaskExecutionsStorage
  let executor: DurableExecutor

  beforeEach(() => {
    storage = new InMemoryTaskExecutionsStorage()
    executor = new DurableExecutor(storage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    executor.startBackgroundProcesses()
  })

  afterEach(async () => {
    if (executor) {
      await executor.shutdown()
    }
  })

  it('should handle invalid task id', async () => {
    await expect(
      executor.getTaskExecutionHandle(
        {
          id: 'invalid',
          retryOptions: { maxAttempts: 1 },
          sleepMsBeforeRun: 0,
          timeoutMs: 10_000,
          isSleepingTask: false,
          areChildrenSequential: false,
        },
        'invalid',
      ),
    ).rejects.toThrow('Task invalid not found')
  })

  it('should handle correct execution id', async () => {
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: async () => {
        executionCount++
        await sleep(1000)
        return 'test'
      },
    })

    const originalHandle = await executor.enqueueTask(task)
    const executionId = originalHandle.getExecutionId()
    const handle = await executor.getTaskExecutionHandle(task, executionId)
    expect(handle.getTaskId()).toBe('test')
    expect(handle.getExecutionId()).toBeDefined()

    await sleep(250)
    const execution = await handle.getExecution()
    expect(executionCount).toBe(1)
    expect(execution.status).toBe('running')
    expect(execution.taskId).toBe('test')
  })

  it('should handle invalid execution id', async () => {
    const task = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: async () => {
        await sleep(1000)
        return 'test'
      },
    })

    await expect(executor.getTaskExecutionHandle(task, 'invalid')).rejects.toThrow(
      'Task execution invalid not found',
    )
  })

  it('should handle wait and get finished task execution with cancelled signal', async () => {
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: async () => {
        await sleep(1)
        return 'test'
      },
    })

    const originalHandle = await executor.enqueueTask(task)
    const executionId = originalHandle.getExecutionId()
    const handle = await executor.getTaskExecutionHandle(task, executionId)
    expect(handle.getTaskId()).toBe('test')
    expect(handle.getExecutionId()).toBeDefined()

    const [cancelSignal, cancel] = createCancelSignal()
    cancel()
    await expect(
      handle.waitAndGetFinishedExecution({ signal: cancelSignal, pollingIntervalMs: 100 }),
    ).rejects.toThrow('Task execution cancelled')
  })

  it('should handle wait and get finished task execution with cancelled abort signal', async () => {
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: async () => {
        await sleep(1)
        return 'test'
      },
    })

    const originalHandle = await executor.enqueueTask(task)
    const executionId = originalHandle.getExecutionId()
    const handle = await executor.getTaskExecutionHandle(task, executionId)
    expect(handle.getTaskId()).toBe('test')
    expect(handle.getExecutionId()).toBeDefined()

    const abortController = new AbortController()
    abortController.abort()
    await expect(
      handle.waitAndGetFinishedExecution({
        signal: abortController.signal,
        pollingIntervalMs: 100,
      }),
    ).rejects.toThrow('Task execution cancelled')
  })

  it('should handle wait and get finished task execution with cancel signal after finishing', async () => {
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: async () => {
        await sleep(100)
        return 'test'
      },
    })

    const originalHandle = await executor.enqueueTask(task)
    const executionId = originalHandle.getExecutionId()
    const handle = await executor.getTaskExecutionHandle(task, executionId)
    expect(handle.getTaskId()).toBe('test')
    expect(handle.getExecutionId()).toBeDefined()

    const [cancelSignal, clearTimeout] = createTimeoutCancelSignal(10_000)
    await sleep(1000)
    await expect(
      handle.waitAndGetFinishedExecution({ signal: cancelSignal, pollingIntervalMs: 100 }),
    ).resolves.toBeDefined()
    clearTimeout()
  })

  it('should handle wait and get finished task execution with cancel signal before finishing', async () => {
    const task = executor.task({
      id: 'test',
      timeoutMs: 5000,
      run: async () => {
        await sleep(1000)
        return 'test'
      },
    })

    const originalHandle = await executor.enqueueTask(task)
    const executionId = originalHandle.getExecutionId()
    const handle = await executor.getTaskExecutionHandle(task, executionId)
    expect(handle.getTaskId()).toBe('test')
    expect(handle.getExecutionId()).toBeDefined()

    const [cancelSignal, clearTimeout] = createTimeoutCancelSignal(1000)
    await expect(
      handle.waitAndGetFinishedExecution({ signal: cancelSignal, pollingIntervalMs: 100 }),
    ).rejects.toThrow('Task execution cancelled')
    clearTimeout()
  })
})
