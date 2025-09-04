import { sleep } from '@gpahal/std/promises'

import { DurableExecutor, InMemoryTaskExecutionsStorage } from '../src'

describe('taskHandle', () => {
  let storage: InMemoryTaskExecutionsStorage
  let executor: DurableExecutor

  beforeEach(async () => {
    storage = new InMemoryTaskExecutionsStorage()
    executor = await DurableExecutor.make(storage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    await executor.start()
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
          taskType: 'task',
          id: 'invalid',
          retryOptions: { maxAttempts: 1 },
          sleepMsBeforeRun: 0,
          timeoutMs: 10_000,
        },
        'invalid',
      ),
    ).rejects.toThrow('Task not found [taskId=invalid]')
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
    const executionId = originalHandle.executionId
    const handle = await executor.getTaskExecutionHandle(task, executionId)
    expect(handle.taskId).toBe('test')
    expect(handle.executionId).toBeDefined()

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
      'Task execution not found [executionId=invalid]',
    )
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
    const executionId = originalHandle.executionId
    const handle = await executor.getTaskExecutionHandle(task, executionId)
    expect(handle.taskId).toBe('test')
    expect(handle.executionId).toBeDefined()

    const abortController = new AbortController()
    abortController.abort()
    await expect(
      handle.waitAndGetFinishedExecution({
        signal: abortController.signal,
        pollingIntervalMs: 100,
      }),
    ).rejects.toThrow('Task execution cancelled')
  })

  it('should handle wait and get finished task execution with cancelled abort signal after polling delay', async () => {
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: async () => {
        await sleep(1)
        return 'test'
      },
    })

    const originalHandle = await executor.enqueueTask(task)
    const executionId = originalHandle.executionId
    const handle = await executor.getTaskExecutionHandle(task, executionId)
    expect(handle.taskId).toBe('test')
    expect(handle.executionId).toBeDefined()

    const abortController = new AbortController()
    setTimeout(() => {
      abortController.abort()
    }, 500)
    await expect(
      handle.waitAndGetFinishedExecution({
        signal: abortController.signal,
        pollingIntervalMs: 1000,
      }),
    ).rejects.toThrow('Task execution cancelled')
  })

  it('should handle wait and get finished task execution with abort signal after finishing', async () => {
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: async () => {
        await sleep(100)
        return 'test'
      },
    })

    const originalHandle = await executor.enqueueTask(task)
    const executionId = originalHandle.executionId
    const handle = await executor.getTaskExecutionHandle(task, executionId)
    expect(handle.taskId).toBe('test')
    expect(handle.executionId).toBeDefined()

    const abortController = new AbortController()
    const timeout = setTimeout(() => {
      abortController.abort()
    }, 10_000)
    await sleep(1000)
    await expect(
      handle.waitAndGetFinishedExecution({
        signal: abortController.signal,
        pollingIntervalMs: 100,
      }),
    ).resolves.toBeDefined()
    clearTimeout(timeout)
  })

  it('should handle wait and get finished task execution with abort signal before finishing', async () => {
    const task = executor.task({
      id: 'test',
      timeoutMs: 5000,
      run: async () => {
        await sleep(2500)
        return 'test'
      },
    })

    const originalHandle = await executor.enqueueTask(task)
    const executionId = originalHandle.executionId
    const handle = await executor.getTaskExecutionHandle(task, executionId)
    expect(handle.taskId).toBe('test')
    expect(handle.executionId).toBeDefined()

    const abortController = new AbortController()
    const timeout = setTimeout(() => {
      abortController.abort()
    }, 250)
    await expect(
      handle.waitAndGetFinishedExecution({
        signal: abortController.signal,
        pollingIntervalMs: 100,
      }),
    ).rejects.toThrow('Task execution cancelled')
    clearTimeout(timeout)
  })

  it('should handle invalid sleeping task unique id', async () => {
    const task = executor.sleepingTask<string>({
      id: 'test',
      timeoutMs: 1000,
    })

    // @ts-expect-error - Testing invalid input
    await expect(executor.enqueueTask(task, undefined)).rejects.toThrow()

    // @ts-expect-error - Testing invalid input
    await expect(executor.enqueueTask(task, 10)).rejects.toThrow()

    await expect(executor.enqueueTask(task, '')).rejects.toThrow()

    await expect(executor.enqueueTask(task, 'a'.repeat(300))).rejects.toThrow()
  })
})
