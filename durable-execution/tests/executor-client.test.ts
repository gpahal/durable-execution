import {
  DurableExecutionError,
  DurableExecutionNotFoundError,
  DurableExecutor,
  DurableExecutorClient,
  InMemoryTaskExecutionsStorage,
} from '../src'

describe('executorClient', () => {
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

  it('should complete task', async () => {
    let executionCount = 0
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => {
        executionCount++
        return 'test'
      },
    })

    const tasks = { test: testTask }

    const executorClient = new DurableExecutorClient(storage, tasks, {
      logLevel: 'error',
    })
    const handle = await executorClient.enqueueTask('test')

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBe('test')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail task', async () => {
    let executionCount = 0
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => {
        executionCount++
        throw new Error('test')
      },
    })

    const tasks = { test: testTask }

    const executorClient = new DurableExecutorClient(storage, tasks, {
      logLevel: 'error',
    })
    const handle = await executorClient.enqueueTask('test')

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('test')
    expect(finishedExecution.error?.errorType).toBe('generic')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should handle invalid client options', () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    const tasks = { test: testTask }

    expect(() => {
      new DurableExecutorClient(storage, tasks, {
        // @ts-expect-error - Testing invalid input
        maxSerializedInputDataSize: 'invalid',
      })
    }).toThrow('Invalid options')
  })

  it('should throw when enqueuing unknown task', async () => {
    const executorClient = new DurableExecutorClient(
      storage,
      {},
      {
        logLevel: 'error',
      },
    )

    await expect(async () => {
      // @ts-expect-error - Testing invalid input
      await executorClient.enqueueTask('unknown')
    }).rejects.toThrow(DurableExecutionNotFoundError)
  })

  it('should throw when getting handle for unknown task', async () => {
    const executorClient = new DurableExecutorClient(
      storage,
      {},
      {
        logLevel: 'error',
      },
    )

    await expect(async () => {
      // @ts-expect-error - Testing invalid input
      await executorClient.getTaskExecutionHandle('unknown', 'some-execution-id')
    }).rejects.toThrow(DurableExecutionNotFoundError)
  })

  it('should throw when getting handle for non-existent execution', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    const tasks = { test: testTask }
    const executorClient = new DurableExecutorClient(storage, tasks, {
      logLevel: 'error',
    })

    await expect(async () => {
      await executorClient.getTaskExecutionHandle('test', 'non-existent-execution-id')
    }).rejects.toThrow(DurableExecutionNotFoundError)
  })

  it('should throw when getting handle for execution belonging to different task', async () => {
    const testTask1 = executor.task({
      id: 'test1',
      timeoutMs: 10_000,
      run: () => 'test1',
    })

    const testTask2 = executor.task({
      id: 'test2',
      timeoutMs: 10_000,
      run: () => 'test2',
    })

    const tasks = { test1: testTask1, test2: testTask2 }
    const executorClient = new DurableExecutorClient(storage, tasks, {
      logLevel: 'error',
    })

    const handle1 = await executorClient.enqueueTask('test1')
    const execution1 = await handle1.getExecution()

    await expect(async () => {
      await executorClient.getTaskExecutionHandle('test2', execution1.executionId)
    }).rejects.toThrow(DurableExecutionNotFoundError)
  })

  it('should handle shutdown idempotently', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    const tasks = { test: testTask }
    const executorClient = new DurableExecutorClient(storage, tasks, {
      logLevel: 'error',
    })

    await expect(executorClient.shutdown()).resolves.not.toThrow()

    await expect(executorClient.shutdown()).resolves.not.toThrow()
  })

  it('should throw after shutdown', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    const tasks = { test: testTask }
    const executorClient = new DurableExecutorClient(storage, tasks, {
      logLevel: 'error',
    })

    await executorClient.shutdown()

    await expect(async () => {
      await executorClient.enqueueTask('test')
    }).rejects.toThrow(DurableExecutionError)

    await expect(async () => {
      await executorClient.getTaskExecutionHandle('test', 'some-id')
    }).rejects.toThrow(DurableExecutionError)
  })
})
