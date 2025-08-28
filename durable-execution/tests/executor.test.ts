import { sleep } from '@gpahal/std/promises'

import {
  ChildTask,
  DurableExecutionCancelledError,
  DurableExecutionError,
  DurableExecutionNotFoundError,
  DurableExecutor,
  InMemoryTaskExecutionsStorage,
  type ParentTaskOptions,
  type TaskOptions,
  type TaskRunContext,
} from '../src'

describe('executor', () => {
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

  it('should throw with invalid durable executor options', () => {
    expect(
      () =>
        new DurableExecutor(storage, {
          logLevel: 'error',
          // @ts-expect-error - Testing invalid input
          maxSerializedInputDataSize: 'invalid',
        }),
    ).toThrow('Invalid options')
  })

  it('should throw if shutdown', async () => {
    await executor.shutdown()
    expect(() =>
      executor.task({
        id: 'test',
        timeoutMs: 1000,
        run: async () => {
          // Do nothing
        },
      }),
    ).toThrow('Durable executor shutdown')
  })

  it('should handle executor shutdown with running task', async () => {
    let executionCount = 0
    const taskOptions = {
      id: 'test',
      timeoutMs: 10_000,
      run: async (ctx: TaskRunContext) => {
        executionCount++
        await sleep(1000)
        if (ctx.shutdownSignal.isCancelled()) {
          return 'test cancelled'
        }
        executionCount++
        return 'test'
      },
    } as const
    const task = executor.task(taskOptions)

    const handle = await executor.enqueueTask(task)
    let execution = await handle.getExecution()
    expect(['ready', 'running']).toContain(execution.status)

    await sleep(250)

    execution = await handle.getExecution()
    expect(execution.status).toBe('running')
    expect(executionCount).toBe(1)
    await executor.shutdown()

    execution = await handle.getExecution()
    expect(executionCount).toBe(1)
    expect(execution.status).toBe('completed')
    expect(execution.status).toBe('completed')
    assert(execution.status === 'completed')
    expect(execution.taskId).toBe('test')
    expect(execution.executionId).toMatch(/^te_/)
    expect(execution.output).toBe('test cancelled')
    expect(execution.startedAt).toBeInstanceOf(Date)
    expect(execution.finishedAt).toBeInstanceOf(Date)
    expect(execution.finishedAt.getTime()).toBeGreaterThanOrEqual(execution.startedAt.getTime())
  })

  it('should handle unknown task', async () => {
    const task = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: async () => {
        await sleep(1000)
        return 'test'
      },
    })

    await executor.shutdown()
    executor = new DurableExecutor(storage, {
      logLevel: 'error',
    })
    executor.startBackgroundProcesses()

    await expect(executor.enqueueTask(task)).rejects.toThrow('Task test not found')
  })

  it('should handle duplicate task ids', () => {
    executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: () => {
        return 'test'
      },
    })

    expect(() =>
      executor.task({
        id: 'test',
        timeoutMs: 1000,
        run: () => {
          return 'test'
        },
      }),
    ).toThrow('Task test already exists')
  })

  it('should handle invalid options', () => {
    expect(() => {
      new DurableExecutor(storage, {
        logLevel: 'error',
        // @ts-expect-error - Testing invalid input
        maxSerializedInputDataSize: 'invalid',
      })
    }).toThrow('Invalid options')
  })

  it('should disable debug logging when logLevel is info', () => {
    let executionCount = 0
    const logger = {
      debug: () => {
        executionCount++
      },
      info: () => {
        // Do nothing
      },
      error: () => {
        // Do nothing
      },
    }

    let executor = new DurableExecutor(storage, {
      logger,
      logLevel: 'info',
    })
    executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: () => {
        return 'test'
      },
    })

    expect(executor).toBeDefined()
    expect(executionCount).toBe(0)

    executor = new DurableExecutor(storage, {
      logger,
      logLevel: 'debug',
    })
    executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: () => {
        return 'test'
      },
    })

    expect(executor).toBeDefined()
    expect(executionCount).toBe(1)
  })

  it('should throw when enqueuing unknown task', async () => {
    await expect(async () => {
      // @ts-expect-error - Testing invalid input
      await executor.enqueueTask({ id: 'unknown' })
    }).rejects.toThrow(DurableExecutionNotFoundError)
  })

  it('should throw when getting handle for unknown task', async () => {
    await expect(async () => {
      // @ts-expect-error - Testing invalid input
      await executor.getTaskExecutionHandle('unknown', 'some-execution-id')
    }).rejects.toThrow(DurableExecutionNotFoundError)
  })

  it('should throw when getting handle for non-existent execution', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    await expect(async () => {
      await executor.getTaskExecutionHandle(testTask, 'non-existent-execution-id')
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

    const handle1 = await executor.enqueueTask(testTask1)
    const execution1 = await handle1.getExecution()

    await expect(async () => {
      await executor.getTaskExecutionHandle(testTask2, execution1.executionId)
    }).rejects.toThrow(DurableExecutionNotFoundError)
  })

  it('should throw when getting execution for non-existent execution', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    const handle = await executor.enqueueTask(testTask)
    await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })

    await storage.deleteById({ executionId: handle.getExecutionId() })

    await expect(handle.getExecution()).rejects.toThrow(DurableExecutionNotFoundError)
    await expect(
      handle.waitAndGetFinishedExecution({
        pollingIntervalMs: 100,
      }),
    ).rejects.toThrow(DurableExecutionNotFoundError)
  })

  it('should handle shutdown idempotently', async () => {
    await expect(executor.shutdown()).resolves.not.toThrow()
    await expect(executor.shutdown()).resolves.not.toThrow()
  })

  it('should throw after shutdown', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    await executor.shutdown()

    await expect(async () => {
      await executor.enqueueTask(testTask)
    }).rejects.toThrow(DurableExecutionError)

    await expect(async () => {
      await executor.getTaskExecutionHandle(testTask, 'some-id')
    }).rejects.toThrow(DurableExecutionError)
  })

  it('should handle storage insert failures during task enqueueing', async () => {
    const failingStorage = new InMemoryTaskExecutionsStorage()
    const originalInsert = failingStorage.insertMany.bind(failingStorage)
    let insertCallCount = 0
    failingStorage.insertMany = () => {
      insertCallCount++
      throw new Error('Storage insert failed')
    }

    const failingExecutor = new DurableExecutor(failingStorage, {
      logLevel: 'error',
    })
    failingExecutor.startBackgroundProcesses()

    const testTask = failingExecutor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    await expect(async () => {
      await failingExecutor.enqueueTask(testTask)
    }).rejects.toThrow('Storage insert failed')

    expect(insertCallCount).toBeGreaterThan(0)

    failingStorage.insertMany = originalInsert
    const handle = await failingExecutor.enqueueTask(testTask)
    const execution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(execution.status).toBe('completed')

    await failingExecutor.shutdown()
  })

  it('should handle storage updateManyById failures during status transitions', async () => {
    const failingStorage = new InMemoryTaskExecutionsStorage()
    const originalUpdateManyById = failingStorage.updateManyById.bind(failingStorage)
    let updateManyByIdCallCount = 0
    let shouldFail = false

    failingStorage.updateManyById = async (...args) => {
      updateManyByIdCallCount++
      if (shouldFail && updateManyByIdCallCount > 2) {
        throw new Error('Storage updateManyById failed')
      }
      return originalUpdateManyById(...args)
    }

    const failingExecutor = new DurableExecutor(failingStorage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    failingExecutor.startBackgroundProcesses()

    const testTask = failingExecutor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: async () => {
        await sleep(100)
        return 'test'
      },
    })

    shouldFail = true
    const handle = await failingExecutor.enqueueTask(testTask)

    shouldFail = false
    await sleep(500)

    const execution = await handle.getExecution()
    expect(['ready', 'running', 'completed']).toContain(execution.status)

    await failingExecutor.shutdown()
  })

  it('should handle storage getManyById failures during task retrieval', async () => {
    const failingStorage = new InMemoryTaskExecutionsStorage()
    const originalGetManyById = failingStorage.getManyById.bind(failingStorage)
    let shouldFail = false

    failingStorage.getManyById = async (...args) => {
      if (shouldFail) {
        throw new Error('Storage getManyById failed')
      }
      return originalGetManyById(...args)
    }

    const failingExecutor = new DurableExecutor(failingStorage, {
      logLevel: 'error',
    })
    failingExecutor.startBackgroundProcesses()

    const testTask = failingExecutor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    const handle = await failingExecutor.enqueueTask(testTask)

    shouldFail = true
    await expect(async () => {
      await handle.getExecution()
    }).rejects.toThrow('Storage getManyById failed')

    shouldFail = false
    const execution = await handle.getExecution()
    expect(execution).toBeDefined()

    await failingExecutor.shutdown()
  })

  it('should handle race condition with duplicate task execution pickup', async () => {
    const testStorage = new InMemoryTaskExecutionsStorage()

    const executor1 = new DurableExecutor(testStorage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    const executor2 = new DurableExecutor(testStorage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })

    executor1.startBackgroundProcesses()
    executor2.startBackgroundProcesses()

    let executionCount = 0
    const testTask = executor1.task({
      id: 'test',
      timeoutMs: 10_000,
      run: async () => {
        executionCount++
        await sleep(100)
        return 'test'
      },
    })

    executor2.task({
      id: 'test',
      timeoutMs: 10_000,
      run: async () => {
        executionCount++
        await sleep(100)
        return 'test'
      },
    })

    const handle = await executor1.enqueueTask(testTask)
    const execution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })

    expect(execution.status).toBe('completed')
    expect(executionCount).toBe(1)

    await executor1.shutdown()
    await executor2.shutdown()
  })

  it('should handle storage failures within retry attempts during atomic updateManyByIdAndInsertManyIfUpdated operations', async () => {
    const failingStorage = new InMemoryTaskExecutionsStorage()
    const originalMethod =
      failingStorage.updateManyByIdAndInsertChildrenIfUpdated.bind(failingStorage)
    let failureCount = 0

    failingStorage.updateManyByIdAndInsertChildrenIfUpdated = async (...args) => {
      if (failureCount < 1) {
        failureCount++
        throw new Error('Atomic operation failed')
      }
      return originalMethod(...args)
    }

    const failingExecutor = new DurableExecutor(failingStorage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    failingExecutor.startBackgroundProcesses()

    const childTask = failingExecutor.task({
      id: 'child',
      timeoutMs: 5000,
      run: () => 'child result',
    })

    const parentTask = failingExecutor.parentTask({
      id: 'parent',
      timeoutMs: 10_000,
      runParent: () => {
        return {
          output: undefined,
          children: [new ChildTask(childTask)],
        }
      },
    })

    const handle = await failingExecutor.enqueueTask(parentTask)

    await sleep(500)

    const execution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(execution.status).toBe('completed')
    expect(failureCount).toBeGreaterThan(0)

    await failingExecutor.shutdown()
  })

  it('should handle storage failures more than retry attempts during atomic updateManyByIdAndInsertManyIfUpdated operations', async () => {
    const failingStorage = new InMemoryTaskExecutionsStorage()
    const originalMethod =
      failingStorage.updateManyByIdAndInsertChildrenIfUpdated.bind(failingStorage)
    let failureCount = 0

    failingStorage.updateManyByIdAndInsertChildrenIfUpdated = async (...args) => {
      if (failureCount < 2) {
        failureCount++
        throw new Error('Atomic operation failed')
      }
      return originalMethod(...args)
    }

    const failingExecutor = new DurableExecutor(failingStorage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    failingExecutor.startBackgroundProcesses()

    const childTask = failingExecutor.task({
      id: 'child',
      timeoutMs: 5000,
      run: () => 'child result',
    })

    const parentTask = failingExecutor.parentTask({
      id: 'parent',
      timeoutMs: 10_000,
      runParent: () => {
        return {
          output: undefined,
          children: [new ChildTask(childTask)],
        }
      },
    })

    const handle = await failingExecutor.enqueueTask(parentTask)

    await sleep(500)

    const execution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(execution.status).toBe('failed')
    expect(failureCount).toBeGreaterThan(0)

    await failingExecutor.shutdown()
  })

  it('should handle non-existent task in executor', async () => {
    await executor.shutdown()
    executor = new DurableExecutor(storage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })

    const task = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    const handle = await executor.enqueueTask(task)

    await executor.shutdown()
    executor = new DurableExecutor(storage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    executor.startBackgroundProcesses()

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error?.message).toContain('Task test not found')
  })

  it('should handle non-existent parent task in executor', async () => {
    const originalFn =
      storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn
    storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn =
      () => {
        return Promise.resolve([])
      }

    try {
      const childTask = executor.task({
        id: 'child',
        timeoutMs: 10_000,
        run: () => 'child',
      })

      const parentTask = executor.parentTask({
        id: 'parent',
        timeoutMs: 10_000,
        runParent: () => {
          return {
            output: 'parent',
            children: [new ChildTask(childTask)],
          }
        },
        finalize: {
          id: 'finalize',
          timeoutMs: 60_000,
          run: (ctx, { output }) => {
            return output
          },
        },
      })

      const handle = await executor.enqueueTask(parentTask)
      await sleep(500)

      await executor.shutdown()
      executor = new DurableExecutor(storage, {
        logLevel: 'error',
        backgroundProcessIntraBatchSleepMs: 50,
      })
      storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn =
        originalFn

      const execution = await handle.getExecution()
      expect(execution.status).toBe('waiting_for_children')
      assert(execution.status === 'waiting_for_children')
      expect(execution.children).toHaveLength(1)
      expect(execution.children[0]!.taskId).toBe('child')
      expect(execution.children[0]!.executionId).toMatch(/^te_/)
      executor.startBackgroundProcesses()

      const finishedExecution = await handle.waitAndGetFinishedExecution({
        pollingIntervalMs: 100,
      })
      expect(finishedExecution.status).toBe('failed')
      assert(finishedExecution.status === 'failed')
      expect(finishedExecution.error?.message).toContain('Task parent not found')
    } finally {
      storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn =
        originalFn
    }
  })

  it('should handle missing children task executions in storage', async () => {
    const originalFn =
      storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn
    storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn =
      () => {
        return Promise.resolve([])
      }

    try {
      const childTaskOptions: TaskOptions<undefined, string> = {
        id: 'child',
        timeoutMs: 10_000,
        run: () => 'child',
      }
      const childTask = executor.task(childTaskOptions)

      const parentTaskOptions: ParentTaskOptions<undefined, string, string, unknown> = {
        id: 'parent',
        timeoutMs: 10_000,
        runParent: () => {
          return {
            output: 'parent',
            children: [new ChildTask(childTask)],
          }
        },
        finalize: {
          id: 'finalize',
          timeoutMs: 60_000,
          run: (ctx, { output }) => {
            return output
          },
        },
      }
      const parentTask = executor.parentTask(parentTaskOptions)

      const handle = await executor.enqueueTask(parentTask)
      await sleep(500)

      await executor.shutdown()
      executor = new DurableExecutor(storage, {
        logLevel: 'error',
        backgroundProcessIntraBatchSleepMs: 50,
      })
      executor.task(childTaskOptions)
      executor.parentTask(parentTaskOptions)
      storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn =
        originalFn

      const execution = await handle.getExecution()
      expect(execution.status).toBe('waiting_for_children')
      assert(execution.status === 'waiting_for_children')
      expect(execution.children).toHaveLength(1)
      expect(execution.children[0]!.taskId).toBe('child')
      expect(execution.children[0]!.executionId).toMatch(/^te_/)

      const childTaskExecutionId = execution.children[0]!.executionId
      await storage.deleteById({ executionId: childTaskExecutionId })
      executor.startBackgroundProcesses()

      const finishedExecution = await handle.waitAndGetFinishedExecution({
        pollingIntervalMs: 100,
      })
      expect(finishedExecution.status).toBe('failed')
      assert(finishedExecution.status === 'failed')
      expect(finishedExecution.error?.message).toContain('Some children task executions not found')
    } finally {
      storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn =
        originalFn
    }
  })

  it('should return storage timing stats', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test result',
    })

    await executor.enqueueTask(testTask)
    await sleep(500)

    const stats = executor.getStorageTimingStats()
    expect(stats).toBeDefined()
    expect(typeof stats).toBe('object')

    const statKeys = Object.keys(stats)
    expect(statKeys.length).toBeGreaterThan(0)

    for (const key of statKeys) {
      expect(stats[key]).toBeDefined()
      expect(typeof stats[key]!.count).toBe('number')
      expect(typeof stats[key]!.meanMs).toBe('number')
      expect(stats[key]!.count).toBeGreaterThan(0)
      expect(stats[key]!.meanMs).toBeGreaterThanOrEqual(0)
    }
  })

  it('should return storage per second call counts', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test result',
    })

    await executor.enqueueTask(testTask)
    await sleep(500)

    const callCounts = executor.getStoragePerSecondCallCounts()
    expect(callCounts).toBeDefined()
    expect(callCounts instanceof Map).toBe(true)

    expect(callCounts.size).toBeGreaterThan(0)

    for (const [timestamp, count] of callCounts.entries()) {
      expect(typeof timestamp).toBe('number')
      expect(typeof count).toBe('number')
      expect(count).toBeGreaterThan(0)
    }
  })
})

class TestDurableExecutor extends DurableExecutor {
  testRunBackgroundProcess(
    processName: string,
    singleBatchProcessFn: () => Promise<boolean>,
    sleepMultiplier?: number,
  ): Promise<void> {
    return this.runBackgroundProcess(processName, singleBatchProcessFn, sleepMultiplier)
  }
}

describe('runBackgroundProcess', () => {
  let storage: InMemoryTaskExecutionsStorage
  let executor: TestDurableExecutor

  beforeEach(() => {
    storage = new InMemoryTaskExecutionsStorage()
    executor = new TestDurableExecutor(storage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
  })

  afterEach(async () => {
    if (executor) {
      await executor.shutdown()
    }
  })

  it('should handle shutdown signal', async () => {
    const promise = executor.testRunBackgroundProcess('test', () => {
      return Promise.resolve(false)
    })

    await sleep(500)
    await executor.shutdown()

    await expect(promise).resolves.not.toThrow()
  })

  it('should sleep on empty results', async () => {
    const startTimes: Array<number> = []
    const promise = executor.testRunBackgroundProcess('test', () => {
      startTimes.push(Date.now())
      return Promise.resolve(true)
    })

    await sleep(500)
    await executor.shutdown()

    expect(startTimes.length).toBeGreaterThan(0)
    const sleepTimes = startTimes
      .map((startTime, index) => {
        return index === 0 ? 0 : startTime - startTimes[index - 1]!
      })
      .slice(1)
    expect(sleepTimes).toHaveLength(startTimes.length - 1)
    expect(sleepTimes.every((sleepTime) => sleepTime > 10)).toBe(true)

    await expect(promise).resolves.not.toThrow()
  })

  it('should handle consecutive errors and cancellation in background process', async () => {
    let errorCount = 0
    let wasShutdown = false

    const promise = executor.testRunBackgroundProcess('test', () => {
      if (wasShutdown) {
        throw new DurableExecutionCancelledError('Shutdown requested')
      }

      errorCount++
      if (errorCount <= 50) {
        throw new Error(`Consecutive error ${errorCount}`)
      }

      return Promise.resolve(true)
    })

    await sleep(1500)
    const shutdownPromise = executor.shutdown()
    await sleep(100)
    wasShutdown = true
    await shutdownPromise

    await expect(promise).resolves.not.toThrow()
    expect(errorCount).toBeGreaterThan(10)
  })

  it('should handle background process errors without shutdown', async () => {
    let errorCount = 0

    const promise = executor.testRunBackgroundProcess('test', () => {
      errorCount++
      if (errorCount <= 3) {
        throw new Error(`Background process error ${errorCount}`)
      }
      return Promise.resolve(true)
    })

    await sleep(400)
    await executor.shutdown()

    await expect(promise).resolves.not.toThrow()
  })

  it('should apply exponential backoff when consecutive errors exceed threshold', async () => {
    let errorCount = 0

    const promise = executor.testRunBackgroundProcess('test', async () => {
      errorCount++
      await sleep(1)
      if (errorCount <= 10) {
        throw new Error(`Consecutive error ${errorCount}`)
      }
      return true
    })

    await sleep(500)
    await executor.shutdown()

    await expect(promise).resolves.not.toThrow()
    expect(errorCount).toBeGreaterThan(5)
  })

  it('should catch and log errors in background process loop', async () => {
    let errorLogged = false

    const mockLogger = {
      debug: () => {
        // Do nothing
      },
      info: () => {
        // Do nothing
      },
      error: (message: string) => {
        if (message.includes('Error in test')) {
          errorLogged = true
        }
      },
    }

    const testExecutor = new TestDurableExecutor(storage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
      logger: mockLogger,
    })

    let callCount = 0
    const promise = testExecutor.testRunBackgroundProcess('test', () => {
      callCount++
      if (callCount === 1) {
        throw new Error('Simulated error in background process loop')
      }
      return Promise.resolve(true)
    })

    await sleep(200)
    await testExecutor.shutdown()

    await expect(promise).resolves.not.toThrow()
    expect(errorLogged).toBe(true)
  })
})
