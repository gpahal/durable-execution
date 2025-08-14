import { sleep } from '@gpahal/std/promises'

import {
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

  it('should handle executor shutdown with running task', { timeout: 10_000 }, async () => {
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

  it('should handle unknown task', { timeout: 10_000 }, async () => {
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
      await executor.getTaskHandle('unknown', 'some-execution-id')
    }).rejects.toThrow(DurableExecutionNotFoundError)
  })

  it('should throw when getting handle for non-existent execution', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    await expect(async () => {
      await executor.getTaskHandle(testTask, 'non-existent-execution-id')
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
      await executor.getTaskHandle(testTask2, execution1.executionId)
    }).rejects.toThrow(DurableExecutionNotFoundError)
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
      await executor.getTaskHandle(testTask, 'some-id')
    }).rejects.toThrow(DurableExecutionError)
  })

  it('should handle storage insert failures during task enqueueing', async () => {
    const failingStorage = new InMemoryTaskExecutionsStorage()
    const originalInsert = failingStorage.insert.bind(failingStorage)
    let insertCallCount = 0
    failingStorage.insert = () => {
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

    failingStorage.insert = originalInsert
    const handle = await failingExecutor.enqueueTask(testTask)
    const execution = await handle.waitAndGetFinishedExecution()
    expect(execution.status).toBe('completed')

    await failingExecutor.shutdown()
  })

  it('should handle storage updateById failures during status transitions', async () => {
    const failingStorage = new InMemoryTaskExecutionsStorage()
    const originalUpdateById = failingStorage.updateById.bind(failingStorage)
    let updateByIdCallCount = 0
    let shouldFail = false

    failingStorage.updateById = async (...args) => {
      updateByIdCallCount++
      if (shouldFail && updateByIdCallCount > 2) {
        throw new Error('Storage updateById failed')
      }
      return originalUpdateById(...args)
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

  it('should handle storage getById failures during task retrieval', async () => {
    const failingStorage = new InMemoryTaskExecutionsStorage()
    const originalGetById = failingStorage.getById.bind(failingStorage)
    let shouldFail = false

    failingStorage.getById = async (...args) => {
      if (shouldFail) {
        throw new Error('Storage getById failed')
      }
      return originalGetById(...args)
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
    }).rejects.toThrow('Storage getById failed')

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
    const execution = await handle.waitAndGetFinishedExecution()

    expect(execution.status).toBe('completed')
    expect(executionCount).toBe(1)

    await executor1.shutdown()
    await executor2.shutdown()
  })

  it('should handle storage failures within retry attempts during atomic updateByIdAndInsertIfUpdated operations', async () => {
    const failingStorage = new InMemoryTaskExecutionsStorage()
    const originalMethod = failingStorage.updateByIdAndInsertIfUpdated.bind(failingStorage)
    let failureCount = 0

    failingStorage.updateByIdAndInsertIfUpdated = async (...args) => {
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
          children: [{ task: childTask }],
        }
      },
    })

    const handle = await failingExecutor.enqueueTask(parentTask)

    await sleep(500)

    const execution = await handle.waitAndGetFinishedExecution()
    expect(execution.status).toBe('completed')
    expect(failureCount).toBeGreaterThan(0)

    await failingExecutor.shutdown()
  })

  it('should handle storage failures more than retry attempts during atomic updateByIdAndInsertIfUpdated operations', async () => {
    const failingStorage = new InMemoryTaskExecutionsStorage()
    const originalMethod = failingStorage.updateByIdAndInsertIfUpdated.bind(failingStorage)
    let failureCount = 0

    failingStorage.updateByIdAndInsertIfUpdated = async (...args) => {
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
          children: [{ task: childTask }],
        }
      },
    })

    const handle = await failingExecutor.enqueueTask(parentTask)

    await sleep(500)

    const execution = await handle.waitAndGetFinishedExecution()
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

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error?.message).toContain('Task test not found')
  })

  it('should handle non-existent parent task in executor', async () => {
    const originalFn =
      storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn
    storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn =
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
            children: [{ task: childTask }],
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
      storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn =
        originalFn

      const execution = await handle.getExecution()
      expect(execution.status).toBe('waiting_for_children')
      assert(execution.status === 'waiting_for_children')
      expect(execution.children).toHaveLength(1)
      expect(execution.children[0]!.taskId).toBe('child')
      expect(execution.children[0]!.executionId).toMatch(/^te_/)
      executor.startBackgroundProcesses()

      const finishedExecution = await handle.waitAndGetFinishedExecution()
      expect(finishedExecution.status).toBe('failed')
      assert(finishedExecution.status === 'failed')
      expect(finishedExecution.error?.message).toContain('Task parent not found')
    } finally {
      storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn =
        originalFn
    }
  })

  it('should handle missing children task executions in storage', async () => {
    const originalFn =
      storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn
    storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn =
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
            children: [{ task: childTask }],
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
      storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn =
        originalFn

      const execution = await handle.getExecution()
      expect(execution.status).toBe('waiting_for_children')
      assert(execution.status === 'waiting_for_children')
      expect(execution.children).toHaveLength(1)
      expect(execution.children[0]!.taskId).toBe('child')
      expect(execution.children[0]!.executionId).toMatch(/^te_/)

      const childTaskExecutionId = execution.children[0]!.executionId
      await storage.deleteById(childTaskExecutionId)
      executor.startBackgroundProcesses()

      const finishedExecution = await handle.waitAndGetFinishedExecution()
      expect(finishedExecution.status).toBe('failed')
      assert(finishedExecution.status === 'failed')
      expect(finishedExecution.error?.message).toContain('Some children task executions not found')
    } finally {
      storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn =
        originalFn
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

  it('should increase sleep time on empty results', async () => {
    const startTimes: Array<number> = []
    const promise = executor.testRunBackgroundProcess('test', () => {
      startTimes.push(Date.now())
      return Promise.resolve(false)
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
    expect(sleepTimes.every((sleepTime) => sleepTime > 20)).toBe(true)

    await expect(promise).resolves.not.toThrow()
  })
})
