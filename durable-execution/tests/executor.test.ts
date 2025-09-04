import { sleep } from '@gpahal/std/promises'

import {
  childTask,
  DurableExecutor,
  InMemoryTaskExecutionsStorage,
  type ParentTaskOptions,
  type TaskOptions,
  type TaskRunContext,
} from '../src'

describe('executor', () => {
  let storage: InMemoryTaskExecutionsStorage
  let executor: DurableExecutor

  beforeEach(async () => {
    storage = new InMemoryTaskExecutionsStorage()
    executor = await DurableExecutor.make(storage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
      enableStorageStats: true,
    })
    await executor.start()
  })

  afterEach(async () => {
    if (executor) {
      await executor.shutdown()
    }
  })

  it('should throw with invalid durable executor options', async () => {
    await expect(
      DurableExecutor.make(storage, {
        logLevel: 'error',
        // @ts-expect-error - Testing invalid input
        maxSerializedInputDataSize: 'invalid',
      }),
    ).rejects.toThrow('Invalid options')
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
        if (ctx.shutdownSignal.aborted) {
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

    execution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
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

  it('should handle custom serializer', async () => {
    await executor.shutdown()

    executor = await DurableExecutor.make(storage, {
      logLevel: 'error',
      serializer: {
        serialize: <T>(_: T) => {
          return 'test'
        },
        deserialize: <T>(value: string) => {
          return value as T
        },
      },
    })
    await executor.start()

    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: () => {
        return 'random'
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.output).toBe('test')
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
    executor = await DurableExecutor.make(storage, {
      logLevel: 'error',
    })
    await executor.start()

    await expect(executor.enqueueTask(task)).rejects.toThrow('Task not found [taskId=test]')
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
    ).toThrow('Task with given id already exists [taskId=test]')
  })

  it('should handle invalid options', async () => {
    await expect(
      DurableExecutor.make(storage, {
        logLevel: 'error',
        // @ts-expect-error - Testing invalid input
        maxSerializedInputDataSize: 'invalid',
      }),
    ).rejects.toThrow('Invalid options')
  })

  it('should throw when enqueuing unknown task', async () => {
    await expect(async () => {
      // @ts-expect-error - Testing invalid input
      await executor.enqueueTask({ id: 'unknown' })
    }).rejects.toThrow('not found')
  })

  it('should throw when getting handle for unknown task', async () => {
    await expect(async () => {
      // @ts-expect-error - Testing invalid input
      await executor.getTaskExecutionHandle('unknown', 'some-execution-id')
    }).rejects.toThrow('not found')
  })

  it('should throw when getting handle for non-existent execution', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    await expect(async () => {
      await executor.getTaskExecutionHandle(testTask, 'non-existent-execution-id')
    }).rejects.toThrow('not found')
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
    }).rejects.toThrow('belongs to another task')
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

    await storage.deleteById({ executionId: handle.executionId })

    await expect(handle.getExecution()).rejects.toThrow('not found')
    await expect(
      handle.waitAndGetFinishedExecution({
        pollingIntervalMs: 100,
      }),
    ).rejects.toThrow('not found')
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
    }).rejects.toThrow('Durable executor shutdown')

    await expect(async () => {
      await executor.getTaskExecutionHandle(testTask, 'some-id')
    }).rejects.toThrow('Durable executor shutdown')
  })

  it('should handle storage insert failures during task enqueueing', async () => {
    const failingStorage = new InMemoryTaskExecutionsStorage()
    const originalInsert = failingStorage.insertMany.bind(failingStorage)
    let insertCallCount = 0
    failingStorage.insertMany = () => {
      insertCallCount++
      throw new Error('Storage insert failed')
    }

    const failingExecutor = await DurableExecutor.make(failingStorage, {
      logLevel: 'error',
    })
    await failingExecutor.start()

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

    const failingExecutor = await DurableExecutor.make(failingStorage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    await failingExecutor.start()

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

    const failingExecutor = await DurableExecutor.make(failingStorage, {
      logLevel: 'error',
    })
    await failingExecutor.start()

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

    const executor1 = await DurableExecutor.make(testStorage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    const executor2 = await DurableExecutor.make(testStorage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })

    await executor1.start()
    await executor2.start()

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

    const failingExecutor = await DurableExecutor.make(failingStorage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    await failingExecutor.start()

    const child = failingExecutor.task({
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
          children: [childTask(child)],
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

    const failingExecutor = await DurableExecutor.make(failingStorage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    await failingExecutor.start()

    const child = failingExecutor.task({
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
          children: [childTask(child)],
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
    executor = await DurableExecutor.make(storage, {
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
    executor = await DurableExecutor.make(storage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    await executor.start()

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error?.message).toContain('Task not found [taskId=test]')
  })

  it('should handle non-existent parent task in executor', async () => {
    const originalFn =
      storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn
    storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn =
      () => {
        return Promise.resolve([])
      }

    try {
      const child = executor.task({
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
            children: [childTask(child)],
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
      executor = await DurableExecutor.make(storage, {
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
      await executor.start()

      const finishedExecution = await handle.waitAndGetFinishedExecution({
        pollingIntervalMs: 100,
      })
      expect(finishedExecution.status).toBe('failed')
      assert(finishedExecution.status === 'failed')
      expect(finishedExecution.error?.message).toContain('Task not found [taskId=parent]')
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
      const child = executor.task(childTaskOptions)

      const parentTaskOptions: ParentTaskOptions<undefined, string, string, unknown> = {
        id: 'parent',
        timeoutMs: 10_000,
        runParent: () => {
          return {
            output: 'parent',
            children: [childTask(child)],
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
      executor = await DurableExecutor.make(storage, {
        logLevel: 'error',
        backgroundProcessIntraBatchSleepMs: 50,
      })
      await executor.start()
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
      await executor.start()

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
})
