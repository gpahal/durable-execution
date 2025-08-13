import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { sleep } from '@gpahal/std/promises'

import {
  DurableExecutionError,
  DurableExecutionNotFoundError,
  DurableExecutor,
  type TaskRunContext,
} from '../src'
import { InMemoryStorage } from './in-memory-storage'

describe('executor', () => {
  let storage: InMemoryStorage
  let executor: DurableExecutor

  beforeEach(() => {
    storage = new InMemoryStorage({ enableDebug: false })
    executor = new DurableExecutor(storage, {
      enableDebug: false,
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
    let executed = 0
    const taskOptions = {
      id: 'test',
      timeoutMs: 10_000,
      run: async (ctx: TaskRunContext) => {
        executed++
        await sleep(1000)
        if (ctx.shutdownSignal.isCancelled()) {
          return 'test cancelled'
        }
        executed++
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
    expect(executed).toBe(1)
    await executor.shutdown()

    execution = await handle.getExecution()
    expect(executed).toBe(1)
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
    const taskOptions = {
      id: 'test',
      timeoutMs: 10_000,
      run: async () => {
        await sleep(1000)
        return 'test'
      },
    } as const
    const task = executor.task(taskOptions)

    await executor.shutdown()
    executor = new DurableExecutor(storage, {
      enableDebug: false,
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
        // @ts-expect-error - Testing invalid input
        maxSerializedInputDataSize: 'invalid',
      })
    }).toThrow('Invalid options')
  })

  it('should disable debug logging when enableDebug is false', () => {
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
      enableDebug: false,
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
      enableDebug: true,
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
})
