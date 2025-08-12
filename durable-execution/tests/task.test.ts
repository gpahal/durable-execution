import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { z } from 'zod'

import { sleep } from '@gpahal/std/promises'

import {
  DurableExecutionCancelledError,
  DurableExecutionError,
  DurableExecutor,
  type TaskRunContext,
} from '../src'
import { InMemoryStorage } from './in-memory-storage'

describe('simpleTask', () => {
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
    await executor.shutdown()
  })

  it('should complete', async () => {
    let executed = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: () => {
        executed++
        return 'test'
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(1)
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

  it('should complete async', async () => {
    let executed = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: async () => {
        executed++
        await sleep(1)
        return 'test'
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(1)
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

  it('should complete with valid input', async () => {
    let executed = 0
    const task = executor
      .validateInput((input: string) => {
        if (input !== 'test') {
          throw new Error('Invalid input')
        }
        return input
      })
      .task({
        id: 'test',
        timeoutMs: 1000,
        run: (_, input) => {
          executed++
          return input
        },
      })

    const handle = await executor.enqueueTask(task, 'test')

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(1)
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

  it('should fail with invalid input', async () => {
    let executed = 0
    const task = executor
      .validateInput((input: string) => {
        if (input !== 'test') {
          throw new Error('Invalid input')
        }
        return input
      })
      .task({
        id: 'test',
        timeoutMs: 1000,
        run: (_, input) => {
          executed++
          return input
        },
      })

    await expect(executor.enqueueTask(task, 'invalid')).rejects.toThrow('Invalid input')
    expect(executed).toBe(0)
  })

  it('should complete with input schema', async () => {
    let executed = 0
    const task = executor
      .inputSchema(
        z.object({
          name: z.string(),
        }),
      )
      .task({
        id: 'test',
        timeoutMs: 1000,
        run: async (_, input) => {
          executed++
          await sleep(1)
          return input.name
        },
      })

    const handle = await executor.enqueueTask(task, { name: 'test' })

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(1)
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

  it('should fail with input schema', async () => {
    let executed = 0
    const task = executor
      .inputSchema(
        z.object({
          name: z.string(),
        }),
      )
      .task({
        id: 'test',
        timeoutMs: 1000,
        run: async (_, input) => {
          executed++
          await sleep(1)
          return input.name
        },
      })

    // @ts-expect-error - Testing invalid input
    await expect(executor.enqueueTask(task, { name: 0 })).rejects.toThrow(
      'Invalid input to task test',
    )
    expect(executed).toBe(0)
  })

  it('should fail', async () => {
    let executed = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: () => {
        executed++
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error')
    expect(finishedExecution.error?.errorType).toBe('generic')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete after retry', async () => {
    let executed = 0
    const task = executor.task({
      id: 'test',
      retryOptions: {
        maxAttempts: 1,
      },
      timeoutMs: 1000,
      run: async (ctx) => {
        executed++
        if (ctx.attempt === 0) {
          throw new Error('Test error')
        }
        await sleep(1)
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(2)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail after retry', async () => {
    let executed = 0
    const task = executor.task({
      id: 'test',
      retryOptions: {
        maxAttempts: 1,
      },
      timeoutMs: 1000,
      run: (ctx) => {
        executed++
        throw new Error(`Test error ${ctx.attempt}`)
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(2)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error 1')
    expect(finishedExecution.error?.errorType).toBe('generic')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail after non-retryable error', async () => {
    let executed = 0
    const task = executor.task({
      id: 'test',
      retryOptions: {
        maxAttempts: 1,
      },
      timeoutMs: 1000,
      run: () => {
        executed++
        throw new DurableExecutionError('Test error', false)
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error')
    expect(finishedExecution.error?.errorType).toBe('generic')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should timeout', async () => {
    let executed = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 10,
      run: async () => {
        executed++
        await sleep(100)
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('timed_out')
    assert(finishedExecution.status === 'timed_out')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Task timed out')
    expect(finishedExecution.error?.errorType).toBe('timed_out')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with enqueue timeout', async () => {
    let executed = 0
    const task = executor.task({
      id: 'test',
      retryOptions: {
        maxAttempts: 1,
      },
      timeoutMs: 10,
      run: async () => {
        executed++
        await sleep(50)
      },
    })

    const handle = await executor.enqueueTask(task, undefined, {
      timeoutMs: 1000,
    })

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should handle invalid timeout', () => {
    expect(() =>
      executor.task({
        id: 'test',
        timeoutMs: -1,
        run: async () => {
          await sleep(100)
        },
      }),
    ).toThrow('Invalid timeout value for task test')
  })

  it('should cancel immediately', async () => {
    let executed = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: async () => {
        executed++
        await sleep(100)
      },
    })

    const handle = await executor.enqueueTask(task)
    await handle.cancel()

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(0)
    expect(finishedExecution.status).toBe('cancelled')
    assert(finishedExecution.status === 'cancelled')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Task cancelled')
    expect(finishedExecution.error?.errorType).toBe('cancelled')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeUndefined()
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
  })

  it('should cancel after start', { timeout: 10_000 }, async () => {
    let executed = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 5000,
      run: async () => {
        executed++
        await sleep(2500)
      },
    })

    const handle = await executor.enqueueTask(task)
    await sleep(500)
    expect(executor.getRunningTaskExecutionIds()).toContain(handle.getExecutionId())
    await handle.cancel()

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('cancelled')
    assert(finishedExecution.status === 'cancelled')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Task cancelled')
    expect(finishedExecution.error?.errorType).toBe('cancelled')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt!.getTime(),
    )
    expect(executor.getRunningTaskExecutionIds()).toContain(finishedExecution.executionId)

    await sleep(2500)
    expect(executor.getRunningTaskExecutionIds()).not.toContain(handle.getExecutionId())
  })

  it('should handle immediate multiple cancellations', async () => {
    let executed = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: async () => {
        executed++
        await sleep(100)
      },
    })

    const handle = await executor.enqueueTask(task)
    await Promise.all([handle.cancel(), handle.cancel(), handle.cancel()])

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(0)
    expect(finishedExecution.status).toBe('cancelled')
    assert(finishedExecution.status === 'cancelled')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Task cancelled')
    expect(finishedExecution.error?.errorType).toBe('cancelled')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeUndefined()
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
  })

  it('should handle multiple cancellations after start', async () => {
    let executed = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 5000,
      run: async () => {
        executed++
        await sleep(2500)
      },
    })

    const handle = await executor.enqueueTask(task)
    await sleep(500)
    await Promise.all([handle.cancel(), handle.cancel(), handle.cancel()])

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('cancelled')
    assert(finishedExecution.status === 'cancelled')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Task cancelled')
    expect(finishedExecution.error?.errorType).toBe('cancelled')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt!.getTime(),
    )
  })

  it('should provide context with attempt and prevError on retry', async () => {
    let executed = 0
    const contexts: Array<TaskRunContext> = []
    const task = executor.task({
      id: 'test',
      retryOptions: {
        maxAttempts: 1,
      },
      timeoutMs: 1000,
      run: async (ctx) => {
        executed++
        await sleep(0)
        contexts.push(ctx)
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(2)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error')
    expect(finishedExecution.error?.errorType).toBe('generic')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
    expect(contexts.length).toBe(2)
    expect(contexts[0]!.attempt).toBe(0)
    expect(contexts[0]!.prevError).toBeUndefined()
    expect(contexts[1]!.attempt).toBe(1)
    expect(contexts[1]!.prevError?.message).toBe('Test error')
  })

  it('should apply sleepMsBeforeRun', async () => {
    let executed = 0
    const startTimes: Array<number> = []
    const task = executor.task({
      id: 'test',
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 250,
      timeoutMs: 1000,
      run: async () => {
        executed++
        await sleep(0)
        startTimes.push(Date.now())
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(2)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error')
    expect(finishedExecution.error?.errorType).toBe('generic')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.startedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.createdAt.getTime() + 250,
    )
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
    expect(startTimes.length).toBe(2)
    expect(startTimes[1]! - startTimes[0]!).toBeLessThan(250)
  })

  it('should apply negative sleepMsBeforeRun', async () => {
    let executed = 0
    const startTimes: Array<number> = []
    const task = executor.task({
      id: 'test',
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: -100,
      timeoutMs: 1000,
      run: async () => {
        executed++
        await sleep(0)
        startTimes.push(Date.now())
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(2)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error')
    expect(finishedExecution.error?.errorType).toBe('generic')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
    expect(startTimes.length).toBe(2)
    expect(startTimes[1]! - startTimes[0]!).toBeGreaterThanOrEqual(0)
    expect(startTimes[1]! - startTimes[0]!).toBeLessThan(250)
  })

  it('should apply undefined delay after failed attempt', async () => {
    let executed = 0
    const startTimes: Array<number> = []
    const task = executor.task({
      id: 'test',
      retryOptions: {
        maxAttempts: 1,
      },
      timeoutMs: 1000,
      run: async () => {
        executed++
        await sleep(0)
        startTimes.push(Date.now())
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(2)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error')
    expect(finishedExecution.error?.errorType).toBe('generic')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
    expect(startTimes.length).toBe(2)
    expect(startTimes[1]! - startTimes[0]!).toBeGreaterThanOrEqual(0)
    expect(startTimes[1]! - startTimes[0]!).toBeLessThan(250)
  })

  it('should handle negative retryOptions.maxAttempts', () => {
    expect(() =>
      executor.task({
        id: 'test',
        retryOptions: {
          maxAttempts: -5,
        },
        timeoutMs: 1000,
        run: () => {
          return 'test'
        },
      }),
    ).toThrow('Invalid retry options for task test')
  })

  it('should handle undefined retryOptions.maxAttempts', () => {
    expect(() =>
      executor.task({
        id: 'test',
        retryOptions: {
          // @ts-expect-error - Testing invalid input
          maxAttempts: undefined,
        },
        timeoutMs: 1000,
        run: () => {
          return 'test'
        },
      }),
    ).toThrow('Invalid retry options for task test')
  })

  it('should respect shutdown signal within task', async () => {
    let executed = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 20_000,
      run: async (ctx) => {
        executed++
        for (let i = 0; i < 20; i++) {
          if (ctx.shutdownSignal.isCancelled()) {
            throw new DurableExecutionCancelledError()
          }
          await sleep(500)
        }
      },
    })

    const handle = await executor.enqueueTask(task)
    await sleep(500)
    await executor.shutdown()

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('cancelled')
    assert(finishedExecution.status === 'cancelled')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Task cancelled')
    expect(finishedExecution.error?.errorType).toBe('cancelled')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt!.getTime(),
    )
    expect(
      finishedExecution.finishedAt.getTime() - finishedExecution.startedAt!.getTime(),
    ).toBeLessThan(2000)
  })

  it('should complete sequential tasks', async () => {
    let executed = 0
    const taskString = executor.task({
      id: 'string',
      timeoutMs: 1000,
      run: (ctx, input: string) => {
        executed++
        return input
      },
    })
    const taskNumber = executor.validateInput(Number).task({
      id: 'number',
      timeoutMs: 1000,
      run: (ctx, input) => {
        executed++
        return input
      },
    })
    const taskBoolean = executor.task({
      id: 'boolean',
      timeoutMs: 1000,
      run: (ctx, input: number) => {
        executed++
        return input > 10 ? true : false
      },
    })

    const task = executor.sequentialTasks(taskString, taskNumber, taskBoolean)

    let handle = await executor.enqueueTask(task, '10.5')

    let finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(3)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toContain('st_')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )

    handle = await executor.enqueueTask(task, '9.5')

    finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(6)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toContain('st_')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })
})
