import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { sleep } from '@gpahal/std/promises'

import {
  DurableExecutor,
  DurableExecutorCancelledError,
  DurableExecutorError,
  type DurableFunctionContext,
} from '../src'
import { InMemoryStorage } from './in-memory-storage'

describe('DurableFunction', () => {
  let storage: InMemoryStorage
  let executor: DurableExecutor

  beforeEach(() => {
    storage = new InMemoryStorage({ enableDebug: false })
    executor = new DurableExecutor(storage, {
      enableDebug: false,
    })
    void executor.start()
  })

  afterEach(() => {
    void executor.shutdown()
  })

  it('should complete', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: 1000,
      execute: async () => {
        executed = 1
        await sleep(1)
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('completed')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeUndefined()
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: 1000,
      execute: () => {
        executed++
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorError')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete after retry', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: 1000,
      maxRetryAttempts: 1,
      execute: async (ctx) => {
        executed++
        if (ctx.attempt === 0) {
          throw new Error('Test error')
        }
        await sleep(1)
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(2)
    expect(finishedExecution.status).toBe('completed')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeUndefined()
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail after retry', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: 1000,
      maxRetryAttempts: 1,
      execute: (ctx) => {
        executed++
        throw new Error(`Test error ${ctx.attempt}`)
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(2)
    expect(finishedExecution.status).toBe('failed')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error 1')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorError')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail after non-retryable error', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: 1000,
      maxRetryAttempts: 1,
      execute: () => {
        executed++
        throw new DurableExecutorError('Test error', false)
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorError')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should timeout', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: 10,
      execute: async () => {
        executed++
        await sleep(100)
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('timed_out')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Function timed out')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorTimedOutError')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with dynamic timeout and retry', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: (attempt) => 25 + attempt * 75,
      maxRetryAttempts: 1,
      execute: async () => {
        executed++
        await sleep(50)
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(2)
    expect(finishedExecution.status).toBe('completed')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeUndefined()
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should timeout with dynamic timeout', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: (attempt) => (attempt + 1) * 10,
      maxRetryAttempts: 1,
      execute: async () => {
        executed++
        await sleep(100)
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(2)
    expect(finishedExecution.status).toBe('timed_out')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Function timed out')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorTimedOutError')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should timeout with invalid timeout', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: -1,
      execute: async () => {
        executed++
        await sleep(100)
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(0)
    expect(finishedExecution.status).toBe('timed_out')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe(
      'Invalid timeout value for function test on attempt 0',
    )
    expect(finishedExecution.error?.tag).toBe('DurableExecutorTimedOutError')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should cancel immediately', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: 1000,
      execute: async () => {
        executed++
        await sleep(100)
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')
    await handle.cancel()

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(0)
    expect(finishedExecution.status).toBe('cancelled')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Function cancelled')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorCancelledError')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeUndefined()
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
  })

  it('should cancel after start', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: 5000,
      execute: async () => {
        executed++
        await sleep(2500)
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')
    await sleep(500)
    await handle.cancel()

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('cancelled')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Function cancelled')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorCancelledError')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should handle immediate multiple cancellations', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: 1000,
      execute: async () => {
        executed++
        await sleep(100)
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')
    await Promise.all([handle.cancel(), handle.cancel(), handle.cancel()])

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(0)
    expect(finishedExecution.status).toBe('cancelled')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Function cancelled')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorCancelledError')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeUndefined()
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
  })

  it('should handle multiple cancellations after start', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: 5000,
      execute: async () => {
        executed++
        await sleep(2500)
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')
    await sleep(500)
    await Promise.all([handle.cancel(), handle.cancel(), handle.cancel()])

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('cancelled')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Function cancelled')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorCancelledError')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should provide context with attempt and prevError on retry', async () => {
    let executed = 0
    const contexts: Array<DurableFunctionContext> = []
    executor.addFunction({
      id: 'test',
      timeoutMs: 1000,
      maxRetryAttempts: 1,
      execute: async (ctx) => {
        executed++
        await sleep(0)
        contexts.push(ctx)
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(2)
    expect(finishedExecution.status).toBe('failed')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorError')
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

  it('should apply delay after failed attempt', async () => {
    let executed = 0
    const startTimes: Array<number> = []
    executor.addFunction({
      id: 'test',
      timeoutMs: 1000,
      maxRetryAttempts: 1,
      delayMsAfterAttempt: 250,
      execute: async () => {
        executed++
        await sleep(0)
        startTimes.push(Date.now())
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(2)
    expect(finishedExecution.status).toBe('failed')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorError')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
    expect(startTimes.length).toBe(2)
    expect(startTimes[1]! - startTimes[0]!).toBeGreaterThanOrEqual(250)
  })

  it('should apply dynamic delay after failed attempt', async () => {
    let executed = 0
    const startTimes: Array<number> = []
    executor.addFunction({
      id: 'test',
      timeoutMs: 1000,
      maxRetryAttempts: 2,
      delayMsAfterAttempt: (attempt) => (attempt + 1) * 250,
      execute: async () => {
        executed++
        await sleep(0)
        startTimes.push(Date.now())
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(3)
    expect(finishedExecution.status).toBe('failed')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorError')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
    expect(startTimes.length).toBe(3)
    expect(startTimes[1]! - startTimes[0]!).toBeGreaterThanOrEqual(250)
    expect(startTimes[1]! - startTimes[0]!).toBeLessThan(500)
    expect(startTimes[2]! - startTimes[1]!).toBeGreaterThanOrEqual(500)
  })

  it('should apply negative delay after failed attempt', async () => {
    let executed = 0
    const startTimes: Array<number> = []
    executor.addFunction({
      id: 'test',
      timeoutMs: 1000,
      maxRetryAttempts: 1,
      delayMsAfterAttempt: -100,
      execute: async () => {
        executed++
        await sleep(0)
        startTimes.push(Date.now())
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(2)
    expect(finishedExecution.status).toBe('failed')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorError')
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
    executor.addFunction({
      id: 'test',
      timeoutMs: 1000,
      maxRetryAttempts: 1,
      execute: async () => {
        executed++
        await sleep(0)
        startTimes.push(Date.now())
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(2)
    expect(finishedExecution.status).toBe('failed')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorError')
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

  it('should handle negative maxRetryAttempts', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: 1000,
      maxRetryAttempts: -5,
      execute: () => {
        executed++
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorError')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should handle undefined maxRetryAttempts', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: 1000,
      execute: () => {
        executed++
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Test error')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorError')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should respect shutdown signal within function', async () => {
    let executed = 0
    executor.addFunction({
      id: 'test',
      timeoutMs: 20_000,
      execute: async (ctx) => {
        executed++
        for (let i = 0; i < 20; i++) {
          if (ctx.shutdownSignal.isCancelled()) {
            throw new DurableExecutorCancelledError()
          }
          await sleep(500)
        }
      },
    })

    const handle = await executor.enqueueFunction('test', 'test_1')
    await sleep(500)
    await executor.shutdown()

    const finishedExecution = await handle.waitAndGetExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('cancelled')
    expect(finishedExecution.fnId).toBe('test')
    expect(finishedExecution.executionId).toBe('test_1')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Function cancelled')
    expect(finishedExecution.error?.tag).toBe('DurableExecutorCancelledError')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
    expect(
      finishedExecution.finishedAt.getTime() - finishedExecution.startedAt.getTime(),
    ).toBeLessThan(2000)
  })
})
