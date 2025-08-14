import { z } from 'zod'

import { sleep } from '@gpahal/std/promises'

import {
  DurableExecutionCancelledError,
  DurableExecutionError,
  DurableExecutor,
  InMemoryTaskExecutionsStorage,
  type TaskRunContext,
} from '../src'

describe('simpleTask', () => {
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
    await executor.shutdown()
  })

  it('should complete', async () => {
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: () => {
        executionCount++
        return 'test'
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
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

  it('should complete async', async () => {
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'test'
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
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

  it('should complete with valid input', async () => {
    let executionCount = 0
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
          executionCount++
          return input
        },
      })

    const handle = await executor.enqueueTask(task, 'test')

    const finishedExecution = await handle.waitAndGetFinishedExecution()
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

  it('should fail with invalid input', async () => {
    let executionCount = 0
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
          executionCount++
          return input
        },
      })

    const handle = await executor.enqueueTask(task, 'invalid')

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(0)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Invalid input to task test: Invalid input')
    expect(finishedExecution.error?.errorType).toBe('generic')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with input schema', async () => {
    let executionCount = 0
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
          executionCount++
          await sleep(1)
          return input.name
        },
      })

    const handle = await executor.enqueueTask(task, { name: 'test' })

    const finishedExecution = await handle.waitAndGetFinishedExecution()
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

  it('should fail with input schema', async () => {
    let executionCount = 0
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
          executionCount++
          await sleep(1)
          return input.name
        },
      })

    // @ts-expect-error - Testing invalid input
    const handle = await executor.enqueueTask(task, { name: 0 })

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(0)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toContain('Invalid input to task test')
    expect(finishedExecution.error?.errorType).toBe('generic')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail', async () => {
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: () => {
        executionCount++
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(1)
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
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      retryOptions: {
        maxAttempts: 1,
      },
      timeoutMs: 1000,
      run: async (ctx) => {
        executionCount++
        if (ctx.attempt === 0) {
          throw new Error('Test error')
        }
        await sleep(1)
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(2)
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
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      retryOptions: {
        maxAttempts: 1,
      },
      timeoutMs: 1000,
      run: (ctx) => {
        executionCount++
        throw new Error(`Test error ${ctx.attempt}`)
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(2)
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
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      retryOptions: {
        maxAttempts: 1,
      },
      timeoutMs: 1000,
      run: () => {
        executionCount++
        throw DurableExecutionError.nonRetryable('Test error')
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(1)
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
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 10,
      run: async () => {
        executionCount++
        await sleep(100)
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('timed_out')
    assert(finishedExecution.status === 'timed_out')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Task execution timed out')
    expect(finishedExecution.error?.errorType).toBe('timed_out')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with enqueue timeout', async () => {
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      retryOptions: {
        maxAttempts: 1,
      },
      timeoutMs: 10,
      run: async () => {
        executionCount++
        await sleep(50)
      },
    })

    const handle = await executor.enqueueTask(task, undefined, {
      timeoutMs: 1000,
    })

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(1)
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
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(100)
      },
    })

    const handle = await executor.enqueueTask(task)
    await handle.cancel()

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(0)
    expect(finishedExecution.status).toBe('cancelled')
    assert(finishedExecution.status === 'cancelled')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Task execution cancelled')
    expect(finishedExecution.error?.errorType).toBe('cancelled')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeUndefined()
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
  })

  it('should cancel after start', { timeout: 10_000 }, async () => {
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 5000,
      run: async () => {
        executionCount++
        await sleep(2500)
      },
    })

    const handle = await executor.enqueueTask(task)
    await sleep(500)
    expect(executor.getRunningTaskExecutionIds()).toContain(handle.getExecutionId())
    await handle.cancel()

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('cancelled')
    assert(finishedExecution.status === 'cancelled')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Task execution cancelled')
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
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(100)
      },
    })

    const handle = await executor.enqueueTask(task)
    await Promise.all([handle.cancel(), handle.cancel(), handle.cancel()])

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(0)
    expect(finishedExecution.status).toBe('cancelled')
    assert(finishedExecution.status === 'cancelled')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Task execution cancelled')
    expect(finishedExecution.error?.errorType).toBe('cancelled')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeUndefined()
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
  })

  it('should handle multiple cancellations after start', async () => {
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 5000,
      run: async () => {
        executionCount++
        await sleep(2500)
      },
    })

    const handle = await executor.enqueueTask(task)
    await sleep(500)
    await Promise.all([handle.cancel(), handle.cancel(), handle.cancel()])

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('cancelled')
    assert(finishedExecution.status === 'cancelled')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Task execution cancelled')
    expect(finishedExecution.error?.errorType).toBe('cancelled')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt!.getTime(),
    )
  })

  it('should provide context with attempt and prevError on retry', async () => {
    let executionCount = 0
    const contexts: Array<TaskRunContext> = []
    const task = executor.task({
      id: 'test',
      retryOptions: {
        maxAttempts: 1,
      },
      timeoutMs: 1000,
      run: async (ctx) => {
        executionCount++
        await sleep(0)
        contexts.push(ctx)
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(2)
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
    let executionCount = 0
    const startTimes: Array<number> = []
    const task = executor.task({
      id: 'test',
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 250,
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(0)
        startTimes.push(Date.now())
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(2)
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

  it('should apply negative sleepMsBeforeRun', () => {
    expect(() =>
      executor.task({
        id: 'test',
        retryOptions: {
          maxAttempts: 1,
        },
        sleepMsBeforeRun: -100,
        timeoutMs: 1000,
        run: async () => {
          await sleep(0)
          throw new Error('Test error')
        },
      }),
    ).toThrow('Invalid sleep ms before run for task test')
  })

  it('should apply undefined delay after failed attempt', async () => {
    let executionCount = 0
    const startTimes: Array<number> = []
    const task = executor.task({
      id: 'test',
      retryOptions: {
        maxAttempts: 1,
      },
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(0)
        startTimes.push(Date.now())
        throw new Error('Test error')
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(2)
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
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 20_000,
      run: async (ctx) => {
        executionCount++
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
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('cancelled')
    assert(finishedExecution.status === 'cancelled')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toBe('Task execution cancelled')
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
    let executionCount = 0
    const taskString = executor.task({
      id: 'string',
      timeoutMs: 1000,
      run: (ctx, input: string) => {
        executionCount++
        return input
      },
    })
    const taskNumber = executor.validateInput(Number).task({
      id: 'number',
      timeoutMs: 1000,
      run: (ctx, input) => {
        executionCount++
        return input
      },
    })
    const taskBoolean = executor.task({
      id: 'boolean',
      timeoutMs: 1000,
      run: (ctx, input: number) => {
        executionCount++
        return input > 10 ? true : false
      },
    })

    const task = executor.sequentialTasks(taskString, taskNumber, taskBoolean)

    let handle = await executor.enqueueTask(task, '10.5')

    let finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(3)
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
    expect(executionCount).toBe(6)
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

  it('should fail when task input exceeds serialization size limit', async () => {
    const tooLargeString = 'x'.repeat(1024 * 1024 + 1000)

    const testTask = executor.task({
      id: 'too_large_input',
      timeoutMs: 5000,
      run: (_ctx, input: string) => {
        return input.length
      },
    })

    await expect(async () => {
      await executor.enqueueTask(testTask, tooLargeString)
    }).rejects.toThrow()
  })

  it('should fail when task output exceeds serialization size limit', async () => {
    const tooLargeOutput = 'z'.repeat(10 * 1024 * 1024 + 10_000)

    const testTask = executor.task({
      id: 'too_large_output',
      timeoutMs: 5000,
      run: () => {
        return tooLargeOutput
      },
    })

    const handle = await executor.enqueueTask(testTask)
    const finishedExecution = await handle.waitAndGetFinishedExecution()

    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error?.message).toContain('size')
  })

  it('should handle sequential task error propagation correctly', async () => {
    let executionCount = 0

    const task1 = executor.task({
      id: 'seq_task1',
      timeoutMs: 1000,
      run: () => {
        executionCount++
        return 'task1'
      },
    })

    const task2 = executor.task<string, string>({
      id: 'seq_task2',
      timeoutMs: 1000,
      run: () => {
        executionCount++
        throw new Error('Task 2 failed')
      },
    })

    const task3 = executor.task<string, string>({
      id: 'seq_task3',
      timeoutMs: 1000,
      run: () => {
        executionCount++
        return 'task3'
      },
    })

    const sequentialTask = executor.sequentialTasks(task1, task2, task3)

    const handle = await executor.enqueueTask(sequentialTask)
    const finishedExecution = await handle.waitAndGetFinishedExecution()

    expect(finishedExecution.status).toBe('finalize_failed')
    assert(finishedExecution.status === 'finalize_failed')
    expect(finishedExecution.error?.message).toContain('Task 2 failed')
    expect(executionCount).toBe(2)
  })

  it('should handle sequential task cancellation at different stages', async () => {
    let task1Started = false
    let task2Started = false

    const task1 = executor.task({
      id: 'seq_cancel_task1',
      timeoutMs: 5000,
      run: async () => {
        task1Started = true
        await sleep(100)
        return 'task1'
      },
    })

    const task2 = executor.task<string, string>({
      id: 'seq_cancel_task2',
      timeoutMs: 5000,
      run: async () => {
        task2Started = true
        await sleep(1000)
        return 'task2'
      },
    })

    const sequentialTask = executor.sequentialTasks(task1, task2)

    const handle = await executor.enqueueTask(sequentialTask)
    await sleep(500)
    await handle.cancel()

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(finishedExecution.status).toBe('cancelled')
    expect(task1Started).toBe(true)
    expect(task2Started).toBe(true)
  })

  it('should handle sequential task timeout scenarios', async () => {
    const task1 = executor.task({
      id: 'seq_timeout_task1',
      timeoutMs: 100,
      run: async () => {
        await sleep(500)
        return 'task1'
      },
    })

    const task2 = executor.task<string, string>({
      id: 'seq_timeout_task2',
      timeoutMs: 1000,
      run: () => {
        return 'task2'
      },
    })

    const sequentialTask = executor.sequentialTasks(task1, task2)

    const handle = await executor.enqueueTask(sequentialTask)
    const finishedExecution = await handle.waitAndGetFinishedExecution()

    expect(finishedExecution.status).toBe('finalize_failed')
    assert(finishedExecution.status === 'finalize_failed')
    expect(finishedExecution.error?.message).toContain('Task execution timed out')
  })

  it('should handle empty sequential task array', () => {
    // @ts-expect-error - Testing invalid input
    expect(() => executor.sequentialTasks()).toThrow()
  })

  it('should complete single sequential task', async () => {
    let executionCount = 0
    const task1 = executor.task({
      id: 'seq_task',
      timeoutMs: 1000,
      run: () => {
        executionCount++
        return 'task1'
      },
    })

    const sequentialTask = executor.sequentialTasks(task1)

    const handle = await executor.enqueueTask(sequentialTask)
    const finishedExecution = await handle.waitAndGetFinishedExecution()

    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.output).toBe('task1')
    expect(executionCount).toBe(1)
  })

  it('should handle single sequential task error propagation correctly', async () => {
    let executionCount = 0
    const task1 = executor.task({
      id: 'seq_task',
      timeoutMs: 1000,
      run: () => {
        executionCount++
        throw new Error('Task 1 failed')
      },
    })

    const sequentialTask = executor.sequentialTasks(task1)

    const handle = await executor.enqueueTask(sequentialTask)
    const finishedExecution = await handle.waitAndGetFinishedExecution()

    expect(finishedExecution.status).toBe('finalize_failed')
    assert(finishedExecution.status === 'finalize_failed')
    expect(finishedExecution.error?.message).toContain('Task 1 failed')
    expect(executionCount).toBe(1)
  })

  it('should handle task closure', async () => {
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: () => {
        executionCount++
        return 'test'
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.output).toBe('test')

    while (true) {
      const executionStorageValue = await storage.getById(handle.getExecutionId(), {})
      expect(executionStorageValue).toBeDefined()
      assert(executionStorageValue)
      expect(executionStorageValue.status).toBe('completed')

      if (executionStorageValue.closeStatus === 'closed') {
        expect(executionStorageValue.closedAt).toBeInstanceOf(Date)
        break
      }

      await sleep(100)
    }
  })
})
