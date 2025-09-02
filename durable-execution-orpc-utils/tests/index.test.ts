import { createRouterClient, ORPCError, os, type } from '@orpc/server'
import {
  DurableExecutionError,
  DurableExecutionNotFoundError,
  DurableExecutor,
  InMemoryTaskExecutionsStorage,
  type FinishedTaskExecution,
  type Task,
} from 'durable-execution'

import { sleep } from '@gpahal/std/promises'

import { convertProcedureClientToTask, createTasksRouter } from '../src'

async function waitAndGetFinishedExecution<TInput, TOutput>(
  executor: DurableExecutor,
  task: Task<TInput, TOutput>,
  executionId: string,
): Promise<FinishedTaskExecution<TOutput>> {
  const handle = await executor.getTaskExecutionHandle(task, executionId)
  return await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 25 })
}

describe('server', () => {
  let storage: InMemoryTaskExecutionsStorage
  let executor: DurableExecutor

  beforeEach(() => {
    storage = new InMemoryTaskExecutionsStorage()
    executor = new DurableExecutor(storage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    executor.start()
  })

  afterEach(async () => {
    await executor.shutdown()
  })

  it('should enqueue and get execution', async () => {
    let executionCount = 0
    const add1 = executor.task({
      id: 'add1',
      timeoutMs: 5000,
      run: async (_, input: { n: number }) => {
        executionCount++
        await sleep(250)
        return { n: input.n + 1 }
      },
    })

    const tasks = { add1 } as const
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })

    const executionId = await client.enqueueTask({ taskId: 'add1', input: { n: 0 } })
    let execution = await client.getTaskExecution({ taskId: 'add1', executionId })
    expect(['ready', 'running']).toContain(execution.status)

    await waitAndGetFinishedExecution(executor, add1, executionId)
    execution = await client.getTaskExecution({ taskId: 'add1', executionId })
    expect(executionCount).toBe(1)
    expect(execution.status).toBe('completed')
    assert(execution.status === 'completed')
    expect(execution.output).toBeDefined()
    expect(execution.output.n).toEqual(1)
  })

  it('should handle invalid task id to enqueue', async () => {
    const router = createTasksRouter(os, executor, {})
    const client = createRouterClient(router, { context: {} })

    // @ts-expect-error - Testing invalid input
    await expect(client.enqueueTask({ taskId: 'invalid', input: { n: 0 } })).rejects.toThrow(
      'Task invalid not found',
    )
  })

  it('should handle invalid task id to get execution', async () => {
    const router = createTasksRouter(os, executor, {})
    const client = createRouterClient(router, { context: {} })

    await expect(
      // @ts-expect-error - Testing invalid input
      client.getTaskExecution({ taskId: 'invalid', executionId: 'invalid' }),
    ).rejects.toThrow('Task invalid not found')
  })

  it('should handle invalid execution id to get execution', async () => {
    const add1 = executor.task({
      id: 'add1',
      timeoutMs: 5000,
      run: (_, input: { n: number }) => {
        return { n: input.n + 1 }
      },
    })

    const tasks = { add1 }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })

    await expect(
      client.getTaskExecution({ taskId: 'add1', executionId: 'invalid' }),
    ).rejects.toThrow('Task execution invalid not found')
  })

  it('should wakeup sleeping task execution', async () => {
    const sleepingTask = executor.sleepingTask<string>({
      id: 'sleeping',
      timeoutMs: 10_000,
    })

    const tasks = { sleepingTask }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })

    const executionId = await client.enqueueTask({ taskId: 'sleepingTask', input: 'unique_id' })
    expect(typeof executionId).toBe('string')
    expect(executionId.length).toBeGreaterThan(0)

    const execution = await client.getTaskExecution({ taskId: 'sleepingTask', executionId })
    expect(['ready', 'running']).toContain(execution.status)

    await sleep(250)
    const finishedExecution = await client.wakeupSleepingTaskExecution({
      taskId: 'sleepingTask',
      sleepingTaskUniqueId: 'unique_id',
      options: {
        status: 'completed',
        output: 'sleeping_output',
      },
    })
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.output).toBe('sleeping_output')
  })

  it('should handle invalid sleeping task unique id to wakeup sleeping task execution', async () => {
    const sleepingTask = executor.sleepingTask<string>({
      id: 'sleeping',
      timeoutMs: 10_000,
    })

    const tasks = { sleepingTask }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })

    const executionId = await client.enqueueTask({ taskId: 'sleepingTask', input: 'unique_id' })
    expect(typeof executionId).toBe('string')
    expect(executionId.length).toBeGreaterThan(0)

    const execution = await client.getTaskExecution({ taskId: 'sleepingTask', executionId })
    expect(['ready', 'running']).toContain(execution.status)

    await sleep(250)
    await expect(
      client.wakeupSleepingTaskExecution({
        taskId: 'sleepingTask',
        sleepingTaskUniqueId: 'invalid',
        options: {
          status: 'completed',
          output: 'sleeping_output',
        },
      }),
    ).rejects.toThrow('Sleeping task execution invalid not found')
  })

  it('should handle invalid task id to wakeup sleeping task execution', async () => {
    const router = createTasksRouter(os, executor, {})
    const client = createRouterClient(router, { context: {} })

    await expect(
      // @ts-expect-error - Testing invalid input
      client.wakeupSleepingTaskExecution({
        taskId: 'invalid',
        sleepingTaskUniqueId: 'unique_id',
        options: {
          status: 'completed',
          output: 'sleeping_output' as never,
        },
      }),
    ).rejects.toThrow('Task invalid not found')
  })

  it('should handle non-sleeping task to wakeup sleeping task execution', async () => {
    const regularTask = executor.task({
      id: 'regular',
      timeoutMs: 5000,
      run: (_, input: { n: number }) => {
        return { n: input.n + 1 }
      },
    })

    const tasks = { regularTask }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })

    await expect(
      client.wakeupSleepingTaskExecution({
        taskId: 'regularTask',
        sleepingTaskUniqueId: 'unique_id',
        options: {
          status: 'completed',
          output: { n: 1 } as never,
        },
      }),
    ).rejects.toThrow('Task regularTask is not a sleeping task')
  })

  it('should handle generic error when waking up sleeping task execution', async () => {
    const sleepingTask = executor.sleepingTask<string>({
      id: 'sleeping',
      timeoutMs: 10_000,
    })

    const tasks = { sleepingTask }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })

    vi.spyOn(executor, 'wakeupSleepingTaskExecution').mockRejectedValueOnce(
      new Error('Database connection lost'),
    )

    await expect(
      client.wakeupSleepingTaskExecution({
        taskId: 'sleepingTask',
        sleepingTaskUniqueId: 'unique_id',
        options: {
          status: 'completed',
          output: 'test_output',
        },
      }),
    ).rejects.toThrow('Database connection lost')
  })

  it('should complete procedureClientTask', async () => {
    let executionCount = 0
    const add1Proc = os
      .input(type<{ n: number }>())
      .output(type<{ n: number }>())
      .handler(({ input }: { input: { n: number } }) => {
        executionCount++
        return { n: input.n + 1 }
      })

    const router = { add1: add1Proc }

    const client = createRouterClient(router, { context: {} })
    const add1 = convertProcedureClientToTask(
      executor,
      { id: 'add1', timeoutMs: 1000 },
      client.add1,
    )

    const handle = await executor.enqueueTask(add1, { n: 0 })
    const finishedExecution = await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 25 })
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.n).toEqual(1)
  })

  it('should handle procedureClientTask generic error', async () => {
    let executionCount = 0
    const add1Proc = os
      .input(type<{ n: number }>())
      .output(type<{ n: number }>())
      .handler(() => {
        executionCount++
        throw new ORPCError('BAD_REQUEST', { message: 'invalid' })
      })

    const router = { add1: add1Proc }

    const client = createRouterClient(router, { context: {} })
    const add1 = convertProcedureClientToTask(
      executor,
      { id: 'add1', timeoutMs: 1000 },
      client.add1,
    )

    const handle = await executor.enqueueTask(add1, { n: 0 })
    const finishedExecution = await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 25 })
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error.errorType).toBe('generic')
    expect(finishedExecution.error.isRetryable).toBe(false)
    expect(finishedExecution.error.isInternal).toBe(false)
    expect(finishedExecution.error.message).toBe('invalid')
  })

  it('should handle procedureClientTask not found error', async () => {
    let executionCount = 0
    const add1Proc = os
      .input(type<{ n: number }>())
      .output(type<{ n: number }>())
      .handler(() => {
        executionCount++
        throw new ORPCError('NOT_FOUND', { message: 'missing' })
      })

    const router = { add1: add1Proc }

    const client = createRouterClient(router, { context: {} })
    const add1 = convertProcedureClientToTask(
      executor,
      { id: 'add1', timeoutMs: 1000 },
      client.add1,
    )

    const handle = await executor.enqueueTask(add1, { n: 0 })
    const finishedExecution = await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 25 })
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error.errorType).toBe('not_found')
    expect(finishedExecution.error.isRetryable).toBe(false)
    expect(finishedExecution.error.isInternal).toBe(false)
    expect(finishedExecution.error.message).toBe('missing')
  })

  it('should handle procedureClientTask internal server error', async () => {
    let executionCount = 0
    const add1Proc = os
      .input(type<{ n: number }>())
      .output(type<{ n: number }>())
      .handler(() => {
        executionCount++
        throw new ORPCError('INTERNAL_SERVER_ERROR', { message: 'server down' })
      })

    const router = { add1: add1Proc }

    const client = createRouterClient(router, { context: {} })
    const add1 = convertProcedureClientToTask(
      executor,
      { id: 'add1', timeoutMs: 1000 },
      client.add1,
    )

    const handle = await executor.enqueueTask(add1, { n: 0 })
    const finishedExecution = await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 25 })
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error.errorType).toBe('generic')
    expect(finishedExecution.error.isRetryable).toBe(true)
    expect(finishedExecution.error.isInternal).toBe(true)
    expect(finishedExecution.error.message).toBe('server down')
  })

  it('should handle procedureClientTask custom error', async () => {
    let executionCount = 0
    const add1Proc = os
      .input(type<{ n: number }>())
      .output(type<{ n: number }>())
      .handler(() => {
        executionCount++
        throw new Error('boom')
      })

    const router = { add1: add1Proc }

    const client = createRouterClient(router, { context: {} })
    const add1 = convertProcedureClientToTask(
      executor,
      { id: 'add1', timeoutMs: 1000 },
      client.add1,
    )

    const handle = await executor.enqueueTask(add1, { n: 0 })
    const finishedExecution = await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 25 })
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error.errorType).toBe('generic')
    expect(finishedExecution.error.isRetryable).toBe(true)
    expect(finishedExecution.error.isInternal).toBe(true)
    expect(finishedExecution.error.message).toBe('Internal server error')
  })

  it('should handle DurableExecutionError when enqueueing task', async () => {
    const add1 = executor.task({
      id: 'add1',
      timeoutMs: 5000,
      run: (_, input: { n: number }) => {
        return { n: input.n + 1 }
      },
    })

    const tasks = { add1 }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })

    vi.spyOn(executor, 'enqueueTask').mockRejectedValueOnce(
      DurableExecutionError.nonRetryable('Task queue is full'),
    )

    await expect(client.enqueueTask({ taskId: 'add1', input: { n: 0 } })).rejects.toThrow(
      'Task queue is full',
    )
  })

  it('should handle DurableExecutionNotFoundError when enqueueing task', async () => {
    const add1 = executor.task({
      id: 'add1',
      timeoutMs: 5000,
      run: (_, input: { n: number }) => {
        return { n: input.n + 1 }
      },
    })

    const tasks = { add1 }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })

    vi.spyOn(executor, 'enqueueTask').mockRejectedValueOnce(
      new DurableExecutionNotFoundError('Task definition not found'),
    )

    await expect(client.enqueueTask({ taskId: 'add1', input: { n: 0 } })).rejects.toThrow(
      'Task definition not found',
    )
  })

  it('should handle internal DurableExecutionError when enqueueing task', async () => {
    const add1 = executor.task({
      id: 'add1',
      timeoutMs: 5000,
      run: (_, input: { n: number }) => {
        return { n: input.n + 1 }
      },
    })

    const tasks = { add1 }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })

    vi.spyOn(executor, 'enqueueTask').mockRejectedValueOnce(
      DurableExecutionError.retryable('Database connection failed', true),
    )

    await expect(client.enqueueTask({ taskId: 'add1', input: { n: 0 } })).rejects.toThrow(
      'Database connection failed',
    )
  })

  it('should handle generic error when enqueueing task', async () => {
    const add1 = executor.task({
      id: 'add1',
      timeoutMs: 5000,
      run: (_, input: { n: number }) => {
        return { n: input.n + 1 }
      },
    })

    const tasks = { add1 }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })

    vi.spyOn(executor, 'enqueueTask').mockRejectedValueOnce(new Error('Unexpected error'))

    await expect(client.enqueueTask({ taskId: 'add1', input: { n: 0 } })).rejects.toThrow(
      'Unexpected error',
    )
  })

  it('should handle DurableExecutionError when getting task execution', async () => {
    const add1 = executor.task({
      id: 'add1',
      timeoutMs: 5000,
      run: (_, input: { n: number }) => {
        return { n: input.n + 1 }
      },
    })

    const tasks = { add1 }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })

    vi.spyOn(executor, 'getTaskExecutionHandle').mockRejectedValueOnce(
      DurableExecutionError.retryable('Storage error'),
    )

    await expect(
      client.getTaskExecution({ taskId: 'add1', executionId: 'test-id' }),
    ).rejects.toThrow('Storage error')
  })

  it('should handle generic error when getting task execution', async () => {
    const add1 = executor.task({
      id: 'add1',
      timeoutMs: 5000,
      run: (_, input: { n: number }) => {
        return { n: input.n + 1 }
      },
    })

    const tasks = { add1 }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })

    vi.spyOn(executor, 'getTaskExecutionHandle').mockRejectedValueOnce(
      new TypeError('Network error'),
    )

    await expect(
      client.getTaskExecution({ taskId: 'add1', executionId: 'test-id' }),
    ).rejects.toThrow('Network error')
  })

  it('should enqueue task with options', async () => {
    const add1 = executor.task({
      id: 'add1',
      timeoutMs: 5000,
      run: async (_, input: { n: number }) => {
        await sleep(100)
        return { n: input.n + 1 }
      },
    })

    const tasks = { add1 }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })

    const executionId = await client.enqueueTask({
      taskId: 'add1',
      input: { n: 5 },
      options: { retryOptions: { maxAttempts: 3 } },
    })
    expect(typeof executionId).toBe('string')
    expect(executionId.length).toBeGreaterThan(0)

    const execution = await client.getTaskExecution({ taskId: 'add1', executionId })
    expect(['ready', 'running']).toContain(execution.status)
  })
})

describe('convertProcedureClientToTask', () => {
  let storage: InMemoryTaskExecutionsStorage
  let executor: DurableExecutor

  beforeEach(() => {
    storage = new InMemoryTaskExecutionsStorage()
    executor = new DurableExecutor(storage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    executor.start()
  })

  afterEach(async () => {
    await executor.shutdown()
  })

  it('should handle procedure client task with retryable status codes', async () => {
    const retryableStatuses = [408, 429, 500, 502, 503, 504]
    for (const status of retryableStatuses) {
      let executionCount = 0
      const testProc = os
        .input(type<{ n: number }>())
        .output(type<{ n: number }>())
        .handler(() => {
          executionCount++
          throw new ORPCError('INTERNAL_SERVER_ERROR', {
            message: `HTTP ${status} error`,
            status,
          })
        })

      const router = { test: testProc }
      const client = createRouterClient(router, { context: {} })
      const task = convertProcedureClientToTask(
        executor,
        { id: `test_retryable_${status}`, timeoutMs: 1000 },
        client.test,
      )

      const handle = await executor.enqueueTask(task, { n: 0 })
      const finishedExecution = await handle.waitAndGetFinishedExecution({
        pollingIntervalMs: 25,
      })

      expect(executionCount).toBe(1)
      expect(finishedExecution.status).toBe('failed')
      assert(finishedExecution.status === 'failed')
      expect(finishedExecution.error.isRetryable).toBe(true)
      expect(finishedExecution.error.isInternal).toBe(status >= 500)
    }
  })

  it('should handle procedure client task with non-retryable status codes', async () => {
    const nonRetryableStatuses = [400, 401, 403, 422]
    for (const status of nonRetryableStatuses) {
      let executionCount = 0
      const testProc = os
        .input(type<{ n: number }>())
        .output(type<{ n: number }>())
        .handler(() => {
          executionCount++
          throw new ORPCError('BAD_REQUEST', {
            message: `HTTP ${status} error`,
            status,
          })
        })

      const router = { test: testProc }
      const client = createRouterClient(router, { context: {} })
      const task = convertProcedureClientToTask(
        executor,
        { id: `test_non_retryable_${status}`, timeoutMs: 1000 },
        client.test,
      )

      const handle = await executor.enqueueTask(task, { n: 0 })
      const finishedExecution = await handle.waitAndGetFinishedExecution({
        pollingIntervalMs: 25,
      })

      expect(executionCount).toBe(1)
      expect(finishedExecution.status).toBe('failed')
      assert(finishedExecution.status === 'failed')
      expect(finishedExecution.error.isRetryable).toBe(false)
      expect(finishedExecution.error.isInternal).toBe(false)
    }
  })

  it('should handle procedure client task with network failure', async () => {
    let executionCount = 0
    const testProc = os
      .input(type<{ n: number }>())
      .output(type<{ n: number }>())
      .handler(() => {
        executionCount++
        throw new ORPCError('SERVICE_UNAVAILABLE', {
          message: 'Service temporarily unavailable',
          status: 503,
        })
      })

    const router = { test: testProc }
    const client = createRouterClient(router, { context: {} })
    const task = convertProcedureClientToTask(
      executor,
      { id: 'test_network_failure', timeoutMs: 1000 },
      client.test,
    )

    const handle = await executor.enqueueTask(task, { n: 0 })
    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 25,
    })

    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error.isRetryable).toBe(true)
    expect(finishedExecution.error.isInternal).toBe(true)
    expect(finishedExecution.error.message).toBe('Service temporarily unavailable')
  })

  it('should handle procedure client task with client context', async () => {
    type CustomClientContext = { authType: 'bearer' | 'basic' }

    const testProc = os
      .$context<{ headers: { authorization: string } }>()
      .input(type<{ n: number }>())
      .output(type<{ n: number }>())
      .handler(({ input, context }) => {
        expect(context.headers.authorization).toBe('Bearer token')
        return { n: input.n + 1 }
      })

    const router = { test: testProc }
    const client = createRouterClient(router, {
      context: (cc: CustomClientContext) => ({
        headers: { authorization: cc.authType === 'bearer' ? 'Bearer token' : 'Basic token' },
      }),
    })
    const task = convertProcedureClientToTask(
      executor,
      { id: 'test_with_context', timeoutMs: 1000 },
      client.test,
      { context: { authType: 'bearer' } },
    )

    const handle = await executor.enqueueTask(task, { n: 0 })
    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 25,
    })

    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.output.n).toBe(1)
  })

  it('should handle timeout error as retryable', async () => {
    let executionCount = 0
    const testProc = os
      .input(type<{ n: number }>())
      .output(type<{ n: number }>())
      .handler(() => {
        executionCount++
        throw new ORPCError('TIMEOUT', {
          message: 'Request timeout',
          status: 408,
        })
      })

    const router = { test: testProc }
    const client = createRouterClient(router, { context: {} })
    const task = convertProcedureClientToTask(
      executor,
      { id: 'test_timeout', timeoutMs: 1000 },
      client.test,
    )

    const handle = await executor.enqueueTask(task, { n: 0 })
    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 25,
    })

    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error.isRetryable).toBe(true)
    expect(finishedExecution.error.isInternal).toBe(false)
  })

  it('should handle gateway errors as retryable and internal', async () => {
    let executionCount = 0
    const testProc = os
      .input(type<{ n: number }>())
      .output(type<{ n: number }>())
      .handler(() => {
        executionCount++
        throw new ORPCError('BAD_GATEWAY', {
          message: 'Bad gateway',
          status: 502,
        })
      })

    const router = { test: testProc }
    const client = createRouterClient(router, { context: {} })
    const task = convertProcedureClientToTask(
      executor,
      { id: 'test_gateway', timeoutMs: 1000 },
      client.test,
    )

    const handle = await executor.enqueueTask(task, { n: 0 })
    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 25,
    })

    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error.isRetryable).toBe(true)
    expect(finishedExecution.error.isInternal).toBe(true)
  })

  it('should handle non-error thrown values as internal server error', async () => {
    let executionCount = 0
    const testProc = os
      .input(type<{ n: number }>())
      .output(type<{ n: number }>())
      .handler(() => {
        executionCount++
        // eslint-disable-next-line @typescript-eslint/only-throw-error
        throw 'Unknown error type'
      })

    const router = { test: testProc }
    const client = createRouterClient(router, { context: {} })
    const task = convertProcedureClientToTask(
      executor,
      { id: 'test_unknown_error', timeoutMs: 1000 },
      client.test,
    )

    const handle = await executor.enqueueTask(task, { n: 0 })
    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 25,
    })

    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error.isRetryable).toBe(true)
    expect(finishedExecution.error.isInternal).toBe(true)
    expect(finishedExecution.error.message).toBe('Internal server error')
  })
})
