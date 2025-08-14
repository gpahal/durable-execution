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

import { createTaskClientHandles } from '../src/client'
import { convertProcedureClientToTask, createTasksRouter } from '../src/server'

async function waitAndGetFinishedExecution<TInput, TOutput>(
  executor: DurableExecutor,
  task: Task<TInput, TOutput>,
  executionId: string,
): Promise<FinishedTaskExecution<TOutput>> {
  const handle = await executor.getTaskHandle(task, executionId)
  return await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 20 })
}

describe('server', () => {
  let storage: InMemoryTaskExecutionsStorage
  let executor: DurableExecutor

  beforeEach(() => {
    storage = new InMemoryTaskExecutionsStorage()
    executor = new DurableExecutor(storage, {
      backgroundProcessIntraBatchSleepMs: 50,
    })
    executor.startBackgroundProcesses()
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

    const tasks = { add1 }
    const router = createTasksRouter(os, executor, tasks)

    const client = createRouterClient(router, { context: {} })
    const handles = createTaskClientHandles(client, tasks)

    const executionId = await handles.add1.enqueue({ n: 0 })
    let execution = await handles.add1.getExecution(executionId)
    expect(['ready', 'running']).toContain(execution.status)

    await waitAndGetFinishedExecution(executor, add1, executionId)
    execution = await handles.add1.getExecution(executionId)
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
    const finishedExecution = await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 20 })
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
    const finishedExecution = await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 20 })
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
    const finishedExecution = await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 20 })
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
    const finishedExecution = await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 20 })
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
    const finishedExecution = await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 20 })
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

    vi.spyOn(executor, 'getTaskHandle').mockRejectedValueOnce(
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

    vi.spyOn(executor, 'getTaskHandle').mockRejectedValueOnce(new TypeError('Network error'))

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
    const handles = createTaskClientHandles(client, tasks)

    const executionId = await handles.add1.enqueue({ n: 5 }, { retryOptions: { maxAttempts: 3 } })
    expect(typeof executionId).toBe('string')
    expect(executionId.length).toBeGreaterThan(0)

    const execution = await handles.add1.getExecution(executionId)
    expect(['ready', 'running']).toContain(execution.status)
  })
})

describe('client', () => {
  let storage: InMemoryTaskExecutionsStorage
  let executor: DurableExecutor

  beforeEach(() => {
    storage = new InMemoryTaskExecutionsStorage()
    executor = new DurableExecutor(storage, {
      backgroundProcessIntraBatchSleepMs: 50,
    })
    executor.startBackgroundProcesses()
  })

  afterEach(async () => {
    await executor.shutdown()
  })

  it('should create task client handles with correct types', () => {
    const add1 = executor.task({
      id: 'add1',
      timeoutMs: 5000,
      run: (_, input: { n: number }) => {
        return { n: input.n + 1 }
      },
    })

    const multiply2 = executor.task({
      id: 'multiply2',
      timeoutMs: 5000,
      run: (_, input: { value: number }) => {
        return { result: input.value * 2 }
      },
    })

    const tasks = { add1, multiply2 }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })
    const handles = createTaskClientHandles(client, tasks)

    expect(handles).toHaveProperty('add1')
    expect(handles).toHaveProperty('multiply2')
    expect(typeof handles.add1.enqueue).toBe('function')
    expect(typeof handles.add1.getExecution).toBe('function')
    expect(typeof handles.multiply2.enqueue).toBe('function')
    expect(typeof handles.multiply2.getExecution).toBe('function')
  })

  it('should handle enqueue with client handles', async () => {
    const add1 = executor.task({
      id: 'add1',
      timeoutMs: 5000,
      run: async (_, input: { n: number }) => {
        await sleep(250)
        return { n: input.n + 1 }
      },
    })

    const tasks = { add1 }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })
    const handles = createTaskClientHandles(client, tasks)

    const executionId = await handles.add1.enqueue({ n: 10 })
    expect(typeof executionId).toBe('string')
    expect(executionId.length).toBeGreaterThan(0)

    const execution = await handles.add1.getExecution(executionId)
    expect(['ready', 'running']).toContain(execution.status)

    const finishedExecution = await waitAndGetFinishedExecution(executor, add1, executionId)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.output.n).toBe(11)
  })

  it('should handle multiple different tasks with handles', async () => {
    const add1 = executor.task({
      id: 'add1',
      timeoutMs: 5000,
      run: (_, input: { n: number }) => {
        return { n: input.n + 1 }
      },
    })

    const concat = executor.task({
      id: 'concat',
      timeoutMs: 5000,
      run: (_, input: { a: string; b: string }) => {
        return { result: input.a + input.b }
      },
    })

    const tasks = { add1, concat }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })
    const handles = createTaskClientHandles(client, tasks)

    const addId = await handles.add1.enqueue({ n: 5 })
    const concatId = await handles.concat.enqueue({ a: 'hello', b: 'world' })

    const addFinished = await waitAndGetFinishedExecution(executor, add1, addId)
    const concatFinished = await waitAndGetFinishedExecution(executor, concat, concatId)

    expect(addFinished.status).toBe('completed')
    expect(concatFinished.status).toBe('completed')
    assert(addFinished.status === 'completed')
    assert(concatFinished.status === 'completed')
    expect(addFinished.output.n).toBe(6)
    expect(concatFinished.output.result).toBe('helloworld')
  })

  it('should handle task with undefined input', async () => {
    const noInput = executor.task({
      id: 'noInput',
      timeoutMs: 5000,
      run: () => {
        return { timestamp: Date.now() }
      },
    })

    const tasks = { noInput }
    const router = createTasksRouter(os, executor, tasks)
    const client = createRouterClient(router, { context: {} })
    const handles = createTaskClientHandles(client, tasks)

    const executionId = await handles.noInput.enqueue()
    const execution = await handles.noInput.getExecution(executionId)
    expect(['ready', 'running']).toContain(execution.status)

    const finishedExecution = await waitAndGetFinishedExecution(executor, noInput, executionId)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(typeof finishedExecution.output.timestamp).toBe('number')
  })
})

describe('convertProcedureClientToTask edge cases', () => {
  let storage: InMemoryTaskExecutionsStorage
  let executor: DurableExecutor

  beforeEach(() => {
    storage = new InMemoryTaskExecutionsStorage()
    executor = new DurableExecutor(storage, {
      backgroundProcessIntraBatchSleepMs: 50,
    })
    executor.startBackgroundProcesses()
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
        pollingIntervalMs: 20,
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
        pollingIntervalMs: 20,
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
      pollingIntervalMs: 20,
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
      pollingIntervalMs: 20,
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
      pollingIntervalMs: 20,
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
      pollingIntervalMs: 20,
    })

    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error.isRetryable).toBe(true)
    expect(finishedExecution.error.isInternal).toBe(true)
  })
})
