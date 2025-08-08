import { ORPCError } from '@orpc/contract'
import { createRouterClient, type as orpcType, os } from '@orpc/server'
import {
  DurableExecutor,
  InMemoryStorage,
  type DurableTask,
  type DurableTaskFinishedExecution,
} from 'durable-execution'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { sleep } from '@gpahal/std/promises'

import {
  createDurableTaskORPCHandles,
  createDurableTaskORPCRouter,
  procedureClientTask,
} from '../src'

async function waitAndGetFinishedExecution<TInput, TOutput>(
  executor: DurableExecutor,
  task: DurableTask<TInput, TOutput>,
  executionId: string,
): Promise<DurableTaskFinishedExecution<TOutput>> {
  const handle = await executor.getTaskHandle(task, executionId)
  return await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 20 })
}

describe('index', () => {
  let storage: InMemoryStorage
  let executor: DurableExecutor

  beforeEach(() => {
    storage = new InMemoryStorage({ enableDebug: false })
    executor = new DurableExecutor(storage, {
      enableDebug: false,
      backgroundProcessIntraBatchSleepMs: 50,
    })
    void executor.start()
  })

  afterEach(async () => {
    await executor.shutdown()
  })

  it('should enqueue and get execution with server-side client', async () => {
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

    const router = createDurableTaskORPCRouter(os, executor)
    const client = createRouterClient(router, { context: {} })
    const handles = createDurableTaskORPCHandles(client, tasks)

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
    const router = createDurableTaskORPCRouter(os, executor)
    const client = createRouterClient(router, { context: {} })

    await expect(client.enqueueTask({ taskId: 'invalid', input: { n: 0 } })).rejects.toThrow(
      'Task invalid not found',
    )
  })

  it('should handle invalid task id to get execution', async () => {
    const router = createDurableTaskORPCRouter(os, executor)
    const client = createRouterClient(router, { context: {} })

    await expect(
      client.getTaskExecution({ taskId: 'invalid', executionId: 'invalid' }),
    ).rejects.toThrow('Task invalid not found')
  })

  it('should handle invalid execution id to get execution', async () => {
    executor.task({
      id: 'add1',
      timeoutMs: 5000,
      run: (_, input: { n: number }) => {
        return { n: input.n + 1 }
      },
    })

    const router = createDurableTaskORPCRouter(os, executor)
    const client = createRouterClient(router, { context: {} })

    await expect(
      client.getTaskExecution({ taskId: 'add1', executionId: 'invalid' }),
    ).rejects.toThrow('Execution invalid not found')
  })

  it('should complete procedureClientTask', async () => {
    let executionCount = 0
    const add1Proc = os
      .input(orpcType<{ n: number }>())
      .output(orpcType<{ n: number }>())
      .handler(({ input }: { input: { n: number } }) => {
        executionCount++
        return { n: input.n + 1 }
      })

    const router = { add1: add1Proc }
    const client = createRouterClient(router, { context: {} })

    const add1 = procedureClientTask(executor, { id: 'add1', timeoutMs: 1000 }, client.add1)

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
      .input(orpcType<{ n: number }>())
      .output(orpcType<{ n: number }>())
      .handler(() => {
        executionCount++
        throw new ORPCError('BAD_REQUEST', { message: 'invalid' })
      })

    const router = { add1: add1Proc }
    const client = createRouterClient(router, { context: {} })

    const add1 = procedureClientTask(executor, { id: 'add1', timeoutMs: 1000 }, client.add1)

    const handle = await executor.enqueueTask(add1, { n: 0 })
    const finishedExecution = await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 20 })
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error.errorType).toBe('generic')
    expect(finishedExecution.error.isInternal).toBe(false)
    expect(finishedExecution.error.isRetryable).toBe(false)
    expect(finishedExecution.error.message).toBe('invalid')
  })

  it('should handle procedureClientTask not found error', async () => {
    let executionCount = 0
    const add1Proc = os
      .input(orpcType<{ n: number }>())
      .output(orpcType<{ n: number }>())
      .handler(() => {
        executionCount++
        throw new ORPCError('NOT_FOUND', { message: 'missing' })
      })

    const router = { add1: add1Proc }
    const client = createRouterClient(router, { context: {} })

    const add1 = procedureClientTask(executor, { id: 'add1', timeoutMs: 1000 }, client.add1)

    const handle = await executor.enqueueTask(add1, { n: 0 })
    const finishedExecution = await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 20 })
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error.errorType).toBe('not_found')
    expect(finishedExecution.error.isInternal).toBe(false)
    expect(finishedExecution.error.isRetryable).toBe(false)
    expect(finishedExecution.error.message).toBe('missing')
  })

  it('should handle procedureClientTask internal server error', async () => {
    let executionCount = 0
    const add1Proc = os
      .input(orpcType<{ n: number }>())
      .output(orpcType<{ n: number }>())
      .handler(() => {
        executionCount++
        throw new ORPCError('INTERNAL_SERVER_ERROR', { message: 'server down' })
      })

    const router = { add1: add1Proc }
    const client = createRouterClient(router, { context: {} })

    const add1 = procedureClientTask(executor, { id: 'add1', timeoutMs: 1000 }, client.add1)

    const handle = await executor.enqueueTask(add1, { n: 0 })
    const finishedExecution = await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 20 })
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error.errorType).toBe('generic')
    expect(finishedExecution.error.isInternal).toBe(true)
    expect(finishedExecution.error.isRetryable).toBe(false)
    expect(finishedExecution.error.message).toBe('server down')
  })

  it('should handle procedureClientTask custom error', async () => {
    let executionCount = 0
    const add1Proc = os
      .input(orpcType<{ n: number }>())
      .output(orpcType<{ n: number }>())
      .handler(() => {
        executionCount++
        throw new Error('boom')
      })

    const router = { add1: add1Proc }
    const client = createRouterClient(router, { context: {} })

    const add1 = procedureClientTask(executor, { id: 'add1', timeoutMs: 1000 }, client.add1)

    const handle = await executor.enqueueTask(add1, { n: 0 })
    const finishedExecution = await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 20 })
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('failed')
    assert(finishedExecution.status === 'failed')
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error.errorType).toBe('generic')
    expect(finishedExecution.error.isInternal).toBe(true)
    expect(finishedExecution.error.isRetryable).toBe(false)
    expect(finishedExecution.error.message).toBe('Internal server error')
  })
})
