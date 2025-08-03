import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { sleep } from '@gpahal/std/promises'

import { createCancelSignal, createTimeoutCancelSignal, DurableExecutor } from '../src'
import { InMemoryStorage } from './in-memory-storage'

describe('taskHandle', () => {
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
    if (executor) {
      void executor.shutdown()
    }
  })

  it('should handle correct execution id', async () => {
    let executed = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: async () => {
        executed++
        await sleep(1000)
        return 'test'
      },
    })

    const originalHandle = await executor.enqueueTask(task, undefined)
    const executionId = originalHandle.getTaskExecutionId()
    const handle = executor.getTaskHandle(task, executionId)
    expect(handle.getTaskId()).toBe('test')
    expect(handle.getTaskExecutionId()).toBeDefined()

    await sleep(250)
    const execution = await handle.getTaskExecution()
    expect(executed).toBe(1)
    expect(execution.status).toBe('running')
    assert(execution.status === 'running')
    expect(execution.taskId).toBe('test')
    expect(execution.executionId).toMatch(/^te_/)
    expect(execution.startedAt).toBeInstanceOf(Date)
    expect(execution.expiresAt).toBeInstanceOf(Date)
  })

  it('should handle invalid execution id', () => {
    const task = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: async () => {
        await sleep(1000)
        return 'test'
      },
    })

    const handle = executor.getTaskHandle(task, 'invalid')
    expect(handle.getTaskId()).toBe('test')
    expect(handle.getTaskExecutionId()).toBeDefined()
  })

  it('should handle wait and get finished task execution with cancelled signal', async () => {
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: async () => {
        await sleep(1)
        return 'test'
      },
    })

    const originalHandle = await executor.enqueueTask(task, undefined)
    const executionId = originalHandle.getTaskExecutionId()
    const handle = executor.getTaskHandle(task, executionId)
    expect(handle.getTaskId()).toBe('test')
    expect(handle.getTaskExecutionId()).toBeDefined()

    const [cancelSignal, cancel] = createCancelSignal()
    cancel()
    await expect(handle.waitAndGetTaskFinishedExecution({ signal: cancelSignal })).rejects.toThrow(
      'Task cancelled',
    )
  })

  it('should handle wait and get finished task execution with cancelled abort signal', async () => {
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: async () => {
        await sleep(1)
        return 'test'
      },
    })

    const originalHandle = await executor.enqueueTask(task, undefined)
    const executionId = originalHandle.getTaskExecutionId()
    const handle = executor.getTaskHandle(task, executionId)
    expect(handle.getTaskId()).toBe('test')
    expect(handle.getTaskExecutionId()).toBeDefined()

    const abortController = new AbortController()
    abortController.abort()
    await expect(
      handle.waitAndGetTaskFinishedExecution({ signal: abortController.signal }),
    ).rejects.toThrow('Task cancelled')
  })

  it('should handle wait and get finished task execution with cancel signal after finishing', async () => {
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: async () => {
        await sleep(100)
        return 'test'
      },
    })

    const originalHandle = await executor.enqueueTask(task, undefined)
    const executionId = originalHandle.getTaskExecutionId()
    const handle = executor.getTaskHandle(task, executionId)
    expect(handle.getTaskId()).toBe('test')
    expect(handle.getTaskExecutionId()).toBeDefined()

    const cancelSignal = createTimeoutCancelSignal(10_000)
    await expect(
      handle.waitAndGetTaskFinishedExecution({ signal: cancelSignal }),
    ).resolves.toBeDefined()
  })

  it('should handle wait and get finished task execution with cancel signal before finishing', async () => {
    const task = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: async () => {
        await sleep(1000)
        return 'test'
      },
    })

    const originalHandle = await executor.enqueueTask(task, undefined)
    const executionId = originalHandle.getTaskExecutionId()
    const handle = executor.getTaskHandle(task, executionId)
    expect(handle.getTaskId()).toBe('test')
    expect(handle.getTaskExecutionId()).toBeDefined()

    const cancelSignal = createTimeoutCancelSignal(1000)
    await expect(handle.waitAndGetTaskFinishedExecution({ signal: cancelSignal })).rejects.toThrow(
      'Task cancelled',
    )
  })
})
