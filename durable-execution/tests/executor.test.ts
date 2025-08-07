import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { sleep } from '@gpahal/std/promises'

import { DurableExecutionError, DurableExecutor, type DurableTaskRunContext } from '../src'
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
    void executor.start()
  })

  afterEach(() => {
    if (executor) {
      void executor.shutdown()
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
    ).toThrow('Executor shutdown')
  })

  it('should handle executor shutdown with running task', { timeout: 10_000 }, async () => {
    let executed = 0
    const taskOptions = {
      id: 'test',
      timeoutMs: 10_000,
      run: async (ctx: DurableTaskRunContext) => {
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

    const handle = await executor.enqueueTask(task, undefined)
    let execution = await handle.getTaskExecution()
    expect(['ready', 'running']).toContain(execution.status)

    await sleep(250)

    execution = await handle.getTaskExecution()
    expect(execution.status).toBe('running')
    expect(executed).toBe(1)
    await executor.shutdown()

    execution = await handle.getTaskExecution()
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
    void executor.start()

    await expect(executor.enqueueTask(task, undefined)).rejects.toThrow('Task test not found')
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

  it('should retry withTransaction', async () => {
    const originalWithTransaction = storage.withTransaction
    let executed = 0
    storage.withTransaction = () => {
      executed++
      throw new Error('Mocked withTransaction error')
    }

    try {
      const task = executor.task({
        id: 'test',
        timeoutMs: 10_000,
        run: async () => {
          await sleep(1000)
          return 'test'
        },
      })

      await expect(executor.enqueueTask(task, undefined)).rejects.toThrow(
        'Mocked withTransaction error',
      )
      expect(executed).toBeGreaterThanOrEqual(2)
    } finally {
      storage.withTransaction = originalWithTransaction
    }
  })

  it('should not retry withTransaction', async () => {
    const originalWithTransaction = storage.withTransaction
    let executed = 0
    storage.withTransaction = () => {
      executed++
      throw new DurableExecutionError('Mocked withTransaction error', false)
    }

    try {
      const task = executor.task({
        id: 'test',
        timeoutMs: 10_000,
        run: async () => {
          await sleep(1000)
          return 'test'
        },
      })

      await expect(executor.enqueueTask(task, undefined)).rejects.toThrow(
        'Mocked withTransaction error',
      )
      expect(executed).toBe(1)
    } finally {
      storage.withTransaction = originalWithTransaction
    }
  })
})
