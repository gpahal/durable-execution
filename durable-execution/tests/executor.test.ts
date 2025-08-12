import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { sleep } from '@gpahal/std/promises'

import { DurableExecutor, type TaskRunContext } from '../src'
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
})
