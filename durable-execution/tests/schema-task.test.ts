import * as v from 'valibot'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { sleep } from '@gpahal/std/promises'

import { DurableExecutor } from '../src'
import { InMemoryStorage } from './in-memory-storage'

describe('schemaTask', () => {
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
    const task = executor.schemaTask({
      id: 'test',
      timeoutMs: 1000,
      inputSchema: v.object({
        name: v.string(),
      }),
      run: async (_, input) => {
        executed++
        await sleep(1)
        return input.name
      },
    })

    const handle = await executor.enqueueTask(task, { name: 'test' })

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
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

  it('should fail', async () => {
    let executed = 0
    const task = executor.schemaTask({
      id: 'test',
      timeoutMs: 1000,
      inputSchema: v.object({
        name: v.string(),
      }),
      run: async (_, input) => {
        executed++
        await sleep(1)
        return input.name
      },
    })

    // @ts-expect-error - Testing invalid input
    await expect(executor.enqueueTask(task, { name: 0 })).rejects.toThrow(
      'Invalid input for schema task test',
    )
    expect(executed).toBe(0)
  })
})
