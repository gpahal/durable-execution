import * as v from 'valibot'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { sleep } from '@gpahal/std/promises'

import { DurableExecutor } from '../src'
import { InMemoryStorage } from './in-memory-storage'

describe('parentSchemaTask', () => {
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
    const child1 = executor.task({
      id: 'child1',
      timeoutMs: 1000,
      run: async () => {
        executed++
        await sleep(1)
        return 'child1_output'
      },
    })
    const child2 = executor.task({
      id: 'child2',
      timeoutMs: 1000,
      run: async () => {
        executed++
        await sleep(1)
        return 'child2_output'
      },
    })

    const task = executor.parentSchemaTask({
      id: 'test',
      timeoutMs: 1000,
      inputSchema: v.object({
        name: v.string(),
      }),
      runParent: async (_, input) => {
        executed++
        await sleep(1)
        return {
          output: input.name,
          children: [
            {
              task: child1,
              input: undefined,
            },
            {
              task: child2,
              input: undefined,
            },
          ],
        }
      },
    })

    const handle = await executor.enqueueTask(task, {
      name: 'test_output',
    })

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(executed).toBe(3)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.output).toBe('test_output')
    expect(finishedExecution.output.childrenOutputs).toHaveLength(2)
    expect(finishedExecution.output.childrenOutputs[0]!.taskId).toBe('child1')
    expect(finishedExecution.output.childrenOutputs[0]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.output.childrenOutputs[0]!.output).toBe('child1_output')
    expect(finishedExecution.output.childrenOutputs[1]!.taskId).toBe('child2')
    expect(finishedExecution.output.childrenOutputs[1]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.output.childrenOutputs[1]!.output).toBe('child2_output')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail', async () => {
    let executed = 0
    const child1 = executor.task({
      id: 'child1',
      timeoutMs: 1000,
      run: async () => {
        executed++
        await sleep(1)
        return 'child1_output'
      },
    })
    const child2 = executor.task({
      id: 'child2',
      timeoutMs: 1000,
      run: async () => {
        executed++
        await sleep(1)
        return 'child2_output'
      },
    })

    const task = executor.parentSchemaTask({
      id: 'test',
      timeoutMs: 1000,
      inputSchema: v.object({
        name: v.string(),
      }),
      runParent: async (_, input) => {
        executed++
        await sleep(1)
        return {
          output: input.name,
          children: [
            {
              task: child1,
              input: undefined,
            },
            {
              task: child2,
              input: undefined,
            },
          ],
        }
      },
    })

    // @ts-expect-error - Testing invalid input
    await expect(executor.enqueueTask(task, { name: 0 })).rejects.toThrow(
      'Invalid input for schema task test',
    )
    expect(executed).toBe(0)
  })
})
