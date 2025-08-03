import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { sleep } from '@gpahal/std/promises'

import { DurableExecutor } from '../src'
import { InMemoryStorage } from './in-memory-storage'

describe('parentTask', () => {
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

    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executed++
        await sleep(1)
        return {
          output: 'test_output',
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

    const handle = await executor.enqueueTask(task, undefined)

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(executed).toBe(3)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.output).toBe('test_output')
    expect(finishedExecution.output.childrenOutputs).toHaveLength(2)
    expect(finishedExecution.output.childrenOutputs[0]!.index).toBe(0)
    expect(finishedExecution.output.childrenOutputs[0]!.taskId).toBe('child1')
    expect(finishedExecution.output.childrenOutputs[0]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.output.childrenOutputs[0]!.output).toBe('child1_output')
    expect(finishedExecution.output.childrenOutputs[1]!.index).toBe(1)
    expect(finishedExecution.output.childrenOutputs[1]!.taskId).toBe('child2')
    expect(finishedExecution.output.childrenOutputs[1]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.output.childrenOutputs[1]!.output).toBe('child2_output')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with no children', async () => {
    let executed = 0
    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executed++
        await sleep(1)
        return {
          output: 'test_output',
          children: [],
        }
      },
    })

    const handle = await executor.enqueueTask(task, undefined)

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.output).toBe('test_output')
    expect(finishedExecution.output.childrenOutputs).toHaveLength(0)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with onRunAndChildrenComplete', async () => {
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

    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executed++
        await sleep(1)
        return {
          output: 'test_output',
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
      onRunAndChildrenComplete: {
        id: 'onRunAndChildrenComplete',
        timeoutMs: 1000,
        run: async (_, { output, childrenOutputs }) => {
          executed++
          await sleep(1)
          return `${output}_${childrenOutputs.map((c) => c.output).join('_')}`
        },
      },
    })

    const handle = await executor.enqueueTask(task, undefined)

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(executed).toBe(4)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBe('test_output_child1_output_child2_output')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with onRunAndChildrenComplete and no children', async () => {
    let executed = 0
    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executed++
        await sleep(1)
        return {
          output: 'test_output',
          children: [],
        }
      },
      onRunAndChildrenComplete: {
        id: 'onRunAndChildrenComplete',
        timeoutMs: 1000,
        run: async (_, { output, childrenOutputs }) => {
          executed++
          await sleep(1)
          return `${output}_${childrenOutputs.map((c) => c.output).join('_')}`
        },
      },
    })

    const handle = await executor.enqueueTask(task, undefined)

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(executed).toBe(2)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBe('test_output_')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail with one child failing', async () => {
    let executed = 0
    const child1 = executor.task({
      id: 'child1',
      timeoutMs: 1000,
      run: async () => {
        executed++
        await sleep(1)
        throw new Error('child1 error')
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

    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executed++
        await sleep(1)
        return {
          output: 'test_output',
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
      onRunAndChildrenComplete: {
        id: 'onRunAndChildrenComplete',
        timeoutMs: 1000,
        run: async (_, { output, childrenOutputs }) => {
          executed++
          await sleep(1)
          return `${output}_${childrenOutputs.map((c) => c.output).join('_')}`
        },
      },
    })

    const handle = await executor.enqueueTask(task, undefined)

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(executed).toBe(3)
    expect(finishedExecution.status).toBe('children_failed')
    assert(finishedExecution.status === 'children_failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.childrenErrors).toBeDefined()
    expect(finishedExecution.childrenErrors).toHaveLength(1)
    expect(finishedExecution.childrenErrors[0]!.index).toBe(0)
    expect(finishedExecution.childrenErrors[0]!.taskId).toBe('child1')
    expect(finishedExecution.childrenErrors[0]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.childrenErrors[0]!.error?.message).toBe('child1 error')
    expect(finishedExecution.childrenErrors[0]!.error?.tag).toBe('DurableTaskError')
    expect(finishedExecution.childrenErrors[0]!.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail with multiple children failing', async () => {
    let executed = 0
    const child1 = executor.task({
      id: 'child1',
      timeoutMs: 1000,
      run: async () => {
        executed++
        await sleep(1)
        throw new Error('child1 error')
      },
    })
    const child2 = executor.task({
      id: 'child2',
      timeoutMs: 1000,
      run: async () => {
        executed++
        await sleep(1)
        throw new Error('child2 error')
      },
    })

    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executed++
        await sleep(1)
        return {
          output: 'test_output',
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
      onRunAndChildrenComplete: {
        id: 'onRunAndChildrenComplete',
        timeoutMs: 1000,
        run: async (_, { output, childrenOutputs }) => {
          executed++
          await sleep(1)
          return `${output}_${childrenOutputs.map((c) => c.output).join('_')}`
        },
      },
    })

    const handle = await executor.enqueueTask(task, undefined)

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(executed).toBe(3)
    expect(finishedExecution.status).toBe('children_failed')
    assert(finishedExecution.status === 'children_failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.childrenErrors).toBeDefined()
    expect(finishedExecution.childrenErrors).toHaveLength(2)
    finishedExecution.childrenErrors.sort((a, b) => a.index - b.index)
    expect(finishedExecution.childrenErrors[0]!.index).toBe(0)
    expect(finishedExecution.childrenErrors[0]!.taskId).toBe('child1')
    expect(finishedExecution.childrenErrors[0]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.childrenErrors[0]!.error?.message).toBe('child1 error')
    expect(finishedExecution.childrenErrors[0]!.error?.tag).toBe('DurableTaskError')
    expect(finishedExecution.childrenErrors[0]!.error?.isRetryable).toBe(true)
    expect(finishedExecution.childrenErrors[1]!.index).toBe(1)
    expect(finishedExecution.childrenErrors[1]!.taskId).toBe('child2')
    expect(finishedExecution.childrenErrors[1]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.childrenErrors[1]!.error?.message).toBe('child2 error')
    expect(finishedExecution.childrenErrors[1]!.error?.tag).toBe('DurableTaskError')
    expect(finishedExecution.childrenErrors[1]!.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail with onRunAndChildrenComplete failing', async () => {
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

    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executed++
        await sleep(1)
        return {
          output: 'test_output',
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
      onRunAndChildrenComplete: {
        id: 'onRunAndChildrenComplete',
        timeoutMs: 1000,
        run: async () => {
          executed++
          await sleep(1)
          throw new Error('onRunAndChildrenComplete error')
        },
      },
    })

    const handle = await executor.enqueueTask(task, undefined)

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(executed).toBe(4)
    expect(finishedExecution.status).toBe('on_run_and_children_complete_failed')
    assert(finishedExecution.status === 'on_run_and_children_complete_failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.onRunAndChildrenCompleteError).toBeDefined()
    expect(finishedExecution.onRunAndChildrenCompleteError?.message).toBe(
      'onRunAndChildrenComplete error',
    )
    expect(finishedExecution.onRunAndChildrenCompleteError?.tag).toBe('DurableTaskError')
    expect(finishedExecution.onRunAndChildrenCompleteError?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })
})
