import { Schema } from 'effect'

import { sleep } from '@gpahal/std/promises'

import {
  childTask,
  DurableExecutionError,
  DurableExecutor,
  InMemoryTaskExecutionsStorage,
} from '../src'

describe('parentTask', () => {
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

  it('should complete', async () => {
    let executionCount = 0
    const child1 = executor.task({
      id: 'child1',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'child1_output'
      },
    })
    const child2 = executor.task({
      id: 'child2',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'child2_output'
      },
    })

    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executionCount++
        await sleep(1)
        return {
          output: 'test_output',
          children: [childTask(child1), childTask(child2)],
        }
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(executionCount).toBe(3)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.output).toBe('test_output')
    expect(finishedExecution.output.children).toHaveLength(2)
    expect(finishedExecution.output.children[0]!.taskId).toBe('child1')
    expect(finishedExecution.output.children[0]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.output.children[0]!.status).toBe('completed')
    assert(finishedExecution.output.children[0]!.status === 'completed')
    expect(finishedExecution.output.children[0]!.output).toBe('child1_output')
    expect(finishedExecution.output.children[1]!.taskId).toBe('child2')
    expect(finishedExecution.output.children[1]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.output.children[1]!.status).toBe('completed')
    assert(finishedExecution.output.children[1]!.status === 'completed')
    expect(finishedExecution.output.children[1]!.output).toBe('child2_output')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.waitingForChildrenStartedAt).toBeInstanceOf(Date)
    expect(finishedExecution.waitingForFinalizeStartedAt).toBeUndefined()
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with no children', async () => {
    let executionCount = 0
    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executionCount++
        await sleep(1)
        return {
          output: 'test_output',
          children: [],
        }
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.output).toBe('test_output')
    expect(finishedExecution.output.children).toHaveLength(0)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.waitingForChildrenStartedAt).toBeUndefined()
    expect(finishedExecution.waitingForFinalizeStartedAt).toBeUndefined()
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with finalize function', async () => {
    let executionCount = 0
    const child1 = executor.task({
      id: 'child1',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'child1_output'
      },
    })
    const child2 = executor.task({
      id: 'child2',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'child2_output'
      },
    })

    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executionCount++
        await sleep(1)
        return {
          output: 'test_output',
          children: [childTask(child1), childTask(child2)],
        }
      },
      finalize: async ({ output, children }) => {
        executionCount++
        await sleep(1)
        return `${output}_${children.map((c) => (c.status === 'completed' ? c.output : '')).join('_')}`
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(executionCount).toBe(4)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBe('test_output_child1_output_child2_output')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.waitingForChildrenStartedAt).toBeInstanceOf(Date)
    expect(finishedExecution.waitingForFinalizeStartedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with finalize task', async () => {
    let executionCount = 0
    const child1 = executor.task({
      id: 'child1',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'child1_output'
      },
    })
    const child2 = executor.task({
      id: 'child2',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'child2_output'
      },
    })

    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executionCount++
        await sleep(1)
        return {
          output: 'test_output',
          children: [childTask(child1), childTask(child2)],
        }
      },
      finalize: {
        id: 'finalizeTask',
        timeoutMs: 1000,
        run: async (_, { output, children }) => {
          executionCount++
          await sleep(1)
          return `${output}_${children.map((c) => (c.status === 'completed' ? c.output : '')).join('_')}`
        },
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(executionCount).toBe(4)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBe('test_output_child1_output_child2_output')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.waitingForChildrenStartedAt).toBeInstanceOf(Date)
    expect(finishedExecution.waitingForFinalizeStartedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with finalize function and no children', async () => {
    let executionCount = 0
    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executionCount++
        await sleep(1)
        return {
          output: 'test_output',
        }
      },
      finalize: async ({ output, children }) => {
        executionCount++
        await sleep(1)
        return `${output}_${children.map((c) => (c.status === 'completed' ? c.output : '')).join('_')}`
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(executionCount).toBe(2)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBe('test_output_')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.waitingForChildrenStartedAt).toBeUndefined()
    expect(finishedExecution.waitingForFinalizeStartedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with finalize task and no children', async () => {
    let executionCount = 0
    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executionCount++
        await sleep(1)
        return {
          output: 'test_output',
        }
      },
      finalize: {
        id: 'finalizeTask',
        timeoutMs: 1000,
        run: async (_, { output, children }) => {
          executionCount++
          await sleep(1)
          return `${output}_${children.map((c) => (c.status === 'completed' ? c.output : '')).join('_')}`
        },
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(executionCount).toBe(2)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBe('test_output_')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.waitingForChildrenStartedAt).toBeInstanceOf(Date)
    expect(finishedExecution.waitingForFinalizeStartedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with input schema', async () => {
    let executionCount = 0
    const child1 = executor.task({
      id: 'child1',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'child1_output'
      },
    })
    const child2 = executor.task({
      id: 'child2',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'child2_output'
      },
    })

    const task = executor
      .inputSchema(
        Schema.Struct({
          name: Schema.String,
        }),
      )
      .parentTask({
        id: 'test',
        timeoutMs: 1000,
        runParent: async (_, input) => {
          executionCount++
          await sleep(1)
          return {
            output: input.name,
            children: [childTask(child1), childTask(child2)],
          }
        },
      })

    const handle = await executor.enqueueTask(task, {
      name: 'test_output',
    })

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(executionCount).toBe(3)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.output).toBe('test_output')
    expect(finishedExecution.output.children).toHaveLength(2)
    expect(finishedExecution.output.children[0]!.taskId).toBe('child1')
    expect(finishedExecution.output.children[0]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.output.children[0]!.status).toBe('completed')
    assert(finishedExecution.output.children[0]!.status === 'completed')
    expect(finishedExecution.output.children[0]!.output).toBe('child1_output')
    expect(finishedExecution.output.children[1]!.taskId).toBe('child2')
    expect(finishedExecution.output.children[1]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.output.children[1]!.status).toBe('completed')
    assert(finishedExecution.output.children[1]!.status === 'completed')
    expect(finishedExecution.output.children[1]!.output).toBe('child2_output')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail with input schema', async () => {
    let executionCount = 0
    const child1 = executor.task({
      id: 'child1',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'child1_output'
      },
    })
    const child2 = executor.task({
      id: 'child2',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'child2_output'
      },
    })

    const task = executor
      .inputSchema(
        Schema.Struct({
          name: Schema.String,
          version: Schema.Int,
        }),
      )
      .parentTask({
        id: 'test',
        timeoutMs: 1000,
        runParent: async (_, input) => {
          executionCount++
          await sleep(1)
          return {
            output: input.name,
            children: [childTask(child1), childTask(child2)],
          }
        },
      })

    // @ts-expect-error - Testing invalid input
    const handle = await executor.enqueueTask(task, { name: 0 })

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
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

  it('should fail with one child failing', async () => {
    let executionCount = 0
    const child1 = executor.task({
      id: 'child1',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        throw new Error('child1 error')
      },
    })
    const child2 = executor.task({
      id: 'child2',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'child2_output'
      },
    })

    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executionCount++
        await sleep(1)
        return {
          output: 'test_output',
          children: [childTask(child1), childTask(child2)],
        }
      },
      finalize: async ({ output, children }) => {
        executionCount++
        if (children.some((c) => c.status === 'failed')) {
          throw DurableExecutionError.nonRetryable('finalizeTask error')
        }

        executionCount++
        await sleep(1)
        return `${output}_${children.map((c) => (c.status === 'completed' ? c.output : '')).join('_')}`
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(executionCount).toBe(4)
    expect(finishedExecution.status).toBe('finalize_failed')
    assert(finishedExecution.status === 'finalize_failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toContain('finalizeTask error')
    expect(finishedExecution.error?.errorType).toBe('generic')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail with multiple children failing', async () => {
    let executionCount = 0
    const child1 = executor.task({
      id: 'child1',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        throw new Error('child1 error')
      },
    })
    const child2 = executor.task({
      id: 'child2',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        throw new Error('child2 error')
      },
    })

    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executionCount++
        await sleep(1)
        return {
          output: 'test_output',
          children: [childTask(child1), childTask(child2)],
        }
      },
      finalize: async ({ output, children }) => {
        executionCount++
        if (children.some((c) => c.status === 'failed')) {
          throw DurableExecutionError.nonRetryable('finalizeTask error')
        }

        executionCount++
        await sleep(1)
        return `${output}_${children.map((c) => (c.status === 'completed' ? c.output : '')).join('_')}`
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(executionCount).toBe(4)
    expect(finishedExecution.status).toBe('finalize_failed')
    assert(finishedExecution.status === 'finalize_failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toContain('finalizeTask error')
    expect(finishedExecution.error?.errorType).toBe('generic')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail with finalizeTask failing', async () => {
    let executionCount = 0
    const child1 = executor.task({
      id: 'child1',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'child1_output'
      },
    })
    const child2 = executor.task({
      id: 'child2',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'child2_output'
      },
    })

    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executionCount++
        await sleep(1)
        return {
          output: 'test_output',
          children: [childTask(child1), childTask(child2)],
        }
      },
      finalize: {
        id: 'finalizeTask',
        timeoutMs: 1000,
        run: async () => {
          executionCount++
          await sleep(1)
          throw new Error('finalizeTask error')
        },
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(executionCount).toBe(4)
    expect(finishedExecution.status).toBe('finalize_failed')
    assert(finishedExecution.status === 'finalize_failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toContain('finalizeTask error')
    expect(finishedExecution.error?.errorType).toBe('generic')
    expect(finishedExecution.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with finalizeTask parent task', async () => {
    let executionCount = 0
    const child1 = executor.task({
      id: 'child1',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'child1_output'
      },
    })
    const child2 = executor.task({
      id: 'child2',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'child2_output'
      },
    })

    const finalizeTaskChild1 = executor.task({
      id: 'finalizeTaskChild1',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'finalizeTaskChild1_output'
      },
    })
    const finalizeTaskChild2 = executor.task({
      id: 'finalizeTaskChild2',
      timeoutMs: 1000,
      run: async () => {
        executionCount++
        await sleep(1)
        return 'finalizeTaskChild2_output'
      },
    })

    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executionCount++
        await sleep(1)
        return {
          output: 'test_output',
          children: [childTask(child1), childTask(child2)],
        }
      },
      finalize: {
        id: 'finalizeTask',
        timeoutMs: 1000,
        runParent: async (_, { output, children }) => {
          executionCount++
          await sleep(1)
          return {
            output: `${output}_${children.map((c) => (c.status === 'completed' ? c.output : '')).join('_')}`,
            children: [childTask(finalizeTaskChild1), childTask(finalizeTaskChild2)],
          }
        },
        finalize: {
          id: 'finalizeTaskNested',
          timeoutMs: 1000,
          run: async (_, { output, children }) => {
            executionCount++
            await sleep(1)
            return `${output}_${children.map((c) => (c.status === 'completed' ? c.output : '')).join('_')}`
          },
        },
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(executionCount).toBe(7)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBe(
      'test_output_child1_output_child2_output_finalizeTaskChild1_output_finalizeTaskChild2_output',
    )
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should handle parent task cancelled while children are running', async () => {
    let childStarted = false
    let childCompleted = false

    const slowChildTask = executor.task({
      id: 'slow_child',
      timeoutMs: 10_000,
      run: async (ctx) => {
        childStarted = true
        for (let i = 0; i < 100; i++) {
          if (ctx.shutdownSignal.isCancelled()) {
            return 'cancelled_by_parent'
          }
          await sleep(50)
        }
        childCompleted = true
        return 'completed'
      },
    })

    const parentTask = executor.parentTask({
      id: 'parent_cancel',
      timeoutMs: 10_000,
      runParent: () => {
        return {
          output: undefined,
          children: [childTask(slowChildTask)],
        }
      },
    })

    const handle = await executor.enqueueTask(parentTask)

    while (!childStarted) {
      await sleep(50)
    }

    await handle.cancel()

    const execution = await handle.getExecution()
    expect(execution.status).toBe('cancelled')
    expect(childCompleted).toBe(false)
  })

  it('should handle grandparent-parent-child task hierarchy with failures', async () => {
    let grandchildExecuted = false

    const grandchildTask = executor.task({
      id: 'grandchild',
      timeoutMs: 1000,
      run: () => {
        grandchildExecuted = true
        throw new Error('Grandchild failed')
      },
    })

    const childParentTask = executor.parentTask({
      id: 'child_parent',
      timeoutMs: 2000,
      runParent: () => {
        return {
          output: undefined,
          children: [childTask(grandchildTask)],
        }
      },
      finalize: {
        id: 'finalizeTask1',
        timeoutMs: 1000,
        run: (ctx, { children }) => {
          if (children.some((c) => c.status !== 'completed')) {
            throw new Error('Grandchild failed')
          }
        },
      },
    })

    const grandparentTask = executor.parentTask({
      id: 'grandparent',
      timeoutMs: 5000,
      runParent: () => {
        return {
          output: undefined,
          children: [childTask(childParentTask)],
        }
      },
      finalize: {
        id: 'finalizeTask2',
        timeoutMs: 1000,
        run: (ctx, { children }) => {
          if (children.some((c) => c.status !== 'completed')) {
            throw new Error('Child parent failed')
          }
        },
      },
    })

    const handle = await executor.enqueueTask(grandparentTask)
    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })

    expect(grandchildExecuted).toBe(true)
    expect(finishedExecution.status).toBe('finalize_failed')
    assert(finishedExecution.status === 'finalize_failed')
    expect(finishedExecution.error?.message).toContain('Child parent failed')
  })

  it('should handle parent task with children finalization timeout', async () => {
    const child = executor.task({
      id: 'timeout_child',
      timeoutMs: 100,
      run: async () => {
        await sleep(500)
        return 'should_not_complete'
      },
    })

    const parentTask = executor.parentTask({
      id: 'parent_child_timeout',
      timeoutMs: 5000,
      runParent: () => {
        return {
          output: undefined,
          children: [childTask(child)],
        }
      },
      finalize: {
        id: 'finalizeTask',
        timeoutMs: 1000,
        run: (ctx, { children }) => {
          if (children.some((c) => c.status === 'timed_out')) {
            throw new Error('Finalize task failed')
          }
        },
      },
    })

    const handle = await executor.enqueueTask(parentTask)
    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })

    expect(finishedExecution.status).toBe('finalize_failed')
    assert(finishedExecution.status === 'finalize_failed')
    expect(finishedExecution.error?.message).toContain('Finalize task failed')
  })

  it('should complete child sleeping task', async () => {
    const task = executor.sleepingTask<string>({
      id: 'test',
      timeoutMs: 1000,
    })

    const parentTask = executor.parentTask({
      id: 'parent',
      timeoutMs: 1000,
      runParent: () => {
        return { output: 'parent_output', children: [childTask(task, 'test_unique_id')] }
      },
      finalize: {
        id: 'finalizeTask',
        timeoutMs: 1000,
        run: (ctx, { children }) => {
          const child = children[0]!
          if (child.status !== 'completed') {
            throw new Error('Finalize task failed')
          }
          return child.output
        },
      },
    })

    const handle = await executor.enqueueTask(parentTask)
    while (true) {
      const execution = await handle.getExecution()
      if (execution.status === 'waiting_for_children') {
        break
      }
      await sleep(100)
    }

    await sleep(100)
    const childExecution = await executor.wakeupSleepingTaskExecution(task, 'test_unique_id', {
      status: 'completed',
      output: 'test_output',
    })
    expect(childExecution.status).toBe('completed')
    assert(childExecution.status === 'completed')
    expect(childExecution.output).toBe('test_output')

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.output).toBe('test_output')
  })

  it('should fail child sleeping task', async () => {
    const task = executor.sleepingTask<string>({
      id: 'test',
      timeoutMs: 1000,
    })

    const parentTask = executor.parentTask({
      id: 'parent',
      timeoutMs: 1000,
      runParent: () => {
        return { output: 'parent_output', children: [childTask(task, 'test_unique_id')] }
      },
      finalize: {
        id: 'finalizeTask',
        timeoutMs: 1000,
        run: (ctx, { children }) => {
          const child = children[0]!
          if (child.status !== 'completed') {
            throw new Error('Finalize task failed')
          }
          return child.output
        },
      },
    })

    const handle = await executor.enqueueTask(parentTask)
    while (true) {
      const execution = await handle.getExecution()
      if (execution.status === 'waiting_for_children') {
        break
      }
      await sleep(100)
    }

    await sleep(100)
    const childExecution = await executor.wakeupSleepingTaskExecution(task, 'test_unique_id', {
      status: 'failed',
      error: new Error('test_error'),
    })
    expect(childExecution.status).toBe('failed')
    assert(childExecution.status === 'failed')
    expect(childExecution.error?.message).toBe('test_error')

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(finishedExecution.status).toBe('finalize_failed')
    assert(finishedExecution.status === 'finalize_failed')
    expect(finishedExecution.error?.message).toBe('Finalize task failed')
  })

  it('should handle parent task closure', async () => {
    let executionCount = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 1000,
      run: () => {
        executionCount++
        return 'test'
      },
    })

    const parentTask = executor.parentTask({
      id: 'parent',
      timeoutMs: 1000,
      runParent: () => {
        return { output: 'parent_output', children: [childTask(task)] }
      },
    })

    const handle = await executor.enqueueTask(parentTask)

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(executionCount).toBe(1)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.output.output).toBe('parent_output')

    while (true) {
      const executionStorageValues = await storage.getManyById([
        { executionId: handle.getExecutionId(), filters: {} },
      ])
      expect(executionStorageValues).toBeDefined()
      assert(executionStorageValues)
      expect(executionStorageValues.length).toBe(1)

      const executionStorageValue = executionStorageValues[0]!
      expect(executionStorageValue).toBeDefined()
      assert(executionStorageValue)
      expect(executionStorageValue.status).toBe('completed')
      expect(executionStorageValue.onChildrenFinishedProcessingStatus).toBe('processed')
      expect(executionStorageValue.onChildrenFinishedProcessingFinishedAt).toBeDefined()
      assert(executionStorageValue.onChildrenFinishedProcessingFinishedAt)
      expect(executionStorageValue.onChildrenFinishedProcessingFinishedAt).toBeGreaterThan(0)

      if (executionStorageValue.closeStatus === 'closed') {
        expect(executionStorageValue.closedAt).toBeDefined()
        expect(executionStorageValue.closedAt).toBeGreaterThan(
          executionStorageValue.onChildrenFinishedProcessingFinishedAt,
        )
        break
      }

      await sleep(100)
    }
  })
})
