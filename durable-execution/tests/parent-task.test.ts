import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { z } from 'zod'

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
      backgroundProcessIntraBatchSleepMs: 50,
    })
    executor.startBackgroundProcesses()
  })

  afterEach(async () => {
    await executor.shutdown()
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
          childrenTasks: [
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

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(3)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.output).toBe('test_output')
    expect(finishedExecution.output.childrenTaskExecutionsOutputs).toHaveLength(2)
    expect(finishedExecution.output.childrenTaskExecutionsOutputs[0]!.index).toBe(0)
    expect(finishedExecution.output.childrenTaskExecutionsOutputs[0]!.taskId).toBe('child1')
    expect(finishedExecution.output.childrenTaskExecutionsOutputs[0]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.output.childrenTaskExecutionsOutputs[0]!.output).toBe('child1_output')
    expect(finishedExecution.output.childrenTaskExecutionsOutputs[1]!.index).toBe(1)
    expect(finishedExecution.output.childrenTaskExecutionsOutputs[1]!.taskId).toBe('child2')
    expect(finishedExecution.output.childrenTaskExecutionsOutputs[1]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.output.childrenTaskExecutionsOutputs[1]!.output).toBe('child2_output')
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
          childrenTasks: [],
        }
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(1)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.output).toBe('test_output')
    expect(finishedExecution.output.childrenTaskExecutionsOutputs).toHaveLength(0)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with finalizeTask', async () => {
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
          childrenTasks: [
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
      finalizeTask: {
        id: 'finalizeTask',
        timeoutMs: 1000,
        run: async (_, { output, childrenTaskExecutionsOutputs }) => {
          executed++
          await sleep(1)
          return `${output}_${childrenTaskExecutionsOutputs.map((c) => c.output).join('_')}`
        },
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
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

  it('should complete with finalizeTask and no children', async () => {
    let executed = 0
    const task = executor.parentTask({
      id: 'test',
      timeoutMs: 1000,
      runParent: async () => {
        executed++
        await sleep(1)
        return {
          output: 'test_output',
        }
      },
      finalizeTask: {
        id: 'finalizeTask',
        timeoutMs: 1000,
        run: async (_, { output, childrenTaskExecutionsOutputs }) => {
          executed++
          await sleep(1)
          return `${output}_${childrenTaskExecutionsOutputs.map((c) => c.output).join('_')}`
        },
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
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

  it('should complete with input schema', async () => {
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

    const task = executor
      .inputSchema(
        z.object({
          name: z.string(),
        }),
      )
      .parentTask({
        id: 'test',
        timeoutMs: 1000,
        runParent: async (_, input) => {
          executed++
          await sleep(1)
          return {
            output: input.name,
            childrenTasks: [
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

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(3)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.output).toBe('test_output')
    expect(finishedExecution.output.childrenTaskExecutionsOutputs).toHaveLength(2)
    expect(finishedExecution.output.childrenTaskExecutionsOutputs[0]!.taskId).toBe('child1')
    expect(finishedExecution.output.childrenTaskExecutionsOutputs[0]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.output.childrenTaskExecutionsOutputs[0]!.output).toBe('child1_output')
    expect(finishedExecution.output.childrenTaskExecutionsOutputs[1]!.taskId).toBe('child2')
    expect(finishedExecution.output.childrenTaskExecutionsOutputs[1]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.output.childrenTaskExecutionsOutputs[1]!.output).toBe('child2_output')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail with input schema', async () => {
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

    const task = executor
      .inputSchema(
        z.object({
          name: z.string(),
          version: z.number(),
        }),
      )
      .parentTask({
        id: 'test',
        timeoutMs: 1000,
        runParent: async (_, input) => {
          executed++
          await sleep(1)
          return {
            output: input.name,
            childrenTasks: [
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
      'Invalid input to task test',
    )
    expect(executed).toBe(0)
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
          childrenTasks: [
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
      finalizeTask: {
        id: 'finalizeTask',
        timeoutMs: 1000,
        run: async (_, { output, childrenTaskExecutionsOutputs }) => {
          executed++
          await sleep(1)
          return `${output}_${childrenTaskExecutionsOutputs.map((c) => c.output).join('_')}`
        },
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(3)
    expect(finishedExecution.status).toBe('children_tasks_failed')
    assert(finishedExecution.status === 'children_tasks_failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.childrenTaskExecutionsErrors).toBeDefined()
    expect(finishedExecution.childrenTaskExecutionsErrors).toHaveLength(1)
    expect(finishedExecution.childrenTaskExecutionsErrors[0]!.index).toBe(0)
    expect(finishedExecution.childrenTaskExecutionsErrors[0]!.taskId).toBe('child1')
    expect(finishedExecution.childrenTaskExecutionsErrors[0]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.childrenTaskExecutionsErrors[0]!.error?.message).toBe('child1 error')
    expect(finishedExecution.childrenTaskExecutionsErrors[0]!.error?.errorType).toBe('generic')
    expect(finishedExecution.childrenTaskExecutionsErrors[0]!.error?.isRetryable).toBe(true)
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
          childrenTasks: [
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
      finalizeTask: {
        id: 'finalizeTask',
        timeoutMs: 1000,
        run: async (_, { output, childrenTaskExecutionsOutputs }) => {
          executed++
          await sleep(1)
          return `${output}_${childrenTaskExecutionsOutputs.map((c) => c.output).join('_')}`
        },
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(3)
    expect(finishedExecution.status).toBe('children_tasks_failed')
    assert(finishedExecution.status === 'children_tasks_failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.childrenTaskExecutionsErrors).toBeDefined()
    expect(finishedExecution.childrenTaskExecutionsErrors).toHaveLength(2)
    finishedExecution.childrenTaskExecutionsErrors.sort((a, b) => a.index - b.index)
    expect(finishedExecution.childrenTaskExecutionsErrors[0]!.index).toBe(0)
    expect(finishedExecution.childrenTaskExecutionsErrors[0]!.taskId).toBe('child1')
    expect(finishedExecution.childrenTaskExecutionsErrors[0]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.childrenTaskExecutionsErrors[0]!.error?.message).toBe('child1 error')
    expect(finishedExecution.childrenTaskExecutionsErrors[0]!.error?.errorType).toBe('generic')
    expect(finishedExecution.childrenTaskExecutionsErrors[0]!.error?.isRetryable).toBe(true)
    expect(finishedExecution.childrenTaskExecutionsErrors[1]!.index).toBe(1)
    expect(finishedExecution.childrenTaskExecutionsErrors[1]!.taskId).toBe('child2')
    expect(finishedExecution.childrenTaskExecutionsErrors[1]!.executionId).toMatch(/^te_/)
    expect(finishedExecution.childrenTaskExecutionsErrors[1]!.error?.message).toBe('child2 error')
    expect(finishedExecution.childrenTaskExecutionsErrors[1]!.error?.errorType).toBe('generic')
    expect(finishedExecution.childrenTaskExecutionsErrors[1]!.error?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should fail with finalizeTask failing', async () => {
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
          childrenTasks: [
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
      finalizeTask: {
        id: 'finalizeTask',
        timeoutMs: 1000,
        run: async () => {
          executed++
          await sleep(1)
          throw new Error('finalizeTask error')
        },
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(4)
    expect(finishedExecution.status).toBe('finalize_task_failed')
    assert(finishedExecution.status === 'finalize_task_failed')
    expect(finishedExecution.taskId).toBe('test')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.finalizeTaskExecutionError).toBeDefined()
    expect(finishedExecution.finalizeTaskExecutionError?.message).toBe('finalizeTask error')
    expect(finishedExecution.finalizeTaskExecutionError?.errorType).toBe('generic')
    expect(finishedExecution.finalizeTaskExecutionError?.isRetryable).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete with finalizeTask parent task', async () => {
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

    const finalizeTaskChild1 = executor.task({
      id: 'finalizeTaskChild1',
      timeoutMs: 1000,
      run: async () => {
        executed++
        await sleep(1)
        return 'finalizeTaskChild1_output'
      },
    })
    const finalizeTaskChild2 = executor.task({
      id: 'finalizeTaskChild2',
      timeoutMs: 1000,
      run: async () => {
        executed++
        await sleep(1)
        return 'finalizeTaskChild2_output'
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
          childrenTasks: [
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
      finalizeTask: {
        id: 'finalizeTask',
        timeoutMs: 1000,
        runParent: async (_, { output, childrenTaskExecutionsOutputs }) => {
          executed++
          await sleep(1)
          return {
            output: `${output}_${childrenTaskExecutionsOutputs.map((c) => c.output).join('_')}`,
            childrenTasks: [
              { task: finalizeTaskChild1, input: undefined },
              { task: finalizeTaskChild2, input: undefined },
            ],
          }
        },
        finalizeTask: {
          id: 'finalizeTaskNested',
          timeoutMs: 1000,
          run: async (_, { output, childrenTaskExecutionsOutputs }) => {
            executed++
            await sleep(1)
            return `${output}_${childrenTaskExecutionsOutputs.map((c) => c.output).join('_')}`
          },
        },
      },
    })

    const handle = await executor.enqueueTask(task)

    const finishedExecution = await handle.waitAndGetFinishedExecution()
    expect(executed).toBe(7)
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
})
