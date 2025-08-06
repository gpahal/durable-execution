import * as v from 'valibot'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { sleep } from '@gpahal/std/promises'

import { DurableExecutor, type DurableTask } from '../src'
import { InMemoryStorage } from './in-memory-storage'

describe('examples', () => {
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
    void executor.shutdown()
  })

  it('should complete simple sync task', async () => {
    const taskA = executor.task({
      id: 'a',
      timeoutMs: 1000,
      run: (ctx, input: { name: string }) => {
        // ... do some synchronous work
        return `Hello, ${input.name}!`
      },
    })

    const handle = await executor.enqueueTask(taskA, { name: 'world' })

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('a')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBe('Hello, world!')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete simple async task', async () => {
    const taskA = executor.task({
      id: 'a',
      timeoutMs: 1000,
      run: async (ctx, input: { name: string }) => {
        // ... do some asynchronous work
        await sleep(1)
        return `Hello, ${input.name}!`
      },
    })

    const handle = await executor.enqueueTask(taskA, { name: 'world' })

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('a')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBe('Hello, world!')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete task with validate input', async () => {
    const taskA = executor
      .validateInput((input: { name: string }) => {
        if (input.name !== 'world') {
          throw new Error('Invalid input')
        }
        return input
      })
      .task({
        id: 'a',
        timeoutMs: 1000,
        run: (ctx, input) => {
          // ... do some work
          return `Hello, ${input.name}!`
        },
      })

    const handle = await executor.enqueueTask(taskA, { name: 'world' })

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('a')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBe('Hello, world!')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete task with input schema', async () => {
    const taskA = executor.inputSchema(v.object({ name: v.string() })).task({
      id: 'a',
      timeoutMs: 1000,
      run: (ctx, input) => {
        // ... do some work
        return `Hello, ${input.name}!`
      },
    })

    const handle = await executor.enqueueTask(taskA, { name: 'world' })

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('a')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBe('Hello, world!')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete task with retries', async () => {
    let totalAttempts = 0
    const taskA = executor.task({
      id: 'a',
      timeoutMs: 1000,
      maxRetryAttempts: 5,
      run: (ctx, input: { name: string }) => {
        totalAttempts++
        if (ctx.attempt < 2) {
          throw new Error('Failed')
        }
        return {
          totalAttempts,
          output: `Hello, ${input.name}!`,
        }
      },
    })

    const handle = await executor.enqueueTask(taskA, { name: 'world' })

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('a')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.totalAttempts).toBe(3)
    expect(finishedExecution.output.output).toBe('Hello, world!')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete task run context', async () => {
    const taskA = executor.task({
      id: 'a',
      timeoutMs: 1000,
      run: (ctx) => {
        return {
          taskId: ctx.taskId,
          executionId: ctx.executionId,
          attempt: ctx.attempt,
          prevError: ctx.prevError,
        }
      },
    })

    const handle = await executor.enqueueTask(taskA, undefined)

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('a')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.taskId).toBe('a')
    expect(finishedExecution.output.executionId).toBe(finishedExecution.executionId)
    expect(finishedExecution.output.attempt).toBe(0)
    expect(finishedExecution.output.prevError).toBeUndefined()
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete parent task with parallel children', async () => {
    const taskA = executor.task({
      id: 'a',
      timeoutMs: 1000,
      run: (ctx, input: { name: string }) => {
        return `Hello from task A, ${input.name}!`
      },
    })
    const taskB = executor.task({
      id: 'b',
      timeoutMs: 1000,
      run: (ctx, input: { name: string }) => {
        return `Hello from task B, ${input.name}!`
      },
    })

    const parentTask = executor.parentTask({
      id: 'parent',
      timeoutMs: 1000,
      runParent: (ctx, input: { name: string }) => {
        return {
          output: `Hello from parent task, ${input.name}!`,
          children: [
            {
              task: taskA,
              input: { name: input.name },
            },
            {
              task: taskB,
              input: { name: input.name },
            },
          ],
        }
      },
    })

    const handle = await executor.enqueueTask(parentTask, { name: 'world' })

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('parent')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.output).toBe('Hello from parent task, world!')
    expect(finishedExecution.output.childrenOutputs[0]!.output).toBe('Hello from task A, world!')
    expect(finishedExecution.output.childrenOutputs[1]!.output).toBe('Hello from task B, world!')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete parent task with parallel children and combined output', async () => {
    const taskA = executor.task({
      id: 'a',
      timeoutMs: 1000,
      run: (ctx, input: { name: string }) => {
        return `Hello from task A, ${input.name}!`
      },
    })
    const taskB = executor.task({
      id: 'b',
      timeoutMs: 1000,
      run: (ctx, input: { name: string }) => {
        return `Hello from task B, ${input.name}!`
      },
    })

    const parentTask = executor.parentTask({
      id: 'parent',
      timeoutMs: 1000,
      runParent: (ctx, input: { name: string }) => {
        return {
          output: `Hello from parent task, ${input.name}!`,
          children: [
            {
              task: taskA,
              input: { name: input.name },
            },
            {
              task: taskB,
              input: { name: input.name },
            },
          ],
        }
      },
      onRunAndChildrenComplete: {
        id: 'onParentRunAndChildrenComplete',
        timeoutMs: 1000,
        run: (ctx, { output, childrenOutputs }) => {
          return {
            parentOutput: output,
            taskAOutput: childrenOutputs[0]!.output as string,
            taskBOutput: childrenOutputs[1]!.output as string,
          }
        },
      },
    })

    const handle = await executor.enqueueTask(parentTask, { name: 'world' })

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('parent')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.parentOutput).toBe('Hello from parent task, world!')
    expect(finishedExecution.output.taskAOutput).toBe('Hello from task A, world!')
    expect(finishedExecution.output.taskBOutput).toBe('Hello from task B, world!')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete tasks sequentially', async () => {
    const taskA = executor.task({
      id: 'a',
      timeoutMs: 1000,
      run: (ctx, input: { name: string }) => {
        return {
          name: input.name,
          taskAOutput: `Hello from task A, ${input.name}!`,
        }
      },
    })
    const taskB = executor.task({
      id: 'b',
      timeoutMs: 1000,
      run: (ctx, input: { name: string; taskAOutput: string }) => {
        return {
          name: input.name,
          taskAOutput: input.taskAOutput,
          taskBOutput: `Hello from task B, ${input.name}!`,
        }
      },
    })
    const taskC = executor.task({
      id: 'c',
      timeoutMs: 1000,
      run: (ctx, input: { name: string; taskAOutput: string; taskBOutput: string }) => {
        return {
          taskAOutput: input.taskAOutput,
          taskBOutput: input.taskBOutput,
          taskCOutput: `Hello from task C, ${input.name}!`,
        }
      },
    })

    const task = executor.sequentialTasks(taskA, taskB, taskC)

    const handle = await executor.enqueueTask(task, { name: 'world' })

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toContain('st_')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.taskAOutput).toBe('Hello from task A, world!')
    expect(finishedExecution.output.taskBOutput).toBe('Hello from task B, world!')
    expect(finishedExecution.output.taskCOutput).toBe('Hello from task C, world!')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete tasks sequentially manually', async () => {
    const taskC = executor.task({
      id: 'c',
      timeoutMs: 1000,
      run: (ctx, input: { name: string }) => {
        return `Hello from task C, ${input.name}!`
      },
    })
    const taskB = executor.parentTask({
      id: 'b',
      timeoutMs: 1000,
      runParent: (ctx, input: { name: string }) => {
        return {
          output: `Hello from task B, ${input.name}!`,
        }
      },
      onRunAndChildrenComplete: {
        id: 'onTaskBRunAndChildrenComplete',
        timeoutMs: 1000,
        runParent: (ctx, { input, output }) => {
          return {
            output,
            children: [{ task: taskC, input: { name: input.name } }],
          }
        },
        onRunAndChildrenComplete: {
          id: 'onTaskBRunAndChildrenCompleteNested',
          timeoutMs: 1000,
          run: (ctx, { output, childrenOutputs }) => {
            return {
              taskBOutput: output,
              taskCOutput: childrenOutputs[0]!.output as string,
            }
          },
        },
      },
    })
    const taskA = executor.parentTask({
      id: 'a',
      timeoutMs: 1000,
      runParent: (ctx, input: { name: string }) => {
        return {
          output: `Hello from task A, ${input.name}!`,
        }
      },
      onRunAndChildrenComplete: {
        id: 'onTaskARunAndChildrenComplete',
        timeoutMs: 1000,
        runParent: (ctx, { input, output }) => {
          return {
            output,
            children: [{ task: taskB, input: { name: input.name } }],
          }
        },
        onRunAndChildrenComplete: {
          id: 'onTaskARunAndChildrenCompleteNested',
          timeoutMs: 1000,
          run: (ctx, { output, childrenOutputs }) => {
            const taskBOutput = childrenOutputs[0]!.output as {
              taskBOutput: string
              taskCOutput: string
            }
            return {
              taskAOutput: output,
              taskBOutput: taskBOutput.taskBOutput,
              taskCOutput: taskBOutput.taskCOutput,
            }
          },
        },
      },
    })

    const handle = await executor.enqueueTask(taskA, { name: 'world' })

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('a')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.taskAOutput).toBe('Hello from task A, world!')
    expect(finishedExecution.output.taskBOutput).toBe('Hello from task B, world!')
    expect(finishedExecution.output.taskCOutput).toBe('Hello from task C, world!')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete multiple parent tasks with parallel children run sequentially', async () => {
    const taskA1 = executor.task({
      id: 'a1',
      timeoutMs: 1000,
      run: (ctx, input: { name: string }) => {
        return `Hello from task A1, ${input.name}!`
      },
    })
    const taskA2 = executor.task({
      id: 'a2',
      timeoutMs: 1000,
      run: (ctx, input: { name: string }) => {
        return `Hello from task A2, ${input.name}!`
      },
    })
    const taskB1 = executor.task({
      id: 'b1',
      timeoutMs: 1000,
      run: (ctx, input: { name: string }) => {
        return `Hello from task B1, ${input.name}!`
      },
    })
    const taskB2 = executor.task({
      id: 'b2',
      timeoutMs: 1000,
      run: (ctx, input: { name: string }) => {
        return `Hello from task B2, ${input.name}!`
      },
    })

    const taskA = executor.parentTask({
      id: 'a',
      timeoutMs: 1000,
      runParent: (ctx, input: { name: string }) => {
        return {
          output: `Hello from task A, ${input.name}!`,
          children: [
            { task: taskA1, input: { name: input.name } },
            { task: taskA2, input: { name: input.name } },
          ],
        }
      },
      onRunAndChildrenComplete: {
        id: 'onTaskARunAndChildrenComplete',
        timeoutMs: 1000,
        run: (ctx, { input, output, childrenOutputs }) => {
          return {
            name: input.name,
            taskAOutput: output,
            taskA1Output: childrenOutputs[0]!.output as string,
            taskA2Output: childrenOutputs[1]!.output as string,
          }
        },
      },
    })
    const taskB = executor.parentTask({
      id: 'b',
      timeoutMs: 1000,
      runParent: (
        ctx,
        input: { name: string; taskAOutput: string; taskA1Output: string; taskA2Output: string },
      ) => {
        return {
          output: {
            taskAOutput: input.taskAOutput,
            taskA1Output: input.taskA1Output,
            taskA2Output: input.taskA2Output,
            taskBOutput: `Hello from task B, ${input.name}!`,
          },
          children: [
            { task: taskB1, input: { name: input.name } },
            { task: taskB2, input: { name: input.name } },
          ],
        }
      },
      onRunAndChildrenComplete: {
        id: 'onTaskBRunAndChildrenComplete',
        timeoutMs: 1000,
        run: (ctx, { output, childrenOutputs }) => {
          return {
            ...output,
            taskB1Output: childrenOutputs[0]!.output as string,
            taskB2Output: childrenOutputs[1]!.output as string,
          }
        },
      },
    })

    const task = executor.sequentialTasks(taskA, taskB)

    const handle = await executor.enqueueTask(task, { name: 'world' })

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toContain('st_')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.taskAOutput).toBe('Hello from task A, world!')
    expect(finishedExecution.output.taskA1Output).toBe('Hello from task A1, world!')
    expect(finishedExecution.output.taskA2Output).toBe('Hello from task A2, world!')
    expect(finishedExecution.output.taskBOutput).toBe('Hello from task B, world!')
    expect(finishedExecution.output.taskB1Output).toBe('Hello from task B1, world!')
    expect(finishedExecution.output.taskB2Output).toBe('Hello from task B2, world!')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete task tree', async () => {
    const taskB1 = executor.task({
      id: 'b1',
      timeoutMs: 1000,
      run: (ctx, input: { name: string }) => {
        return {
          name: input.name,
          taskB1Output: `Hello from task B1, ${input.name}!`,
        }
      },
    })
    const taskB2 = executor.task({
      id: 'b2',
      timeoutMs: 1000,
      run: (ctx, input: { name: string; taskB1Output: string }) => {
        return {
          name: input.name,
          taskB1Output: input.taskB1Output,
          taskB2Output: `Hello from task B2, ${input.name}!`,
        }
      },
    })
    const taskB3 = executor.task({
      id: 'b3',
      timeoutMs: 1000,
      run: (ctx, input: { name: string; taskB1Output: string; taskB2Output: string }) => {
        return {
          taskB1Output: input.taskB1Output,
          taskB2Output: input.taskB2Output,
          taskB3Output: `Hello from task B3, ${input.name}!`,
        }
      },
    })
    const taskB = executor.sequentialTasks(taskB1, taskB2, taskB3)

    const taskA1 = executor.task({
      id: 'a1',
      timeoutMs: 1000,
      run: (ctx, input: { name: string }) => {
        return `Hello from task A1, ${input.name}!`
      },
    })
    const taskA2 = executor.task({
      id: 'a2',
      timeoutMs: 1000,
      run: (ctx, input: { name: string }) => {
        return `Hello from task A2, ${input.name}!`
      },
    })
    const taskA3 = executor.task({
      id: 'a3',
      timeoutMs: 1000,
      run: (ctx, input: { name: string }) => {
        return `Hello from task A3, ${input.name}!`
      },
    })
    const taskA = executor.parentTask({
      id: 'a',
      timeoutMs: 1000,
      runParent: (ctx, input: { name: string }) => {
        return {
          output: `Hello from task A, ${input.name}!`,
          children: [
            { task: taskA1, input: { name: input.name } },
            { task: taskA2, input: { name: input.name } },
            { task: taskA3, input: { name: input.name } },
          ],
        }
      },
      onRunAndChildrenComplete: {
        id: 'onTaskARunAndChildrenComplete',
        timeoutMs: 1000,
        run: (ctx, { output, childrenOutputs }) => {
          return {
            taskAOutput: output,
            taskA1Output: childrenOutputs[0]!.output as string,
            taskA2Output: childrenOutputs[1]!.output as string,
            taskA3Output: childrenOutputs[2]!.output as string,
          }
        },
      },
    })

    const rootTask = executor.parentTask({
      id: 'root',
      timeoutMs: 1000,
      runParent: (ctx, input: { name: string }) => {
        return {
          output: `Hello from root task, ${input.name}!`,
          children: [
            { task: taskA, input: { name: input.name } },
            { task: taskB, input: { name: input.name } },
          ],
        }
      },
      onRunAndChildrenComplete: {
        id: 'onRootRunAndChildrenComplete',
        timeoutMs: 1000,
        run: (ctx, { output, childrenOutputs }) => {
          const taskAOutput = childrenOutputs[0]!.output as {
            taskAOutput: string
            taskA1Output: string
            taskA2Output: string
            taskA3Output: string
          }
          const taskBOutput = childrenOutputs[1]!.output as {
            taskB1Output: string
            taskB2Output: string
            taskB3Output: string
          }
          return {
            rootOutput: output,
            taskAOutput: taskAOutput.taskAOutput,
            taskA1Output: taskAOutput.taskA1Output,
            taskA2Output: taskAOutput.taskA2Output,
            taskA3Output: taskAOutput.taskA3Output,
            taskB1Output: taskBOutput.taskB1Output,
            taskB2Output: taskBOutput.taskB2Output,
            taskB3Output: taskBOutput.taskB3Output,
          }
        },
      },
    })

    const handle = await executor.enqueueTask(rootTask, { name: 'world' })

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('root')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.taskAOutput).toBe('Hello from task A, world!')
    expect(finishedExecution.output.taskA1Output).toBe('Hello from task A1, world!')
    expect(finishedExecution.output.taskA2Output).toBe('Hello from task A2, world!')
    expect(finishedExecution.output.taskA3Output).toBe('Hello from task A3, world!')
    expect(finishedExecution.output.taskB1Output).toBe('Hello from task B1, world!')
    expect(finishedExecution.output.taskB2Output).toBe('Hello from task B2, world!')
    expect(finishedExecution.output.taskB3Output).toBe('Hello from task B3, world!')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete recursive task', async () => {
    const recursiveTask: DurableTask<{ index: number }, { count: number }> = executor
      .inputSchema(v.object({ index: v.pipe(v.number(), v.integer(), v.minValue(0)) }))
      .parentTask({
        id: 'recursive',
        timeoutMs: 1000,
        runParent: async (ctx, input) => {
          await sleep(1)
          return {
            output: undefined,
            children:
              input.index >= 9 ? [] : [{ task: recursiveTask, input: { index: input.index + 1 } }],
          }
        },
        onRunAndChildrenComplete: {
          id: 'onRecursiveRunAndChildrenComplete',
          timeoutMs: 1000,
          run: (ctx, { childrenOutputs }) => {
            return {
              count:
                1 +
                childrenOutputs.reduce(
                  (acc, childOutput) => acc + (childOutput.output as { count: number }).count,
                  0,
                ),
            }
          },
        },
      })

    const handle = await executor.enqueueTask(recursiveTask, { index: 0 })

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('recursive')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.count).toBe(10)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete polling task', { timeout: 10_000 }, async () => {
    let value: number | undefined
    setTimeout(() => {
      value = 10
    }, 2000)

    const pollingTask: DurableTask<{ prevCount: number }, { count: number; value: number }> =
      executor
        .inputSchema(v.object({ prevCount: v.pipe(v.number(), v.integer(), v.minValue(0)) }))
        .parentTask({
          id: 'polling',
          timeoutMs: 1000,
          sleepMsBeforeAttempt: 100,
          runParent: (ctx, input) => {
            if (value != null) {
              return {
                output: {
                  isDone: true,
                  value,
                } as { isDone: false; value: undefined } | { isDone: true; value: number },
                children: [],
              }
            }

            return {
              output: {
                isDone: false,
                value,
              } as { isDone: false; value: undefined } | { isDone: true; value: number },
              children: [{ task: pollingTask, input: { prevCount: input.prevCount + 1 } }],
            }
          },
          onRunAndChildrenComplete: {
            id: 'onPollingRunAndChildrenComplete',
            timeoutMs: 1000,
            run: (ctx, { input, output, childrenOutputs }) => {
              if (output.isDone) {
                return {
                  count: input.prevCount + 1,
                  value: output.value,
                }
              }

              return childrenOutputs[0]!.output as {
                count: number
                value: number
              }
            },
          },
        })

    const handle = await executor.enqueueTask(pollingTask, { prevCount: 0 })

    const finishedExecution = await handle.waitAndGetTaskFinishedExecution()
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('polling')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.count).toBeGreaterThanOrEqual(5)
    expect(finishedExecution.output.count).toBeLessThan(25)
    expect(finishedExecution.output.value).toBe(10)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })
})
