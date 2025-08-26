import { z } from 'zod'

import { sleep } from '@gpahal/std/promises'

import {
  ChildTask,
  DurableExecutionError,
  DurableExecutor,
  InMemoryTaskExecutionsStorage,
  type CompletedChildTaskExecution,
  type Task,
} from '../src'

describe('examples', () => {
  let storage: InMemoryTaskExecutionsStorage
  let executor: DurableExecutor

  beforeEach(() => {
    storage = new InMemoryTaskExecutionsStorage()
    executor = new DurableExecutor(storage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    executor.startBackgroundProcesses()
  })

  afterEach(async () => {
    await executor.shutdown()
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

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
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

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
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

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
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
    const taskA = executor.inputSchema(z.object({ name: z.string() })).task({
      id: 'a',
      timeoutMs: 1000,
      run: (ctx, input) => {
        // ... do some work
        return `Hello, ${input.name}!`
      },
    })

    const handle = await executor.enqueueTask(taskA, { name: 'world' })

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
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
      retryOptions: {
        maxAttempts: 5,
        baseDelayMs: 100,
        delayMultiplier: 1.5,
        maxDelayMs: 1000,
      },
      timeoutMs: 1000,
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

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
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

    const handle = await executor.enqueueTask(taskA)

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
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
            new ChildTask(taskA, { name: input.name }),
            new ChildTask(taskB, { name: input.name }),
          ],
        }
      },
    })

    const handle = await executor.enqueueTask(parentTask, { name: 'world' })

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('parent')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.output).toBe('Hello from parent task, world!')
    expect(finishedExecution.output.children[0]!.status).toBe('completed')
    assert(finishedExecution.output.children[0]!.status === 'completed')
    expect(finishedExecution.output.children[0]!.output).toBe('Hello from task A, world!')
    expect(finishedExecution.output.children[1]!.status).toBe('completed')
    assert(finishedExecution.output.children[1]!.status === 'completed')
    expect(finishedExecution.output.children[1]!.output).toBe('Hello from task B, world!')
    assert(finishedExecution.output.children[0]!.output === 'Hello from task A, world!')
    assert(finishedExecution.output.children[1]!.output === 'Hello from task B, world!')
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
            new ChildTask(taskA, { name: input.name }),
            new ChildTask(taskB, { name: input.name }),
          ],
        }
      },
      finalize: {
        id: 'onParentRunAndChildrenComplete',
        timeoutMs: 1000,
        run: (ctx, { output, children }) => {
          const child1 = children[0]!
          const child2 = children[1]!

          // The finalize function receives all children executions, including failed ones.
          // This allows you to implement custom error handling logic.
          if (child1.status !== 'completed' || child2.status !== 'completed') {
            throw DurableExecutionError.nonRetryable('Children failed')
          }

          return {
            parentOutput: output,
            taskAOutput: child1.output as string,
            taskBOutput: child2.output as string,
          }
        },
      },
    })

    const handle = await executor.enqueueTask(parentTask, { name: 'world' })

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
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

  it('should fail parent task if any parallel children fail', async () => {
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
      run: () => {
        throw new Error('Failed')
      },
    })

    const parentTask = executor.parentTask({
      id: 'parent',
      timeoutMs: 1000,
      runParent: (ctx, input: { name: string }) => {
        return {
          output: `Hello from parent task, ${input.name}!`,
          children: [new ChildTask(taskA, { name: input.name }), new ChildTask(taskB)],
        }
      },
      finalize: {
        id: 'onParentRunAndChildrenComplete',
        timeoutMs: 1000,
        run: (ctx, { output, children }) => {
          const child1 = children[0]!
          const child2 = children[1]!

          // The finalize function receives all children executions, including failed ones.
          // This allows you to implement custom error handling logic.
          if (child1.status !== 'completed' || child2.status !== 'completed') {
            throw DurableExecutionError.nonRetryable('Children failed')
          }

          return {
            parentOutput: output,
            taskAOutput: child1.output as string,
            taskBOutput: child2.output as string,
          }
        },
      },
    })

    const handle = await executor.enqueueTask(parentTask, { name: 'world' })

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(finishedExecution.status).toBe('finalize_failed')
    assert(finishedExecution.status === 'finalize_failed')
    expect(finishedExecution.taskId).toBe('parent')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.error).toBeDefined()
    expect(finishedExecution.error?.message).toContain('Children failed')
    expect(finishedExecution.error?.errorType).toBe('generic')
    expect(finishedExecution.error?.isRetryable).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete parent task with partial success if any parallel children fail', async () => {
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
      run: () => {
        throw new Error('Failed')
      },
    })

    const resilientParentTask = executor.parentTask({
      id: 'resilientParent',
      timeoutMs: 1000,
      runParent: (ctx, input: { name: string }) => {
        return {
          output: `Hello from parent task, ${input.name}!`,
          children: [new ChildTask(taskA, { name: input.name }), new ChildTask(taskB)],
        }
      },
      finalize: {
        id: 'resilientFinalize',
        timeoutMs: 1000,
        run: (ctx, { output, children }) => {
          const results = children.map((child, index) => ({
            index,
            success: child.status === 'completed',
            result: child.status === 'completed' ? child.output : child.error?.message,
          }))

          const successfulResults = results.filter((r) => r.success)

          // Continue even if some children failed.
          return {
            parentOutput: output,
            successfulCount: successfulResults.length,
            totalCount: children.length,
            results,
          }
        },
      },
    })

    const handle = await executor.enqueueTask(resilientParentTask, { name: 'world' })

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('resilientParent')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.parentOutput).toBe('Hello from parent task, world!')
    expect(finishedExecution.output.successfulCount).toBe(1)
    expect(finishedExecution.output.totalCount).toBe(2)
    expect(finishedExecution.output.results).toBeDefined()
    expect(finishedExecution.output.results[0]!.success).toBe(true)
    expect(finishedExecution.output.results[0]!.result).toBe('Hello from task A, world!')
    expect(finishedExecution.output.results[1]!.success).toBe(false)
    expect(finishedExecution.output.results[1]!.result).toBe('Failed')
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

    const task = executor.sequentialTasks('seq', [taskA, taskB, taskC])

    const handle = await executor.enqueueTask(task, { name: 'world' })

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('seq')
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
          output: {
            name: input.name,
            taskBOutput: `Hello from task B, ${input.name}!`,
          },
        }
      },
      finalize: {
        id: 'taskBFinalize',
        timeoutMs: 1000,
        runParent: (ctx, { output }) => {
          return {
            output: output.taskBOutput,
            children: [new ChildTask(taskC, { name: output.name })],
          }
        },
        finalize: {
          id: 'taskBFinalizeNested',
          timeoutMs: 1000,
          run: (ctx, { output, children }) => {
            const child = children[0]!
            if (child.status !== 'completed') {
              throw DurableExecutionError.nonRetryable('Child failed')
            }

            return {
              taskBOutput: output,
              taskCOutput: child.output as string,
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
          output: {
            name: input.name,
            taskAOutput: `Hello from task A, ${input.name}!`,
          },
        }
      },
      finalize: {
        id: 'taskAFinalize',
        timeoutMs: 1000,
        runParent: (ctx, { output }) => {
          return {
            output: output.taskAOutput,
            children: [new ChildTask(taskB, { name: output.name })],
          }
        },
        finalize: {
          id: 'taskAFinalizeNested',
          timeoutMs: 1000,
          run: (ctx, { output, children }) => {
            const child = children[0]!
            if (child.status !== 'completed') {
              throw DurableExecutionError.nonRetryable('Child failed')
            }

            const taskBOutput = child.output as {
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

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
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
          output: {
            name: input.name,
            taskAOutput: `Hello from task A, ${input.name}!`,
          },
          children: [
            new ChildTask(taskA1, { name: input.name }),
            new ChildTask(taskA2, { name: input.name }),
          ],
        }
      },
      finalize: {
        id: 'taskAFinalize',
        timeoutMs: 1000,
        run: (ctx, { output, children }) => {
          const child1 = children[0]!
          const child2 = children[1]!
          if (child1.status !== 'completed' || child2.status !== 'completed') {
            throw DurableExecutionError.nonRetryable('Children failed')
          }

          return {
            name: output.name,
            taskAOutput: output.taskAOutput,
            taskA1Output: child1.output as string,
            taskA2Output: child2.output as string,
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
            new ChildTask(taskB1, { name: input.name }),
            new ChildTask(taskB2, { name: input.name }),
          ],
        }
      },
      finalize: {
        id: 'taskBFinalize',
        timeoutMs: 1000,
        run: (ctx, { output, children }) => {
          const child1 = children[0]!
          const child2 = children[1]!
          if (child1.status !== 'completed' || child2.status !== 'completed') {
            throw DurableExecutionError.nonRetryable('Children failed')
          }

          return {
            ...output,
            taskB1Output: child1.output as string,
            taskB2Output: child2.output as string,
          }
        },
      },
    })

    const task = executor.sequentialTasks('seq', [taskA, taskB])

    const handle = await executor.enqueueTask(task, { name: 'world' })

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('seq')
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
    const taskB = executor.sequentialTasks('b', [taskB1, taskB2, taskB3])

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
            new ChildTask(taskA1, { name: input.name }),
            new ChildTask(taskA2, { name: input.name }),
            new ChildTask(taskA3, { name: input.name }),
          ],
        }
      },
      finalize: {
        id: 'taskAFinalize',
        timeoutMs: 1000,
        run: (ctx, { output, children }) => {
          const child1 = children[0]!
          const child2 = children[1]!
          const child3 = children[2]!
          if (
            child1.status !== 'completed' ||
            child2.status !== 'completed' ||
            child3.status !== 'completed'
          ) {
            throw DurableExecutionError.nonRetryable('Children failed')
          }

          return {
            taskAOutput: output,
            taskA1Output: child1.output as string,
            taskA2Output: child2.output as string,
            taskA3Output: child3.output as string,
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
            new ChildTask(taskA, { name: input.name }),
            new ChildTask(taskB, { name: input.name }),
          ],
        }
      },
      finalize: {
        id: 'rootFinalize',
        timeoutMs: 1000,
        run: (ctx, { output, children }) => {
          const child1 = children[0]!
          const child2 = children[1]!
          if (child1.status !== 'completed' || child2.status !== 'completed') {
            throw DurableExecutionError.nonRetryable('Children failed')
          }

          const taskAOutput = child1.output as {
            taskAOutput: string
            taskA1Output: string
            taskA2Output: string
            taskA3Output: string
          }
          const taskBOutput = child2.output as {
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

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
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
    const recursiveTask: Task<{ index: number }, { count: number }> = executor
      .inputSchema(z.object({ index: z.number().int().min(0) }))
      .parentTask({
        id: 'recursive',
        timeoutMs: 1000,
        runParent: async (ctx, input) => {
          await sleep(1)
          return {
            output: undefined,
            children:
              input.index >= 9 ? [] : [new ChildTask(recursiveTask, { index: input.index + 1 })],
          }
        },
        finalize: {
          id: 'recursiveFinalize',
          timeoutMs: 1000,
          run: (ctx, { children }) => {
            if (children.some((child) => child.status !== 'completed')) {
              throw DurableExecutionError.nonRetryable('Children failed')
            }

            return {
              count:
                1 +
                (children as Array<CompletedChildTaskExecution>).reduce(
                  (acc, child) => acc + (child.output as { count: number }).count,
                  0,
                ),
            }
          },
        },
      })

    const handle = await executor.enqueueTask(recursiveTask, { index: 0 })

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
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
    }, 1000)

    const pollTask = executor.task({
      id: 'poll',
      sleepMsBeforeRun: 100,
      timeoutMs: 1000,
      run: () => {
        return value == null
          ? {
              isDone: false,
            }
          : {
              isDone: true,
              output: value,
            }
      },
    })

    const pollingTask = executor.pollingTask('polling', pollTask, 20, 100)
    const handle = await executor.enqueueTask(pollingTask)

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('polling')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.isSuccess).toBe(true)
    assert(finishedExecution.output.isSuccess)
    expect(finishedExecution.output.output).toBe(10)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete polling task manually', { timeout: 10_000 }, async () => {
    let value: number | undefined
    setTimeout(() => {
      value = 10
    }, 1000)

    const pollingTask: Task<{ prevCount: number }, { count: number; value: number }> = executor
      .inputSchema(z.object({ prevCount: z.number().int().min(0) }))
      .parentTask({
        id: 'polling',
        sleepMsBeforeRun: 100,
        timeoutMs: 1000,
        runParent: (ctx, input) => {
          if (value != null) {
            return {
              output: {
                isDone: true,
                value,
                prevCount: input.prevCount,
              } as
                | { isDone: false; value: undefined; prevCount: number }
                | { isDone: true; value: number; prevCount: number },
            }
          }

          return {
            output: {
              isDone: false,
              value,
              prevCount: input.prevCount,
            } as
              | { isDone: false; value: undefined; prevCount: number }
              | { isDone: true; value: number; prevCount: number },
            children: [new ChildTask(pollingTask, { prevCount: input.prevCount + 1 })],
          }
        },
        finalize: {
          id: 'pollingFinalize',
          timeoutMs: 1000,
          run: (ctx, { output, children }) => {
            if (output.isDone) {
              return {
                count: output.prevCount + 1,
                value: output.value,
              }
            }

            const child = children[0]!
            if (child.status !== 'completed') {
              throw DurableExecutionError.nonRetryable('Child failed')
            }

            return child.output as {
              count: number
              value: number
            }
          },
        },
      })

    const handle = await executor.enqueueTask(pollingTask, { prevCount: 0 })

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('polling')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBeDefined()
    expect(finishedExecution.output.count).toBeGreaterThanOrEqual(2)
    expect(finishedExecution.output.count).toBeLessThan(20)
    expect(finishedExecution.output.value).toBe(10)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete parent with child sleeping task', async () => {
    const waitForWebhookTask = executor.sleepingTask<string>({
      id: 'wait_for_webhook',
      timeoutMs: 60 * 60 * 1000, // 1 hour
    })

    const parentTask = executor.parentTask({
      id: 'parent',
      timeoutMs: 1000,
      runParent: () => {
        // ... generate entity id and wait for webhook or event to wake up the sleeping task
        const entityId = 'entity_id'
        return {
          output: 'parent_output',
          children: [new ChildTask(waitForWebhookTask, entityId)],
        }
      },
      finalize: {
        id: 'finalizeTask',
        timeoutMs: 1000,
        run: (ctx, { children }) => {
          const child = children[0]!
          if (child.status !== 'completed') {
            throw new Error(`Webhook task failed: ${child.error.message}`)
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
    const childExecution = await executor.wakeupSleepingTaskExecution(
      waitForWebhookTask,
      'entity_id',
      {
        status: 'completed',
        output: 'webhook_output',
      },
    )
    expect(childExecution.status).toBe('completed')
    assert(childExecution.status === 'completed')
    expect(childExecution.output).toBe('webhook_output')

    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.output).toBe('webhook_output')
  })
})
