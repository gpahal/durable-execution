import type { Context } from 'effect'
import { Effect, Exit, Schema, Scope } from 'effect'

import { childTask, makeEffectDurableExecutor } from '../src'
import type { EffectDurableExecutor } from '../src'
import { InMemoryTaskExecutionsStorage } from '../src/in-memory-storage'
import { TaskExecutionsStorageService } from '../src/storage'
import type { TaskExecutionsStorage } from '../src/storage'

describe('effectExecutor', () => {
  let storage: TaskExecutionsStorage
  let effectExecutor: EffectDurableExecutor
  let scope: Scope.Closeable
  let runtime: Context.Context<never>

  beforeEach(async () => {
    storage = new InMemoryTaskExecutionsStorage()

    const { effectExecutorLocal, scopeLocal, runtimeLocal } = await Effect.runPromise(
      Effect.gen(function* () {
        const scopeLocal = yield* Scope.make()

        const effect = makeEffectDurableExecutor().pipe(
          Effect.provideService(TaskExecutionsStorageService, storage),
        )
        const effectExecutorLocal = yield* effect.pipe(
          Effect.provideService(Scope.Scope, scopeLocal),
        )
        const runtimeLocal = yield* Effect.context()
        return { effectExecutorLocal, scopeLocal, runtimeLocal }
      }),
    )

    effectExecutor = effectExecutorLocal
    scope = scopeLocal
    runtime = runtimeLocal

    await Effect.runPromiseWith(runtimeLocal)(effectExecutorLocal.start)
  })

  afterEach(async () => {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- beforeEach can fail before it assigns this
    if (effectExecutor !== undefined) {
      await Effect.runPromiseWith(runtime)(effectExecutor.shutdown)
    }
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- beforeEach can fail before it assigns this
    if (scope !== undefined) {
      await Effect.runPromise(Scope.close(scope, Exit.void))
    }
  })

  it('should complete simple task', async () => {
    let executionCount = 0
    const task = Effect.runSyncWith(runtime)(
      effectExecutor.inputSchema(Schema.Struct({ name: Schema.String })).task({
        id: 'test',
        timeoutMs: 1000,
        run: (_, input) => {
          executionCount++
          return Effect.succeed(input.name)
        },
      }),
    )

    const handle = await Effect.runPromiseWith(runtime)(
      effectExecutor.enqueueTask(task, { name: 'test' }),
    )

    const finishedExecution = await Effect.runPromiseWith(runtime)(
      handle.waitAndGetFinishedExecution({
        pollingIntervalMs: 100,
      }),
    )
    expect(executionCount).toBe(1)
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

  it('should complete sleeping task', async () => {
    const sleepingTask = Effect.runSyncWith(runtime)(
      effectExecutor.sleepingTask<string>({
        id: 'test',
        timeoutMs: 1000,
      }),
    )

    const handle = await Effect.runPromiseWith(runtime)(
      effectExecutor.enqueueTask(sleepingTask, 'test_unique_id'),
    )

    let finishedExecution = await Effect.runPromiseWith(runtime)(
      effectExecutor.wakeupSleepingTaskExecution(sleepingTask, 'test_unique_id', {
        status: 'completed',
        output: 'test_output',
      }),
    )
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.output).toBe('test_output')

    finishedExecution = await Effect.runPromiseWith(runtime)(
      handle.waitAndGetFinishedExecution({
        pollingIntervalMs: 100,
      }),
    )
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.output).toBe('test_output')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete parent task', async () => {
    let executionCount = 0
    const child = Effect.runSyncWith(runtime)(
      effectExecutor.inputSchema(Schema.Struct({ name: Schema.String })).task({
        id: 'child',
        timeoutMs: 1000,
        run: (_, input) => {
          executionCount++
          return Effect.succeed(input.name)
        },
      }),
    )

    const parentTask = Effect.runSyncWith(runtime)(
      effectExecutor.inputSchema(Schema.Struct({ name: Schema.String })).parentTask({
        id: 'parent',
        timeoutMs: 1000,
        runParent: (_, input) => {
          executionCount++
          return Effect.succeed({
            output: 'parent',
            children: [childTask(child, input)],
          })
        },
        finalize: ({ output, children }) => {
          executionCount++
          return Effect.succeed(
            `${output}_${children.map((c) => (c.status === 'completed' ? c.output : '')).join('_')}`,
          )
        },
      }),
    )

    const handle = await Effect.runPromiseWith(runtime)(
      effectExecutor.enqueueTask(parentTask, { name: 'test' }),
    )

    const finishedExecution = await Effect.runPromiseWith(runtime)(
      handle.waitAndGetFinishedExecution({
        pollingIntervalMs: 100,
      }),
    )
    expect(executionCount).toBe(3)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.taskId).toBe('parent')
    expect(finishedExecution.executionId).toMatch(/^te_/)
    expect(finishedExecution.output).toBe('parent_test')
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete sequential tasks', async () => {
    let executionCount = 0
    const task1 = Effect.runSyncWith(runtime)(
      effectExecutor.inputSchema(Schema.String).task({
        id: 'task1',
        timeoutMs: 1000,
        run: (_, input) => {
          executionCount++
          return Effect.succeed(input.length)
        },
      }),
    )

    const task2 = Effect.runSyncWith(runtime)(
      effectExecutor.inputSchema(Schema.Int).task({
        id: 'task2',
        timeoutMs: 1000,
        run: (_, input) => {
          executionCount++
          return Effect.succeed(input > 5)
        },
      }),
    )

    const task3 = Effect.runSyncWith(runtime)(
      effectExecutor.inputSchema(Schema.Boolean).task({
        id: 'task3',
        timeoutMs: 1000,
        run: (_, input) => {
          executionCount++
          return Effect.succeed(input)
        },
      }),
    )

    const sequentialTask = Effect.runSyncWith(runtime)(
      effectExecutor.sequentialTasks('seq', [task1, task2, task3]),
    )

    let handle = await Effect.runPromiseWith(runtime)(
      effectExecutor.enqueueTask(sequentialTask, 'test'),
    )

    let finishedExecution = await Effect.runPromiseWith(runtime)(
      handle.waitAndGetFinishedExecution({
        pollingIntervalMs: 100,
      }),
    )
    expect(executionCount).toBe(3)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.output).toBe(false)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )

    handle = await Effect.runPromiseWith(runtime)(
      effectExecutor.enqueueTask(sequentialTask, 'test_longer'),
    )

    finishedExecution = await Effect.runPromiseWith(runtime)(
      handle.waitAndGetFinishedExecution({
        pollingIntervalMs: 100,
      }),
    )
    expect(executionCount).toBe(6)
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.output).toBe(true)
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })

  it('should complete looping task', async () => {
    let value: number | undefined
    setTimeout(() => {
      value = 10
    }, 500)

    const iterationTask = Effect.runSyncWith(runtime)(
      effectExecutor.task({
        id: 'iteration',
        timeoutMs: 1000,
        run: () => {
          if (value != null) {
            return {
              isDone: true,
              output: value,
            } as const
          }

          return {
            isDone: false,
          } as const
        },
      }),
    )

    const loopingTask = Effect.runSyncWith(runtime)(
      effectExecutor.loopingTask('looping', iterationTask, 50),
    )

    const handle = await Effect.runPromiseWith(runtime)(effectExecutor.enqueueTask(loopingTask))

    const finishedExecution = await Effect.runPromiseWith(runtime)(
      handle.waitAndGetFinishedExecution({
        pollingIntervalMs: 100,
      }),
    )
    expect(finishedExecution.status).toBe('completed')
    assert(finishedExecution.status === 'completed')
    expect(finishedExecution.output).toStrictEqual({
      isSuccess: true,
      output: 10,
    })
    expect(finishedExecution.startedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt).toBeInstanceOf(Date)
    expect(finishedExecution.finishedAt.getTime()).toBeGreaterThanOrEqual(
      finishedExecution.startedAt.getTime(),
    )
  })
})
