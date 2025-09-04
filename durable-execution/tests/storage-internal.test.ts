import { Effect, Exit, Runtime, Scope } from 'effect'

import { sleep } from '@gpahal/std/promises'

import { DurableExecutionError } from '../src/errors'
import { InMemoryTaskExecutionsStorage } from '../src/in-memory-storage'
import {
  TaskExecutionsStorageService,
  type TaskExecutionsStorage,
  type TaskExecutionStorageValue,
} from '../src/storage'
import {
  makeTaskExecutionsStorageInternal,
  type TaskExecutionsStorageInternal,
  type TaskExecutionsStorageInternalOptions,
} from '../src/storage-internal'

function createMockStorageValue(
  overrides?: Partial<TaskExecutionStorageValue>,
): TaskExecutionStorageValue {
  const now = Date.now()
  return {
    taskId: 'task-1',
    executionId: 'exec-1',
    isSleepingTask: false,
    retryOptions: { maxAttempts: 3 },
    sleepMsBeforeRun: 0,
    timeoutMs: 30_000,
    areChildrenSequential: false,
    input: '{}',
    status: 'ready',
    isFinished: false,
    retryAttempts: 0,
    startAt: now,
    activeChildrenCount: 0,
    onChildrenFinishedProcessingStatus: 'idle',
    closeStatus: 'idle',
    needsPromiseCancellation: false,
    createdAt: now,
    updatedAt: now,
    ...overrides,
  }
}

describe('TaskExecutionsStorageInternal', () => {
  let storage: TaskExecutionsStorage
  let storageInternal: TaskExecutionsStorageInternal
  let scope: Scope.CloseableScope
  let runtime: Runtime.Runtime<never>
  let makeStorageInternal: (
    options: TaskExecutionsStorageInternalOptions & {
      storage?: TaskExecutionsStorage
    },
  ) => Promise<void>

  beforeEach(() => {
    storage = new InMemoryTaskExecutionsStorage()

    makeStorageInternal = async (
      options: TaskExecutionsStorageInternalOptions & {
        storage?: TaskExecutionsStorage
      },
    ) => {
      const { storageInternalLocal, scopeLocal, runtimeLocal } = await Effect.runPromise(
        Effect.gen(function* () {
          const scopeLocal = yield* Scope.make()

          const effect = makeTaskExecutionsStorageInternal(options).pipe(
            Effect.provideService(TaskExecutionsStorageService, options.storage ?? storage),
          )
          const storageInternalLocal = yield* effect.pipe(
            Effect.provideService(Scope.Scope, scopeLocal),
          )
          const runtimeLocal = yield* Effect.runtime<never>()
          return { storageInternalLocal, scopeLocal, runtimeLocal }
        }),
      )

      storageInternal = storageInternalLocal
      scope = scopeLocal
      runtime = runtimeLocal
    }
  })

  afterEach(async () => {
    if (runtime && storageInternal) {
      await Runtime.runPromise(runtime, storageInternal.shutdown)
    }
    if (scope) {
      await Effect.runPromise(Scope.close(scope, Exit.void))
    }
  })

  describe('initialization', () => {
    it('should initialize with batching disabled', async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      expect(storageInternal).toBeDefined()
      expect(Runtime.runSync(runtime, storageInternal.getMetrics)).toBeDefined()
    })

    it('should initialize with batching enabled', async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: true,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      expect(storageInternal).toBeDefined()
      expect(Runtime.runSync(runtime, storageInternal.getMetrics)).toBeDefined()
    })

    it('should handle custom max retry attempts', async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
        maxRetryAttempts: 5,
      })
      expect(storageInternal).toBeDefined()
    })
  })

  describe('insertMany', () => {
    it('should handle empty executions array', async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      await expect(
        Runtime.runPromise(runtime, storageInternal.insertMany([])),
      ).resolves.toBeUndefined()
    })

    it('should insert single execution without batching', async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      const execution = createMockStorageValue()
      await Runtime.runPromise(runtime, storageInternal.insertMany([execution]))

      const result = await Runtime.runPromise(
        runtime,
        storageInternal.getById({ executionId: execution.executionId, filters: {} }),
      )
      expect(result).toEqual(execution)
    })

    it('should insert multiple executions without batching', async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      const executions = [
        createMockStorageValue({ executionId: 'exec-1' }),
        createMockStorageValue({ executionId: 'exec-2' }),
        createMockStorageValue({ executionId: 'exec-3' }),
      ]
      await Runtime.runPromise(runtime, storageInternal.insertMany(executions))

      for (const execution of executions) {
        const result = await Runtime.runPromise(
          runtime,
          storageInternal.getById({ executionId: execution.executionId, filters: {} }),
        )
        expect(result).toEqual(execution)
      }
    })

    it('should batch small inserts when batching is enabled', async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: true,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      await Runtime.runPromise(runtime, storageInternal.start)

      const execution1 = createMockStorageValue({ executionId: 'exec-1' })
      const execution2 = createMockStorageValue({ executionId: 'exec-2' })

      const promise1 = Runtime.runPromise(runtime, storageInternal.insertMany([execution1]))
      const promise2 = Runtime.runPromise(runtime, storageInternal.insertMany([execution2]))

      await Promise.all([promise1, promise2])

      const result1 = await Runtime.runPromise(
        runtime,
        storageInternal.getById({ executionId: 'exec-1', filters: {} }),
      )
      const result2 = await Runtime.runPromise(
        runtime,
        storageInternal.getById({ executionId: 'exec-2', filters: {} }),
      )
      expect(result1).toEqual(execution1)
      expect(result2).toEqual(execution2)
    })

    it('should not batch large inserts even when batching is enabled', async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: true,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      await Runtime.runPromise(runtime, storageInternal.start)

      const executions = Array.from({ length: 20 }, (_, i) =>
        createMockStorageValue({ executionId: `exec-${i}` }),
      )
      await Runtime.runPromise(runtime, storageInternal.insertMany(executions))

      const validateExecutions = async (execution: TaskExecutionStorageValue) => {
        const result = await Runtime.runPromise(
          runtime,
          storageInternal.getById({ executionId: execution.executionId, filters: {} }),
        )
        expect(result).toEqual(execution)
      }

      await Promise.all(executions.map(validateExecutions))
    })
  })

  describe('getById', () => {
    beforeEach(async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      const execution = createMockStorageValue()
      await Runtime.runPromise(runtime, storageInternal.insertMany([execution]))
    })

    it('should get execution by id', async () => {
      const result = await Runtime.runPromise(
        runtime,
        storageInternal.getById({ executionId: 'exec-1' }),
      )
      expect(result).toBeDefined()
      expect(result?.executionId).toBe('exec-1')
    })

    it('should return undefined for non-existent execution', async () => {
      const result = await Runtime.runPromise(
        runtime,
        storageInternal.getById({ executionId: 'non-existent' }),
      )
      expect(result).toBeUndefined()
    })

    it('should apply filters correctly', async () => {
      const result = await Runtime.runPromise(
        runtime,
        storageInternal.getById({
          executionId: 'exec-1',
          filters: { status: 'ready' },
        }),
      )
      expect(result).toBeDefined()

      const result2 = await Runtime.runPromise(
        runtime,
        storageInternal.getById({
          executionId: 'exec-1',
          filters: { status: 'completed' },
        }),
      )
      expect(result2).toBeUndefined()
    })

    it('should batch getById calls when batching is enabled', async () => {
      await Runtime.runPromise(runtime, storageInternal.shutdown)
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: true,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      await Runtime.runPromise(runtime, storageInternal.start)

      const promise1 = Runtime.runPromise(
        runtime,
        storageInternal.getById({ executionId: 'exec-1', filters: {} }),
      )
      const promise2 = Runtime.runPromise(
        runtime,
        storageInternal.getById({
          executionId: 'exec-1',
          filters: { status: 'ready' },
        }),
      )

      const [result1, result2] = await Promise.all([promise1, promise2])
      expect(result1).toBeDefined()
      expect(result2).toBeDefined()
    })
  })

  describe('getBySleepingTaskUniqueId', () => {
    beforeEach(async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      const execution = createMockStorageValue({
        isSleepingTask: true,
        sleepingTaskUniqueId: 'sleep-1',
      })
      await storage.insertMany([execution])
    })

    it('should get execution by sleeping task unique id', async () => {
      const result = await Runtime.runPromise(
        runtime,
        storageInternal.getBySleepingTaskUniqueId({
          sleepingTaskUniqueId: 'sleep-1',
        }),
      )
      expect(result).toBeDefined()
      expect(result?.sleepingTaskUniqueId).toBe('sleep-1')
    })

    it('should return undefined for non-existent sleeping task', async () => {
      const result = await Runtime.runPromise(
        runtime,
        storageInternal.getBySleepingTaskUniqueId({
          sleepingTaskUniqueId: 'non-existent',
        }),
      )
      expect(result).toBeUndefined()
    })
  })

  describe('updateById', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      const execution = createMockStorageValue()
      await Runtime.runPromise(runtime, storageInternal.insertMany([execution]))
    })

    it('should update execution by id', async () => {
      await Runtime.runPromise(
        runtime,
        storageInternal.updateById(now, {
          executionId: 'exec-1',
          update: { status: 'running' },
        }),
      )

      const result = await Runtime.runPromise(
        runtime,
        storageInternal.getById({ executionId: 'exec-1', filters: {} }),
      )
      expect(result?.status).toBe('running')
    })

    it('should apply filters when updating', async () => {
      await Runtime.runPromise(
        runtime,
        storageInternal.updateById(now, {
          executionId: 'exec-1',
          filters: { status: 'ready' },
          update: { status: 'running' },
        }),
      )

      const result = await Runtime.runPromise(
        runtime,
        storageInternal.getById({ executionId: 'exec-1', filters: {} }),
      )
      expect(result?.status).toBe('running')
    })

    it('should handle finalize of parent with completed status', async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: true,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      await Runtime.runPromise(runtime, storageInternal.start)
      const execution = createMockStorageValue({
        executionId: 'finalize-exec',
        parent: {
          taskId: 'parent-task',
          executionId: 'parent-exec',
          indexInParentChildren: 0,
          isOnlyChildOfParent: false,
          isFinalizeOfParent: true,
        },
      })
      await Runtime.runPromise(runtime, storageInternal.insertMany([execution]))

      await Runtime.runPromise(
        runtime,
        storageInternal.updateById(
          now,
          {
            executionId: 'finalize-exec',
            update: { status: 'completed' },
          },
          execution,
        ),
      )

      const result = await storage.getManyById([{ executionId: 'finalize-exec', filters: {} }])
      expect(result[0]?.closeStatus).toBe('ready')
    })

    it('should batch updateById calls when batching is enabled', async () => {
      await Runtime.runPromise(runtime, storageInternal.shutdown)
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: true,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      await Runtime.runPromise(runtime, storageInternal.start)

      const promise1 = Runtime.runPromise(
        runtime,
        storageInternal.updateById(now, {
          executionId: 'exec-1',
          update: { status: 'running' },
        }),
      )
      const promise2 = Runtime.runPromise(
        runtime,
        storageInternal.updateById(now + 1, {
          executionId: 'exec-1',
          update: { executorId: 'executor-1' },
        }),
      )

      await Promise.all([promise1, promise2])

      const result = await Runtime.runPromise(
        runtime,
        storageInternal.getById({ executionId: 'exec-1', filters: {} }),
      )
      expect(result?.status).toBe('running')
      expect(result?.executorId).toBe('executor-1')
    })
  })

  describe('updateByIdAndInsertChildrenIfUpdated', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      const execution = createMockStorageValue()
      await Runtime.runPromise(runtime, storageInternal.insertMany([execution]))
    })

    it('should update execution without children', async () => {
      await Runtime.runPromise(
        runtime,
        storageInternal.updateByIdAndInsertChildrenIfUpdated(now, {
          executionId: 'exec-1',
          update: { status: 'waiting_for_children' },
          childrenTaskExecutionsToInsertIfAnyUpdated: [],
        }),
      )

      const result = await Runtime.runPromise(
        runtime,
        storageInternal.getById({ executionId: 'exec-1', filters: {} }),
      )
      expect(result?.status).toBe('waiting_for_children')
    })

    it('should update execution and insert children', async () => {
      const children = [
        createMockStorageValue({ executionId: 'child-1' }),
        createMockStorageValue({ executionId: 'child-2' }),
      ]

      await Runtime.runPromise(
        runtime,
        storageInternal.updateByIdAndInsertChildrenIfUpdated(now, {
          executionId: 'exec-1',
          update: { status: 'waiting_for_children' },
          childrenTaskExecutionsToInsertIfAnyUpdated: children,
        }),
      )

      const parent = await Runtime.runPromise(
        runtime,
        storageInternal.getById({ executionId: 'exec-1', filters: {} }),
      )
      expect(parent?.status).toBe('waiting_for_children')

      const child1 = await Runtime.runPromise(
        runtime,
        storageInternal.getById({ executionId: 'child-1', filters: {} }),
      )
      expect(child1).toBeDefined()

      const child2 = await Runtime.runPromise(
        runtime,
        storageInternal.getById({ executionId: 'child-2', filters: {} }),
      )
      expect(child2).toBeDefined()
    })

    it('should not batch when children count is >= 3', async () => {
      await Runtime.runPromise(runtime, storageInternal.shutdown)
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: true,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      await Runtime.runPromise(runtime, storageInternal.start)

      const children = [
        createMockStorageValue({ executionId: 'child-1' }),
        createMockStorageValue({ executionId: 'child-2' }),
        createMockStorageValue({ executionId: 'child-3' }),
      ]

      await Runtime.runPromise(
        runtime,
        storageInternal.updateByIdAndInsertChildrenIfUpdated(now, {
          executionId: 'exec-1',
          update: { status: 'waiting_for_children' },
          childrenTaskExecutionsToInsertIfAnyUpdated: children,
        }),
      )

      for (const child of children) {
        const result = await Runtime.runPromise(
          runtime,
          storageInternal.getById({ executionId: child.executionId, filters: {} }),
        )
        expect(result).toBeDefined()
      }
    })
  })

  describe('updateByParentExecutionIdAndIsFinished', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      const parent = createMockStorageValue({
        executionId: 'parent-1',
        status: 'waiting_for_children',
      })
      const child1 = createMockStorageValue({
        executionId: 'child-1',
        parent: {
          taskId: 'parent-task',
          executionId: 'parent-1',
          indexInParentChildren: 0,
          isOnlyChildOfParent: false,
          isFinalizeOfParent: false,
        },
      })
      const child2 = createMockStorageValue({
        executionId: 'child-2',
        parent: {
          taskId: 'parent-task',
          executionId: 'parent-1',
          indexInParentChildren: 1,
          isOnlyChildOfParent: false,
          isFinalizeOfParent: false,
        },
      })
      await Runtime.runPromise(runtime, storageInternal.insertMany([parent, child1, child2]))
    })

    it('should update children by parent execution id and finished status', async () => {
      await Runtime.runPromise(
        runtime,
        storageInternal.updateByParentExecutionIdAndIsFinished(now, {
          parentExecutionId: 'parent-1',
          isFinished: false,
          update: { closeStatus: 'ready' },
        }),
      )

      const children = await Runtime.runPromise(
        runtime,
        storageInternal.getByParentExecutionId({ parentExecutionId: 'parent-1' }),
      )
      expect(children).toHaveLength(2)
      for (const child of children) {
        expect(child.closeStatus).toBe('ready')
      }
    })

    it('should batch updates when batching is enabled', async () => {
      await Runtime.runPromise(runtime, storageInternal.shutdown)
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: true,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      await Runtime.runPromise(runtime, storageInternal.start)

      const promise1 = Runtime.runPromise(
        runtime,
        storageInternal.updateByParentExecutionIdAndIsFinished(now, {
          parentExecutionId: 'parent-1',
          isFinished: false,
          update: { closeStatus: 'ready' },
        }),
      )
      const promise2 = Runtime.runPromise(
        runtime,
        storageInternal.updateByParentExecutionIdAndIsFinished(now + 1, {
          parentExecutionId: 'parent-1',
          isFinished: false,
          update: { closeStatus: 'closing' },
        }),
      )

      await Promise.all([promise1, promise2])

      const children = await Runtime.runPromise(
        runtime,
        storageInternal.getByParentExecutionId({ parentExecutionId: 'parent-1' }),
      )
      expect(children).toHaveLength(2)
    })
  })

  describe('getByParentExecutionId', () => {
    beforeEach(async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      const parent = createMockStorageValue({ executionId: 'parent-1' })
      const child1 = createMockStorageValue({
        executionId: 'child-1',
        parent: {
          taskId: 'parent-task',
          executionId: 'parent-1',
          indexInParentChildren: 0,
          isOnlyChildOfParent: false,
          isFinalizeOfParent: false,
        },
      })
      const child2 = createMockStorageValue({
        executionId: 'child-2',
        parent: {
          taskId: 'parent-task',
          executionId: 'parent-1',
          indexInParentChildren: 1,
          isOnlyChildOfParent: false,
          isFinalizeOfParent: false,
        },
      })
      await Runtime.runPromise(runtime, storageInternal.insertMany([parent, child1, child2]))
    })

    it('should get children by parent execution id', async () => {
      const children = await Runtime.runPromise(
        runtime,
        storageInternal.getByParentExecutionId({ parentExecutionId: 'parent-1' }),
      )
      expect(children).toHaveLength(2)
      expect(children[0]?.executionId).toBe('child-1')
      expect(children[1]?.executionId).toBe('child-2')
    })

    it('should return empty array for non-existent parent', async () => {
      const children = await Runtime.runPromise(
        runtime,
        storageInternal.getByParentExecutionId({ parentExecutionId: 'non-existent' }),
      )
      expect(children).toEqual([])
    })

    it('should batch requests when batching is enabled', async () => {
      await Runtime.runPromise(runtime, storageInternal.shutdown)
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: true,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      await Runtime.runPromise(runtime, storageInternal.start)

      const promise1 = Runtime.runPromise(
        runtime,
        storageInternal.getByParentExecutionId({ parentExecutionId: 'parent-1' }),
      )
      const promise2 = Runtime.runPromise(
        runtime,
        storageInternal.getByParentExecutionId({ parentExecutionId: 'parent-1' }),
      )

      const [children1, children2] = await Promise.all([promise1, promise2])
      expect(children1).toHaveLength(2)
      expect(children2).toHaveLength(2)
    })
  })

  describe('retry logic', () => {
    it('should retry on retryable errors', async () => {
      let callCount = 0
      const mockStorage: TaskExecutionsStorage = {
        insertMany: vi.fn(() => {
          callCount++
          if (callCount < 3) {
            throw DurableExecutionError.retryable('Temporary error')
          }
        }),
        getManyById: vi.fn(() => []),
        getManyBySleepingTaskUniqueId: vi.fn(() => []),
        updateManyById: vi.fn(),
        updateManyByIdAndInsertChildrenIfUpdated: vi.fn(),
        updateByStatusAndStartAtLessThanAndReturn: vi.fn(() => []),
        updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn:
          vi.fn(() => []),
        updateByCloseStatusAndReturn: vi.fn(() => []),
        updateByStatusAndIsSleepingTaskAndExpiresAtLessThan: vi.fn(() => 0),
        updateByOnChildrenFinishedProcessingExpiresAtLessThan: vi.fn(() => 0),
        updateByCloseExpiresAtLessThan: vi.fn(() => 0),
        updateByExecutorIdAndNeedsPromiseCancellationAndReturn: vi.fn(() => []),
        getManyByParentExecutionId: vi.fn(() => []),
        updateManyByParentExecutionIdAndIsFinished: vi.fn(),
        updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus: vi.fn(() => 0),
        deleteById: vi.fn(),
        deleteAll: vi.fn(),
      }

      await makeStorageInternal({
        executorId: 'test-executor-id',
        storage: mockStorage,
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
        maxRetryAttempts: 3,
      })
      const execution = createMockStorageValue()
      await Runtime.runPromise(runtime, storageInternal.insertMany([execution]))

      expect(callCount).toBe(3)
      expect(mockStorage.insertMany).toHaveBeenCalledTimes(3)
    })

    it('should not retry on non-retryable errors', async () => {
      let callCount = 0
      const mockStorage: TaskExecutionsStorage = {
        insertMany: vi.fn(() => {
          callCount++
          throw DurableExecutionError.nonRetryable('Permanent error')
        }),
        getManyById: vi.fn(() => []),
        getManyBySleepingTaskUniqueId: vi.fn(() => []),
        updateManyById: vi.fn(),
        updateManyByIdAndInsertChildrenIfUpdated: vi.fn(),
        updateByStatusAndStartAtLessThanAndReturn: vi.fn(() => []),
        updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn:
          vi.fn(() => []),
        updateByCloseStatusAndReturn: vi.fn(() => []),
        updateByStatusAndIsSleepingTaskAndExpiresAtLessThan: vi.fn(() => 0),
        updateByOnChildrenFinishedProcessingExpiresAtLessThan: vi.fn(() => 0),
        updateByCloseExpiresAtLessThan: vi.fn(() => 0),
        updateByExecutorIdAndNeedsPromiseCancellationAndReturn: vi.fn(() => []),
        getManyByParentExecutionId: vi.fn(() => []),
        updateManyByParentExecutionIdAndIsFinished: vi.fn(),
        updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus: vi.fn(() => 0),
        deleteById: vi.fn(),
        deleteAll: vi.fn(),
      }

      await makeStorageInternal({
        executorId: 'test-executor-id',
        storage: mockStorage,
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
        maxRetryAttempts: 3,
      })
      const execution = createMockStorageValue()

      await expect(
        Runtime.runPromise(runtime, storageInternal.insertMany([execution])),
      ).rejects.toThrow('Permanent error')
      expect(callCount).toBe(1)
      expect(mockStorage.insertMany).toHaveBeenCalledTimes(1)
    })

    it('should fail after max retry attempts', async () => {
      let executionCount = 0
      const mockStorage: TaskExecutionsStorage = {
        insertMany: vi.fn(() => {
          executionCount++
          throw DurableExecutionError.retryable('Temporary error')
        }),
        getManyById: vi.fn(() => []),
        getManyBySleepingTaskUniqueId: vi.fn(() => []),
        updateManyById: vi.fn(),
        updateManyByIdAndInsertChildrenIfUpdated: vi.fn(),
        updateByStatusAndStartAtLessThanAndReturn: vi.fn(() => []),
        updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn:
          vi.fn(() => []),
        updateByCloseStatusAndReturn: vi.fn(() => []),
        updateByStatusAndIsSleepingTaskAndExpiresAtLessThan: vi.fn(() => 0),
        updateByOnChildrenFinishedProcessingExpiresAtLessThan: vi.fn(() => 0),
        updateByCloseExpiresAtLessThan: vi.fn(() => 0),
        updateByExecutorIdAndNeedsPromiseCancellationAndReturn: vi.fn(() => []),
        getManyByParentExecutionId: vi.fn(() => []),
        updateManyByParentExecutionIdAndIsFinished: vi.fn(),
        updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus: vi.fn(() => 0),
        deleteById: vi.fn(),
        deleteAll: vi.fn(),
      }

      await makeStorageInternal({
        executorId: 'test-executor-id',
        storage: mockStorage,
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
        maxRetryAttempts: 2,
      })
      const execution = createMockStorageValue()

      await expect(
        Runtime.runPromise(runtime, storageInternal.insertMany([execution])),
      ).rejects.toThrow('Temporary error')
      expect(executionCount).toBe(3)
      expect(mockStorage.insertMany).toHaveBeenCalledTimes(3)
    })
  })

  describe('shutdown', () => {
    it('should handle shutdown without background processes', async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      await expect(Runtime.runPromise(runtime, storageInternal.shutdown)).resolves.toBeUndefined()
    })

    it('should handle shutdown with background processes', async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: true,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      await Runtime.runPromise(runtime, storageInternal.start)
      await sleep(10)
      await expect(Runtime.runPromise(runtime, storageInternal.shutdown)).resolves.toBeUndefined()
    })

    it('should handle shutdown with pending requests', async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: true,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      await Runtime.runPromise(runtime, storageInternal.start)

      const promises = []
      for (let i = 0; i < 10; i++) {
        promises.push(
          Runtime.runPromise(
            runtime,
            storageInternal.insertMany([
              createMockStorageValue({ executionId: `shutdown-test-${i}` }),
            ]),
          ),
        )
      }

      await expect(Runtime.runPromise(runtime, storageInternal.shutdown)).resolves.toBeUndefined()
    })

    it('should not throw if operations are attempted after shutdown', async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      await Runtime.runPromise(runtime, storageInternal.shutdown)

      await expect(Runtime.runPromise(runtime, storageInternal.start)).resolves.not.toThrow()
    })
  })

  describe('timing stats', () => {
    it('should track timing stats for operations', async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      const execution = createMockStorageValue()

      await Runtime.runPromise(runtime, storageInternal.insertMany([execution]))
      await Runtime.runPromise(runtime, storageInternal.getById({ executionId: 'exec-1' }))

      expect(
        (await Runtime.runPromise(runtime, storageInternal.getMetrics)).some(
          (a) => a.processName === 'insertMany',
        ),
      ).toBe(true)
      expect(
        (await Runtime.runPromise(runtime, storageInternal.getMetrics)).some(
          (a) => a.processName === 'getManyById',
        ),
      ).toBe(true)

      const insertStats = (await Runtime.runPromise(runtime, storageInternal.getMetrics)).find(
        (a) => a.processName === 'insertMany',
      )
      expect(insertStats?.count).toBeGreaterThan(0)
      expect(insertStats?.max).toBeGreaterThanOrEqual(0)

      const getStats = (await Runtime.runPromise(runtime, storageInternal.getMetrics)).find(
        (a) => a.processName === 'getManyById',
      )
      expect(getStats?.count).toBeGreaterThan(0)
      expect(getStats?.max).toBeGreaterThanOrEqual(0)
    })
  })

  describe('updateByStatusAndStartAtLessThanAndReturn', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      const executions = [
        createMockStorageValue({ executionId: 'exec-1', status: 'ready', startAt: now - 1000 }),
        createMockStorageValue({ executionId: 'exec-2', status: 'ready', startAt: now - 500 }),
        createMockStorageValue({ executionId: 'exec-3', status: 'ready', startAt: now + 1000 }),
      ]
      await Runtime.runPromise(runtime, storageInternal.insertMany(executions))
    })

    it('should update executions by status and startAt', async () => {
      const updated = await Runtime.runPromise(
        runtime,
        storageInternal.updateByStatusAndStartAtLessThanAndReturn(now, {
          status: 'ready',
          startAtLessThan: now,
          update: { status: 'running', executorId: 'executor-1' },
          updateExpiresAtWithStartedAt: now,
          limit: 10,
        }),
      )

      expect(updated).toHaveLength(2)
      expect(updated[0]?.executionId).toBe('exec-1')
      expect(updated[1]?.executionId).toBe('exec-2')

      for (const execution of updated) {
        expect(execution.status).toBe('running')
        expect(execution.executorId).toBe('executor-1')
      }
    })

    it('should respect limit parameter', async () => {
      const updated = await Runtime.runPromise(
        runtime,
        storageInternal.updateByStatusAndStartAtLessThanAndReturn(now, {
          status: 'ready',
          startAtLessThan: now,
          update: { status: 'running' },
          updateExpiresAtWithStartedAt: now,
          limit: 1,
        }),
      )

      expect(updated).toHaveLength(1)
      expect(updated[0]?.executionId).toBe('exec-1')
    })
  })

  describe('updateByCloseStatusAndReturn', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      const executions = [
        createMockStorageValue({ executionId: 'exec-1', closeStatus: 'ready' }),
        createMockStorageValue({ executionId: 'exec-2', closeStatus: 'ready' }),
        createMockStorageValue({ executionId: 'exec-3', closeStatus: 'idle' }),
      ]
      await Runtime.runPromise(runtime, storageInternal.insertMany(executions))
    })

    it('should update executions by close status', async () => {
      const updated = await Runtime.runPromise(
        runtime,
        storageInternal.updateByCloseStatusAndReturn(now, {
          closeStatus: 'ready',
          update: { closeStatus: 'closing' },
          limit: 10,
        }),
      )

      expect(updated).toHaveLength(2)
      for (const execution of updated) {
        expect(execution.closeStatus).toBe('closing')
      }
    })
  })

  describe('updateByStatusAndIsSleepingTaskAndExpiresAtLessThan', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      const executions = [
        createMockStorageValue({
          executionId: 'sleep-1',
          status: 'running',
          isSleepingTask: true,
          sleepingTaskUniqueId: 'sleep-1',
          expiresAt: now - 1000,
        }),
        createMockStorageValue({
          executionId: 'sleep-2',
          status: 'running',
          isSleepingTask: true,
          sleepingTaskUniqueId: 'sleep-2',
          expiresAt: now + 1000,
        }),
      ]
      await Runtime.runPromise(runtime, storageInternal.insertMany(executions))
    })

    it('should update sleeping tasks by expiry time', async () => {
      const count = await Runtime.runPromise(
        runtime,
        storageInternal.updateByStatusAndIsSleepingTaskAndExpiresAtLessThan(now, {
          status: 'running',
          isSleepingTask: true,
          expiresAtLessThan: now,
          update: { status: 'timed_out' },
          limit: 10,
        }),
      )

      expect(count).toBe(1)

      const result = await storage.getManyById([{ executionId: 'sleep-1', filters: {} }])
      expect(result[0]?.status).toBe('timed_out')
    })
  })

  describe('updateByExecutorIdAndNeedsPromiseCancellationAndReturn', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      const executions = [
        createMockStorageValue({
          executionId: 'exec-1',
          executorId: 'executor-1',
          needsPromiseCancellation: true,
        }),
        createMockStorageValue({
          executionId: 'exec-2',
          executorId: 'executor-1',
          needsPromiseCancellation: false,
        }),
        createMockStorageValue({
          executionId: 'exec-3',
          executorId: 'executor-2',
          needsPromiseCancellation: true,
        }),
      ]
      await Runtime.runPromise(runtime, storageInternal.insertMany(executions))
    })

    it('should update executions by executor id and needs promise cancellation', async () => {
      const updated = await Runtime.runPromise(
        runtime,
        storageInternal.updateByExecutorIdAndNeedsPromiseCancellationAndReturn(now, {
          executorId: 'executor-1',
          needsPromiseCancellation: true,
          update: { needsPromiseCancellation: false },
          limit: 10,
        }),
      )

      expect(updated).toHaveLength(1)
      expect(updated[0]?.executionId).toBe('exec-1')
      expect(updated[0]?.needsPromiseCancellation).toBe(false)
    })
  })

  describe('background processes', () => {
    it('should handle background processes lifecycle', async () => {
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: true,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })

      await expect(Runtime.runPromise(runtime, storageInternal.start)).resolves.not.toThrow()

      await sleep(50)

      await expect(Runtime.runPromise(runtime, storageInternal.shutdown)).resolves.toBeUndefined()
    })

    it('should handle errors in background batch processing', async () => {
      const mockStorage: TaskExecutionsStorage = {
        insertMany: vi.fn(() => {
          throw new Error('Test error')
        }),
        getManyById: vi.fn(() => []),
        getManyBySleepingTaskUniqueId: vi.fn(() => []),
        updateManyById: vi.fn(),
        updateManyByIdAndInsertChildrenIfUpdated: vi.fn(),
        updateByStatusAndStartAtLessThanAndReturn: vi.fn(() => []),
        updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn:
          vi.fn(() => []),
        updateByCloseStatusAndReturn: vi.fn(() => []),
        updateByStatusAndIsSleepingTaskAndExpiresAtLessThan: vi.fn(() => 0),
        updateByOnChildrenFinishedProcessingExpiresAtLessThan: vi.fn(() => 0),
        updateByCloseExpiresAtLessThan: vi.fn(() => 0),
        updateByExecutorIdAndNeedsPromiseCancellationAndReturn: vi.fn(() => []),
        getManyByParentExecutionId: vi.fn(() => []),
        updateManyByParentExecutionIdAndIsFinished: vi.fn(),
        updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus: vi.fn(() => 0),
        deleteById: vi.fn(),
        deleteAll: vi.fn(),
      }

      await makeStorageInternal({
        executorId: 'test-executor-id',
        storage: mockStorage,
        enableBatching: true,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      await Runtime.runPromise(runtime, storageInternal.start)

      const execution = createMockStorageValue()
      await expect(
        Runtime.runPromise(runtime, storageInternal.insertMany([execution])),
      ).rejects.toThrow('Test error')

      await Runtime.runPromise(runtime, storageInternal.shutdown)
    })
  })

  describe('updateByOnChildrenFinishedProcessingExpiresAtLessThan', () => {
    it('should handle update by on children finished processing expires at less than', async () => {
      const mockValue = createMockStorageValue({
        onChildrenFinishedProcessingExpiresAt: Date.now() + 1000,
      })
      await storage.insertMany([mockValue])

      const futureTime = Date.now() + 2000
      await makeStorageInternal({
        executorId: 'test-executor-id',
        enableBatching: false,
        enableStats: true,
        backgroundBatchingIntraBatchSleepMs: 10,
      })
      const count = await Runtime.runPromise(
        runtime,
        storageInternal.updateByOnChildrenFinishedProcessingExpiresAtLessThan(Date.now(), {
          onChildrenFinishedProcessingExpiresAtLessThan: futureTime,
          update: { status: 'failed' },
          limit: 10,
        }),
      )

      expect(count).toBe(1)
    })

    it('should handle update by on children finished processing expires at less than with no matches', async () => {
      const mockValue = createMockStorageValue()
      await storage.insertMany([mockValue])

      const pastTime = Date.now() - 1000
      const count = await Runtime.runPromise(
        runtime,
        storageInternal.updateByOnChildrenFinishedProcessingExpiresAtLessThan(Date.now(), {
          onChildrenFinishedProcessingExpiresAtLessThan: pastTime,
          update: { status: 'failed' },
          limit: 10,
        }),
      )

      expect(count).toBe(0)
    })
  })
})
