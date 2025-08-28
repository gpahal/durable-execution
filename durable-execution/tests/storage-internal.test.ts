import { createCancelSignal } from '@gpahal/std/cancel'
import { sleep } from '@gpahal/std/promises'

import { DurableExecutionCancelledError, DurableExecutionError } from '../src/errors'
import { InMemoryTaskExecutionsStorage } from '../src/in-memory-storage'
import type { LoggerInternal } from '../src/logger'
import type { TaskExecutionsStorage, TaskExecutionStorageValue } from '../src/storage'
import { TaskExecutionsStorageInternal } from '../src/storage-internal'

function createMockLogger(): LoggerInternal {
  // @ts-expect-error - This is safe
  return {
    debug: vi.fn(),
    info: vi.fn(),
    error: vi.fn(),
  }
}

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
  let logger: LoggerInternal
  let storage: TaskExecutionsStorage
  let storageInternal: TaskExecutionsStorageInternal

  beforeEach(() => {
    logger = createMockLogger()
    storage = new InMemoryTaskExecutionsStorage()
  })

  afterEach(async () => {
    if (storageInternal) {
      await storageInternal.shutdown()
    }
  })

  describe('initialization', () => {
    it('should initialize with batching disabled', () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      expect(storageInternal).toBeDefined()
      expect(storageInternal.timingStats).toBeDefined()
      expect(storageInternal.perSecondStorageCallCounts).toBeDefined()
    })

    it('should initialize with batching enabled', () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, true, 100)
      expect(storageInternal).toBeDefined()
      expect(storageInternal.timingStats).toBeDefined()
      expect(storageInternal.perSecondStorageCallCounts).toBeDefined()
    })

    it('should handle custom max retry attempts', () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100, 5)
      expect(storageInternal).toBeDefined()
    })
  })

  describe('insertMany', () => {
    it('should handle empty executions array', async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      await expect(storageInternal.insertMany([])).resolves.toBeUndefined()
    })

    it('should insert single execution without batching', async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      const execution = createMockStorageValue()
      await storageInternal.insertMany([execution])

      const result = await storage.getManyById([
        { executionId: execution.executionId, filters: {} },
      ])
      expect(result[0]).toEqual(execution)
    })

    it('should insert multiple executions without batching', async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      const executions = [
        createMockStorageValue({ executionId: 'exec-1' }),
        createMockStorageValue({ executionId: 'exec-2' }),
        createMockStorageValue({ executionId: 'exec-3' }),
      ]
      await storageInternal.insertMany(executions)

      for (const execution of executions) {
        const result = await storage.getManyById([
          { executionId: execution.executionId, filters: {} },
        ])
        expect(result[0]).toEqual(execution)
      }
    })

    it('should batch small inserts when batching is enabled', async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, true, 10)
      storageInternal.startBackgroundProcesses()

      const execution1 = createMockStorageValue({ executionId: 'exec-1' })
      const execution2 = createMockStorageValue({ executionId: 'exec-2' })

      const promise1 = storageInternal.insertMany([execution1])
      const promise2 = storageInternal.insertMany([execution2])

      await Promise.all([promise1, promise2])

      const result1 = await storage.getManyById([{ executionId: 'exec-1', filters: {} }])
      const result2 = await storage.getManyById([{ executionId: 'exec-2', filters: {} }])
      expect(result1[0]).toEqual(execution1)
      expect(result2[0]).toEqual(execution2)
    })

    it('should not batch large inserts even when batching is enabled', async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, true, 100)
      storageInternal.startBackgroundProcesses()

      const executions = [
        createMockStorageValue({ executionId: 'exec-1' }),
        createMockStorageValue({ executionId: 'exec-2' }),
        createMockStorageValue({ executionId: 'exec-3' }),
      ]
      await storageInternal.insertMany(executions)

      for (const execution of executions) {
        const result = await storage.getManyById([
          { executionId: execution.executionId, filters: {} },
        ])
        expect(result[0]).toEqual(execution)
      }
    })
  })

  describe('getById', () => {
    beforeEach(async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      const execution = createMockStorageValue()
      await storage.insertMany([execution])
    })

    it('should get execution by id', async () => {
      const result = await storageInternal.getById({ executionId: 'exec-1' })
      expect(result).toBeDefined()
      expect(result?.executionId).toBe('exec-1')
    })

    it('should return undefined for non-existent execution', async () => {
      const result = await storageInternal.getById({ executionId: 'non-existent' })
      expect(result).toBeUndefined()
    })

    it('should apply filters correctly', async () => {
      const result = await storageInternal.getById({
        executionId: 'exec-1',
        filters: { status: 'ready' },
      })
      expect(result).toBeDefined()

      const result2 = await storageInternal.getById({
        executionId: 'exec-1',
        filters: { status: 'completed' },
      })
      expect(result2).toBeUndefined()
    })

    it('should batch getById calls when batching is enabled', async () => {
      await storageInternal.shutdown()
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, true, 10)
      storageInternal.startBackgroundProcesses()

      const promise1 = storageInternal.getById({ executionId: 'exec-1', filters: {} })
      const promise2 = storageInternal.getById({
        executionId: 'exec-1',
        filters: { status: 'ready' },
      })

      const [result1, result2] = await Promise.all([promise1, promise2])
      expect(result1).toBeDefined()
      expect(result2).toBeDefined()
    })
  })

  describe('getBySleepingTaskUniqueId', () => {
    beforeEach(async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      const execution = createMockStorageValue({
        isSleepingTask: true,
        sleepingTaskUniqueId: 'sleep-1',
      })
      await storage.insertMany([execution])
    })

    it('should get execution by sleeping task unique id', async () => {
      const result = await storageInternal.getBySleepingTaskUniqueId({
        sleepingTaskUniqueId: 'sleep-1',
      })
      expect(result).toBeDefined()
      expect(result?.sleepingTaskUniqueId).toBe('sleep-1')
    })

    it('should return undefined for non-existent sleeping task', async () => {
      const result = await storageInternal.getBySleepingTaskUniqueId({
        sleepingTaskUniqueId: 'non-existent',
      })
      expect(result).toBeUndefined()
    })
  })

  describe('updateById', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      const execution = createMockStorageValue()
      await storage.insertMany([execution])
    })

    it('should update execution by id', async () => {
      await storageInternal.updateById(now, {
        executionId: 'exec-1',
        update: { status: 'running' },
      })

      const result = await storage.getManyById([{ executionId: 'exec-1', filters: {} }])
      expect(result[0]?.status).toBe('running')
    })

    it('should apply filters when updating', async () => {
      await storageInternal.updateById(now, {
        executionId: 'exec-1',
        filters: { status: 'ready' },
        update: { status: 'running' },
      })

      const result = await storage.getManyById([{ executionId: 'exec-1', filters: {} }])
      expect(result[0]?.status).toBe('running')
    })

    it('should handle finalize of parent with completed status', async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, true, 100)
      storageInternal.startBackgroundProcesses()
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
      await storage.insertMany([execution])

      await storageInternal.updateById(
        now,
        {
          executionId: 'finalize-exec',
          update: { status: 'completed' },
        },
        execution,
      )

      const result = await storage.getManyById([{ executionId: 'finalize-exec', filters: {} }])
      expect(result[0]?.closeStatus).toBe('ready')
    })

    it('should batch updateById calls when batching is enabled', async () => {
      await storageInternal.shutdown()
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, true, 10)
      storageInternal.startBackgroundProcesses()

      const promise1 = storageInternal.updateById(now, {
        executionId: 'exec-1',
        update: { status: 'running' },
      })
      const promise2 = storageInternal.updateById(now + 1, {
        executionId: 'exec-1',
        update: { executorId: 'executor-1' },
      })

      await Promise.all([promise1, promise2])

      const result = await storage.getManyById([{ executionId: 'exec-1', filters: {} }])
      expect(result[0]?.status).toBe('running')
      expect(result[0]?.executorId).toBe('executor-1')
    })
  })

  describe('updateByIdAndInsertChildrenIfUpdated', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      const execution = createMockStorageValue()
      await storage.insertMany([execution])
    })

    it('should update execution without children', async () => {
      await storageInternal.updateByIdAndInsertChildrenIfUpdated(now, {
        executionId: 'exec-1',
        update: { status: 'waiting_for_children' },
        childrenTaskExecutionsToInsertIfAnyUpdated: [],
      })

      const result = await storage.getManyById([{ executionId: 'exec-1', filters: {} }])
      expect(result[0]?.status).toBe('waiting_for_children')
    })

    it('should update execution and insert children', async () => {
      const children = [
        createMockStorageValue({ executionId: 'child-1' }),
        createMockStorageValue({ executionId: 'child-2' }),
      ]

      await storageInternal.updateByIdAndInsertChildrenIfUpdated(now, {
        executionId: 'exec-1',
        update: { status: 'waiting_for_children' },
        childrenTaskExecutionsToInsertIfAnyUpdated: children,
      })

      const parent = await storage.getManyById([{ executionId: 'exec-1', filters: {} }])
      expect(parent[0]?.status).toBe('waiting_for_children')

      const child1 = await storage.getManyById([{ executionId: 'child-1', filters: {} }])
      expect(child1[0]).toBeDefined()

      const child2 = await storage.getManyById([{ executionId: 'child-2', filters: {} }])
      expect(child2[0]).toBeDefined()
    })

    it('should not batch when children count is >= 3', async () => {
      await storageInternal.shutdown()
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, true, 10)
      storageInternal.startBackgroundProcesses()

      const children = [
        createMockStorageValue({ executionId: 'child-1' }),
        createMockStorageValue({ executionId: 'child-2' }),
        createMockStorageValue({ executionId: 'child-3' }),
      ]

      await storageInternal.updateByIdAndInsertChildrenIfUpdated(now, {
        executionId: 'exec-1',
        update: { status: 'waiting_for_children' },
        childrenTaskExecutionsToInsertIfAnyUpdated: children,
      })

      for (const child of children) {
        const result = await storage.getManyById([{ executionId: child.executionId, filters: {} }])
        expect(result[0]).toBeDefined()
      }
    })
  })

  describe('updateByParentExecutionIdAndIsFinished', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
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
      await storage.insertMany([parent, child1, child2])
    })

    it('should update children by parent execution id and finished status', async () => {
      await storageInternal.updateByParentExecutionIdAndIsFinished(now, {
        parentExecutionId: 'parent-1',
        isFinished: false,
        update: { closeStatus: 'ready' },
      })

      const children = await storage.getManyByParentExecutionId([{ parentExecutionId: 'parent-1' }])
      expect(children[0]).toHaveLength(2)
      for (const child of children[0]!) {
        expect(child.closeStatus).toBe('ready')
      }
    })

    it('should batch updates when batching is enabled', async () => {
      await storageInternal.shutdown()
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, true, 10)
      storageInternal.startBackgroundProcesses()

      const promise1 = storageInternal.updateByParentExecutionIdAndIsFinished(now, {
        parentExecutionId: 'parent-1',
        isFinished: false,
        update: { closeStatus: 'ready' },
      })
      const promise2 = storageInternal.updateByParentExecutionIdAndIsFinished(now + 1, {
        parentExecutionId: 'parent-1',
        isFinished: false,
        update: { closeStatus: 'closing' },
      })

      await Promise.all([promise1, promise2])

      const children = await storage.getManyByParentExecutionId([{ parentExecutionId: 'parent-1' }])
      expect(children[0]).toHaveLength(2)
    })
  })

  describe('getByParentExecutionId', () => {
    beforeEach(async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
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
      await storage.insertMany([parent, child1, child2])
    })

    it('should get children by parent execution id', async () => {
      const children = await storageInternal.getByParentExecutionId('parent-1')
      expect(children).toHaveLength(2)
      expect(children[0]?.executionId).toBe('child-1')
      expect(children[1]?.executionId).toBe('child-2')
    })

    it('should return empty array for non-existent parent', async () => {
      const children = await storageInternal.getByParentExecutionId('non-existent')
      expect(children).toEqual([])
    })

    it('should batch requests when batching is enabled', async () => {
      await storageInternal.shutdown()
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, true, 10)
      storageInternal.startBackgroundProcesses()

      const promise1 = storageInternal.getByParentExecutionId('parent-1')
      const promise2 = storageInternal.getByParentExecutionId('parent-1')

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
        updateByIsSleepingTaskAndExpiresAtLessThan: vi.fn(() => 0),
        updateByOnChildrenFinishedProcessingExpiresAtLessThan: vi.fn(() => 0),
        updateByCloseExpiresAtLessThan: vi.fn(() => 0),
        updateByExecutorIdAndNeedsPromiseCancellationAndReturn: vi.fn(() => []),
        getManyByParentExecutionId: vi.fn(() => []),
        updateManyByParentExecutionIdAndIsFinished: vi.fn(),
        updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus: vi.fn(() => 0),
        deleteById: vi.fn(),
        deleteAll: vi.fn(),
      }

      storageInternal = new TaskExecutionsStorageInternal(logger, mockStorage, false, 100, 3)
      const execution = createMockStorageValue()
      await storageInternal.insertMany([execution])

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
        updateByIsSleepingTaskAndExpiresAtLessThan: vi.fn(() => 0),
        updateByOnChildrenFinishedProcessingExpiresAtLessThan: vi.fn(() => 0),
        updateByCloseExpiresAtLessThan: vi.fn(() => 0),
        updateByExecutorIdAndNeedsPromiseCancellationAndReturn: vi.fn(() => []),
        getManyByParentExecutionId: vi.fn(() => []),
        updateManyByParentExecutionIdAndIsFinished: vi.fn(),
        updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus: vi.fn(() => 0),
        deleteById: vi.fn(),
        deleteAll: vi.fn(),
      }

      storageInternal = new TaskExecutionsStorageInternal(logger, mockStorage, false, 100, 3)
      const execution = createMockStorageValue()

      await expect(storageInternal.insertMany([execution])).rejects.toThrow('Permanent error')
      expect(callCount).toBe(1)
      expect(mockStorage.insertMany).toHaveBeenCalledTimes(1)
    })

    it('should fail after max retry attempts', async () => {
      let callCount = 0
      const mockStorage: TaskExecutionsStorage = {
        insertMany: vi.fn(() => {
          callCount++
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
        updateByIsSleepingTaskAndExpiresAtLessThan: vi.fn(() => 0),
        updateByOnChildrenFinishedProcessingExpiresAtLessThan: vi.fn(() => 0),
        updateByCloseExpiresAtLessThan: vi.fn(() => 0),
        updateByExecutorIdAndNeedsPromiseCancellationAndReturn: vi.fn(() => []),
        getManyByParentExecutionId: vi.fn(() => []),
        updateManyByParentExecutionIdAndIsFinished: vi.fn(),
        updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus: vi.fn(() => 0),
        deleteById: vi.fn(),
        deleteAll: vi.fn(),
      }

      storageInternal = new TaskExecutionsStorageInternal(logger, mockStorage, false, 100, 2)
      const execution = createMockStorageValue()

      await expect(storageInternal.insertMany([execution])).rejects.toThrow('Temporary error')
      expect(callCount).toBe(3)
      expect(mockStorage.insertMany).toHaveBeenCalledTimes(3)
    })
  })

  describe('shutdown', () => {
    it('should handle shutdown without background processes', async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      await expect(storageInternal.shutdown()).resolves.toBeUndefined()
    })

    it('should handle shutdown with background processes', async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, true, 100)
      storageInternal.startBackgroundProcesses()
      await sleep(10)
      await expect(storageInternal.shutdown()).resolves.toBeUndefined()
    })

    it('should handle shutdown with pending requests', async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, true, 100)
      storageInternal.startBackgroundProcesses()

      const promises = []
      for (let i = 0; i < 10; i++) {
        promises.push(
          storageInternal.insertMany([
            createMockStorageValue({ executionId: `shutdown-test-${i}` }),
          ]),
        )
      }

      await expect(storageInternal.shutdown()).resolves.toBeUndefined()
    })

    it('should throw if operations are attempted after shutdown', async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      await storageInternal.shutdown()

      expect(() => storageInternal.startBackgroundProcesses()).toThrowError()
    })
  })

  describe('timing stats', () => {
    it('should track timing stats for operations', async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      const execution = createMockStorageValue()

      await storageInternal.insertMany([execution])
      await storageInternal.getById({ executionId: 'exec-1' })

      expect(storageInternal.timingStats.has('insertMany')).toBe(true)
      expect(storageInternal.timingStats.has('getManyById')).toBe(true)

      const insertStats = storageInternal.timingStats.get('insertMany')
      expect(insertStats?.count).toBeGreaterThan(0)
      expect(insertStats?.meanMs).toBeGreaterThanOrEqual(0)

      const getStats = storageInternal.timingStats.get('getManyById')
      expect(getStats?.count).toBeGreaterThan(0)
      expect(getStats?.meanMs).toBeGreaterThanOrEqual(0)
    })

    it('should update mean duration correctly', async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      const execution1 = createMockStorageValue({ executionId: 'exec-mean-1' })
      const execution2 = createMockStorageValue({ executionId: 'exec-mean-2' })

      await storageInternal.insertMany([execution1])
      await storageInternal.insertMany([execution2])

      const stats = storageInternal.timingStats.get('insertMany')
      expect(stats?.count).toBe(2)
    })
  })

  describe('storage call counts', () => {
    it('should track storage calls per second', async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      const execution = createMockStorageValue()

      await storageInternal.insertMany([execution])
      await storageInternal.getById({ executionId: 'exec-1' })

      const currentSecond = Math.floor(Date.now() / 1000)
      const count = storageInternal.perSecondStorageCallCounts.get(currentSecond)
      expect(count).toBeGreaterThan(0)
    })
  })

  describe('updateByStatusAndStartAtLessThanAndReturn', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      const executions = [
        createMockStorageValue({ executionId: 'exec-1', status: 'ready', startAt: now - 1000 }),
        createMockStorageValue({ executionId: 'exec-2', status: 'ready', startAt: now - 500 }),
        createMockStorageValue({ executionId: 'exec-3', status: 'ready', startAt: now + 1000 }),
      ]
      await storage.insertMany(executions)
    })

    it('should update executions by status and startAt', async () => {
      const updated = await storageInternal.updateByStatusAndStartAtLessThanAndReturn(now, {
        status: 'ready',
        startAtLessThan: now,
        update: { status: 'running', executorId: 'executor-1' },
        updateExpiresAtWithStartedAt: now,
        limit: 10,
      })

      expect(updated).toHaveLength(2)
      expect(updated[0]?.executionId).toBe('exec-1')
      expect(updated[1]?.executionId).toBe('exec-2')

      for (const execution of updated) {
        expect(execution.status).toBe('running')
        expect(execution.executorId).toBe('executor-1')
      }
    })

    it('should respect limit parameter', async () => {
      const updated = await storageInternal.updateByStatusAndStartAtLessThanAndReturn(now, {
        status: 'ready',
        startAtLessThan: now,
        update: { status: 'running' },
        updateExpiresAtWithStartedAt: now,
        limit: 1,
      })

      expect(updated).toHaveLength(1)
      expect(updated[0]?.executionId).toBe('exec-1')
    })

    it('should handle custom retry attempts', async () => {
      const updated = await storageInternal.updateByStatusAndStartAtLessThanAndReturn(
        now,
        {
          status: 'ready',
          startAtLessThan: now,
          update: { status: 'running' },
          updateExpiresAtWithStartedAt: now,
          limit: 10,
        },
        5,
      )

      expect(updated.length).toBeGreaterThanOrEqual(0)
    })
  })

  describe('updateByCloseStatusAndReturn', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      const executions = [
        createMockStorageValue({ executionId: 'exec-1', closeStatus: 'ready' }),
        createMockStorageValue({ executionId: 'exec-2', closeStatus: 'ready' }),
        createMockStorageValue({ executionId: 'exec-3', closeStatus: 'idle' }),
      ]
      await storage.insertMany(executions)
    })

    it('should update executions by close status', async () => {
      const updated = await storageInternal.updateByCloseStatusAndReturn(now, {
        closeStatus: 'ready',
        update: { closeStatus: 'closing' },
        limit: 10,
      })

      expect(updated).toHaveLength(2)
      for (const execution of updated) {
        expect(execution.closeStatus).toBe('closing')
      }
    })
  })

  describe('updateByIsSleepingTaskAndExpiresAtLessThan', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
      const executions = [
        createMockStorageValue({
          executionId: 'sleep-1',
          isSleepingTask: true,
          sleepingTaskUniqueId: 'sleep-1',
          expiresAt: now - 1000,
        }),
        createMockStorageValue({
          executionId: 'sleep-2',
          isSleepingTask: true,
          sleepingTaskUniqueId: 'sleep-2',
          expiresAt: now + 1000,
        }),
      ]
      await storage.insertMany(executions)
    })

    it('should update sleeping tasks by expiry time', async () => {
      const count = await storageInternal.updateByIsSleepingTaskAndExpiresAtLessThan(now, {
        isSleepingTask: true,
        expiresAtLessThan: now,
        update: { status: 'timed_out' },
        limit: 10,
      })

      expect(count).toBe(1)

      const result = await storage.getManyById([{ executionId: 'sleep-1', filters: {} }])
      expect(result[0]?.status).toBe('timed_out')
    })
  })

  describe('updateByExecutorIdAndNeedsPromiseCancellationAndReturn', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false, 100)
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
      await storage.insertMany(executions)
    })

    it('should update executions by executor id and needs promise cancellation', async () => {
      const updated = await storageInternal.updateByExecutorIdAndNeedsPromiseCancellationAndReturn(
        now,
        {
          executorId: 'executor-1',
          needsPromiseCancellation: true,
          update: { needsPromiseCancellation: false },
          limit: 10,
        },
      )

      expect(updated).toHaveLength(1)
      expect(updated[0]?.executionId).toBe('exec-1')
      expect(updated[0]?.needsPromiseCancellation).toBe(false)
    })
  })

  describe('background processes', () => {
    it('should handle background processes lifecycle', async () => {
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, true, 10)

      expect(() => storageInternal.startBackgroundProcesses()).not.toThrow()

      await sleep(50)

      await expect(storageInternal.shutdown()).resolves.toBeUndefined()
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
        updateByIsSleepingTaskAndExpiresAtLessThan: vi.fn(() => 0),
        updateByOnChildrenFinishedProcessingExpiresAtLessThan: vi.fn(() => 0),
        updateByCloseExpiresAtLessThan: vi.fn(() => 0),
        updateByExecutorIdAndNeedsPromiseCancellationAndReturn: vi.fn(() => []),
        getManyByParentExecutionId: vi.fn(() => []),
        updateManyByParentExecutionIdAndIsFinished: vi.fn(),
        updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus: vi.fn(() => 0),
        deleteById: vi.fn(),
        deleteAll: vi.fn(),
      }

      storageInternal = new TaskExecutionsStorageInternal(logger, mockStorage, true, 10)
      storageInternal.startBackgroundProcesses()

      const execution = createMockStorageValue()
      await expect(storageInternal.insertMany([execution])).rejects.toThrow('Test error')

      await storageInternal.shutdown()
    })

    it('should handle cancelled error during shutdown', async () => {
      const [cancelSignal] = createCancelSignal()
      const mockStorage: TaskExecutionsStorage = {
        insertMany: vi.fn(() => {
          if (cancelSignal.isCancelled()) {
            throw DurableExecutionCancelledError
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
        updateByIsSleepingTaskAndExpiresAtLessThan: vi.fn(() => 0),
        updateByOnChildrenFinishedProcessingExpiresAtLessThan: vi.fn(() => 0),
        updateByCloseExpiresAtLessThan: vi.fn(() => 0),
        updateByExecutorIdAndNeedsPromiseCancellationAndReturn: vi.fn(() => []),
        getManyByParentExecutionId: vi.fn(() => []),
        updateManyByParentExecutionIdAndIsFinished: vi.fn(),
        updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus: vi.fn(() => 0),
        deleteById: vi.fn(),
        deleteAll: vi.fn(),
      }

      storageInternal = new TaskExecutionsStorageInternal(logger, mockStorage, true, 10)
      storageInternal.startBackgroundProcesses()

      await sleep(50)
      await storageInternal.shutdown()

      expect(logger.error).not.toHaveBeenCalledWith(
        expect.stringContaining('Error in batch requester'),
        DurableExecutionCancelledError,
      )
    })
  })

  describe('updateByOnChildrenFinishedProcessingExpiresAtLessThan', () => {
    it('should handle update by on children finished processing expires at less than', async () => {
      const mockValue = createMockStorageValue({
        onChildrenFinishedProcessingExpiresAt: Date.now() + 1000,
      })
      await storage.insertMany([mockValue])

      const futureTime = Date.now() + 2000
      storageInternal = new TaskExecutionsStorageInternal(logger, storage, false)
      const count = await storageInternal.updateByOnChildrenFinishedProcessingExpiresAtLessThan(
        Date.now(),
        {
          onChildrenFinishedProcessingExpiresAtLessThan: futureTime,
          update: { status: 'failed' },
          limit: 10,
        },
      )

      expect(count).toBe(1)
    })

    it('should handle update by on children finished processing expires at less than with no matches', async () => {
      const mockValue = createMockStorageValue()
      await storage.insertMany([mockValue])

      const pastTime = Date.now() - 1000
      const count = await storageInternal.updateByOnChildrenFinishedProcessingExpiresAtLessThan(
        Date.now(),
        {
          onChildrenFinishedProcessingExpiresAtLessThan: pastTime,
          update: { status: 'failed' },
          limit: 10,
        },
      )

      expect(count).toBe(0)
    })
  })
})
