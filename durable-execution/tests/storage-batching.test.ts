import type { TaskExecutionStatus } from '../src'
import {
  createTaskExecutionStorageValue,
  TaskExecutionsStorageWithBatching,
  type TaskExecutionsStorageWithoutBatching,
  type TaskExecutionStorageGetByIdFilters,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
} from '../src/storage'

function createMockStorageValue(
  overrides?: Partial<TaskExecutionStorageValue>,
): TaskExecutionStorageValue {
  const now = Date.now()
  const base = createTaskExecutionStorageValue({
    now,
    taskId: 'task-1',
    executionId: 'exec-1',
    isSleepingTask: false,
    retryOptions: { maxAttempts: 3 },
    sleepMsBeforeRun: 0,
    timeoutMs: 30_000,
    input: '{}',
  })
  return { ...base, ...overrides }
}

function createMockStorageWithoutBatching(): TaskExecutionsStorageWithoutBatching {
  const storage = new Map<string, TaskExecutionStorageValue>()
  const sleepingTaskMap = new Map<string, TaskExecutionStorageValue>()

  return {
    insertMany: vi.fn((executions: ReadonlyArray<TaskExecutionStorageValue>) => {
      for (const execution of executions) {
        storage.set(execution.executionId, execution)
        if (execution.sleepingTaskUniqueId) {
          sleepingTaskMap.set(execution.sleepingTaskUniqueId, execution)
        }
      }
    }),

    getById: vi.fn(
      ({
        executionId,
        filters,
      }: {
        executionId: string
        filters?: TaskExecutionStorageGetByIdFilters
      }): TaskExecutionStorageValue | undefined => {
        const execution = storage.get(executionId)
        if (!execution) return undefined

        if (
          filters?.isSleepingTask != null &&
          execution.isSleepingTask !== filters.isSleepingTask
        ) {
          return undefined
        }
        if (filters?.status != null && execution.status !== filters.status) {
          return undefined
        }
        if (filters?.isFinished != null && execution.isFinished !== filters.isFinished) {
          return undefined
        }

        return execution
      },
    ),

    getBySleepingTaskUniqueId: vi.fn(
      ({
        sleepingTaskUniqueId,
      }: {
        sleepingTaskUniqueId: string
      }): TaskExecutionStorageValue | undefined => {
        return sleepingTaskMap.get(sleepingTaskUniqueId)
      },
    ),

    updateById: vi.fn(
      ({
        executionId,
        filters,
        update,
      }: {
        executionId: string
        filters?: TaskExecutionStorageGetByIdFilters
        update: TaskExecutionStorageUpdate
      }): void => {
        const execution = storage.get(executionId)
        if (!execution) return

        if (
          filters?.isSleepingTask != null &&
          execution.isSleepingTask !== filters.isSleepingTask
        ) {
          return
        }
        if (filters?.status != null && execution.status !== filters.status) {
          return
        }
        if (filters?.isFinished != null && execution.isFinished !== filters.isFinished) {
          return
        }

        Object.assign(execution, update)
      },
    ),

    updateByIdAndInsertChildrenIfUpdated: vi.fn(
      ({
        executionId,
        filters,
        update,
        childrenTaskExecutionsToInsertIfAnyUpdated,
      }: {
        executionId: string
        filters?: TaskExecutionStorageGetByIdFilters
        update: TaskExecutionStorageUpdate
        childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
      }): void => {
        const execution = storage.get(executionId)
        if (!execution) return

        if (
          filters?.isSleepingTask != null &&
          execution.isSleepingTask !== filters.isSleepingTask
        ) {
          return
        }
        if (filters?.status != null && execution.status !== filters.status) {
          return
        }
        if (filters?.isFinished != null && execution.isFinished !== filters.isFinished) {
          return
        }

        Object.assign(execution, update)
        for (const child of childrenTaskExecutionsToInsertIfAnyUpdated) {
          storage.set(child.executionId, child)
        }
      },
    ),

    getByParentExecutionId: vi.fn(
      ({ parentExecutionId }: { parentExecutionId: string }): Array<TaskExecutionStorageValue> => {
        const children: Array<TaskExecutionStorageValue> = []
        for (const execution of storage.values()) {
          if (execution.parent?.executionId === parentExecutionId) {
            children.push(execution)
          }
        }
        return children
      },
    ),

    updateByParentExecutionIdAndIsFinished: vi.fn(
      ({
        parentExecutionId,
        isFinished,
        update,
      }: {
        parentExecutionId: string
        isFinished: boolean
        update: TaskExecutionStorageUpdate
      }): void => {
        for (const execution of storage.values()) {
          if (
            execution.parent?.executionId === parentExecutionId &&
            execution.isFinished === isFinished
          ) {
            Object.assign(execution, update)
          }
        }
      },
    ),

    updateByStatusAndStartAtLessThanAndReturn: vi.fn(
      ({
        status,
        startAtLessThan,
        update,
        updateExpiresAtWithStartedAt,
        limit,
      }: {
        status: string
        startAtLessThan: number
        update: TaskExecutionStorageUpdate
        updateExpiresAtWithStartedAt: number
        limit: number
      }): Array<TaskExecutionStorageValue> => {
        const updated: Array<TaskExecutionStorageValue> = []
        const sortedExecutions = [...storage.values()].sort((a, b) => a.startAt - b.startAt)
        for (const execution of sortedExecutions) {
          if (
            execution.status === status &&
            execution.startAt < startAtLessThan &&
            updated.length < limit
          ) {
            Object.assign(execution, update)
            if (execution.timeoutMs) {
              execution.expiresAt = updateExpiresAtWithStartedAt + execution.timeoutMs
            }
            updated.push(execution)
          }
        }
        return updated
      },
    ),

    updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn: vi.fn(
      ({
        status,
        onChildrenFinishedProcessingStatus,
        update,
        limit,
      }: {
        status: string
        onChildrenFinishedProcessingStatus: string
        update: TaskExecutionStorageUpdate
        limit: number
      }): Array<TaskExecutionStorageValue> => {
        const updated: Array<TaskExecutionStorageValue> = []
        for (const execution of storage.values()) {
          if (
            execution.status === status &&
            execution.onChildrenFinishedProcessingStatus === onChildrenFinishedProcessingStatus &&
            execution.activeChildrenCount === 0 &&
            updated.length < limit
          ) {
            Object.assign(execution, update)
            updated.push(execution)
          }
        }
        return updated
      },
    ),

    updateByCloseStatusAndReturn: vi.fn(
      ({
        closeStatus,
        update,
        limit,
      }: {
        closeStatus: string
        update: TaskExecutionStorageUpdate
        limit: number
      }): Array<TaskExecutionStorageValue> => {
        const updated: Array<TaskExecutionStorageValue> = []
        const sortedExecutions = [...storage.values()].sort((a, b) => a.updatedAt - b.updatedAt)
        for (const execution of sortedExecutions) {
          if (execution.closeStatus === closeStatus && updated.length < limit) {
            Object.assign(execution, update)
            updated.push(execution)
          }
        }
        return updated
      },
    ),

    updateByIsSleepingTaskAndExpiresAtLessThan: vi.fn(
      ({
        isSleepingTask,
        expiresAtLessThan,
        update,
        limit,
      }: {
        isSleepingTask: boolean
        expiresAtLessThan: number
        update: TaskExecutionStorageUpdate
        limit: number
      }): number => {
        let count = 0
        const sortedExecutions = [...storage.values()].sort(
          (a, b) => (a.expiresAt ?? 0) - (b.expiresAt ?? 0),
        )
        for (const execution of sortedExecutions) {
          if (
            execution.isSleepingTask === isSleepingTask &&
            execution.expiresAt !== undefined &&
            execution.expiresAt < expiresAtLessThan &&
            count < limit
          ) {
            Object.assign(execution, update)
            count++
          }
        }
        return count
      },
    ),

    updateByOnChildrenFinishedProcessingExpiresAtLessThan: vi.fn(
      ({
        onChildrenFinishedProcessingExpiresAtLessThan,
        update,
        limit,
      }: {
        onChildrenFinishedProcessingExpiresAtLessThan: number
        update: TaskExecutionStorageUpdate
        limit: number
      }): number => {
        let count = 0
        for (const execution of storage.values()) {
          if (
            execution.onChildrenFinishedProcessingExpiresAt !== undefined &&
            execution.onChildrenFinishedProcessingExpiresAt <
              onChildrenFinishedProcessingExpiresAtLessThan &&
            count < limit
          ) {
            Object.assign(execution, update)
            count++
          }
        }
        return count
      },
    ),

    updateByCloseExpiresAtLessThan: vi.fn(
      ({
        closeExpiresAtLessThan,
        update,
        limit,
      }: {
        closeExpiresAtLessThan: number
        update: TaskExecutionStorageUpdate
        limit: number
      }): number => {
        let count = 0
        for (const execution of storage.values()) {
          if (
            execution.closeExpiresAt !== undefined &&
            execution.closeExpiresAt < closeExpiresAtLessThan &&
            count < limit
          ) {
            Object.assign(execution, update)
            count++
          }
        }
        return count
      },
    ),

    updateByExecutorIdAndNeedsPromiseCancellationAndReturn: vi.fn(
      ({
        executorId,
        needsPromiseCancellation,
        update,
        limit,
      }: {
        executorId: string
        needsPromiseCancellation: boolean
        update: TaskExecutionStorageUpdate
        limit: number
      }): Array<TaskExecutionStorageValue> => {
        const updated: Array<TaskExecutionStorageValue> = []
        for (const execution of storage.values()) {
          if (
            execution.executorId === executorId &&
            execution.needsPromiseCancellation === needsPromiseCancellation &&
            updated.length < limit
          ) {
            Object.assign(execution, update)
            updated.push(execution)
          }
        }
        return updated
      },
    ),

    updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus: vi.fn(
      ({
        isFinished,
        closeStatus,
        update,
        limit,
      }: {
        isFinished: boolean
        closeStatus: string
        update: TaskExecutionStorageUpdate
        limit: number
      }): number => {
        let count = 0
        const sortedExecutions = [...storage.values()].sort((a, b) => a.updatedAt - b.updatedAt)
        for (const execution of sortedExecutions) {
          if (
            execution.isFinished === isFinished &&
            execution.closeStatus === closeStatus &&
            count < limit
          ) {
            Object.assign(execution, update)
            if (execution.parent) {
              const parent = storage.get(execution.parent.executionId)
              if (parent && parent.activeChildrenCount > 0) {
                parent.activeChildrenCount--
              }
            }

            count++
          }
        }
        return count
      },
    ),

    deleteById: vi.fn(({ executionId }: { executionId: string }): void => {
      const execution = storage.get(executionId)
      if (execution?.sleepingTaskUniqueId) {
        sleepingTaskMap.delete(execution.sleepingTaskUniqueId)
      }
      storage.delete(executionId)
    }),

    deleteAll: vi.fn((): void => {
      storage.clear()
      sleepingTaskMap.clear()
    }),
  }
}

describe('TaskExecutionsStorageWithBatching', () => {
  let baseStorage: TaskExecutionsStorageWithoutBatching
  let batchingStorage: TaskExecutionsStorageWithBatching

  beforeEach(() => {
    baseStorage = createMockStorageWithoutBatching()
    batchingStorage = new TaskExecutionsStorageWithBatching(baseStorage)
  })

  describe('insertMany', () => {
    it('should insert single execution', async () => {
      const execution = createMockStorageValue()
      await batchingStorage.insertMany([execution])

      expect(baseStorage.insertMany).toHaveBeenCalledWith([execution])
      expect(baseStorage.insertMany).toHaveBeenCalledTimes(1)
    })

    it('should insert multiple executions', async () => {
      const executions = [
        createMockStorageValue({ executionId: 'exec-1' }),
        createMockStorageValue({ executionId: 'exec-2' }),
        createMockStorageValue({ executionId: 'exec-3' }),
      ]
      await batchingStorage.insertMany(executions)

      expect(baseStorage.insertMany).toHaveBeenCalledWith(executions)
      expect(baseStorage.insertMany).toHaveBeenCalledTimes(1)
    })
  })

  describe('getManyById', () => {
    beforeEach(async () => {
      const executions = [
        createMockStorageValue({ executionId: 'exec-1', status: 'ready' }),
        createMockStorageValue({ executionId: 'exec-2', status: 'running' }),
        createMockStorageValue({ executionId: 'exec-3', status: 'completed' }),
      ]
      await baseStorage.insertMany(executions)
    })

    it('should get single execution by id', async () => {
      const requests = [{ executionId: 'exec-1', filters: {} }]
      const results = await batchingStorage.getManyById(requests)

      expect(results).toHaveLength(1)
      expect(results[0]?.executionId).toBe('exec-1')
      expect(baseStorage.getById).toHaveBeenCalledTimes(1)
    })

    it('should get multiple executions by id in parallel', async () => {
      const requests = [
        { executionId: 'exec-1', filters: {} },
        { executionId: 'exec-2', filters: {} },
        { executionId: 'exec-3', filters: {} },
      ]
      const results = await batchingStorage.getManyById(requests)

      expect(results).toHaveLength(3)
      expect(results[0]?.executionId).toBe('exec-1')
      expect(results[1]?.executionId).toBe('exec-2')
      expect(results[2]?.executionId).toBe('exec-3')
      expect(baseStorage.getById).toHaveBeenCalledTimes(3)
    })

    it('should apply filters correctly', async () => {
      const requests = [
        { executionId: 'exec-1', filters: { status: 'ready' as TaskExecutionStatus } },
        { executionId: 'exec-2', filters: { status: 'completed' as TaskExecutionStatus } },
      ]
      const results = await batchingStorage.getManyById(requests)

      expect(results).toHaveLength(2)
      expect(results[0]?.executionId).toBe('exec-1')
      expect(results[1]).toBeUndefined()
    })

    it('should handle sequential requests when parallel requests are disabled', async () => {
      const sequentialStorage = new TaskExecutionsStorageWithBatching(baseStorage, true)
      const requests = [
        { executionId: 'exec-1', filters: {} },
        { executionId: 'exec-2', filters: {} },
      ]
      const results = await sequentialStorage.getManyById(requests)

      expect(results).toHaveLength(2)
      expect(results[0]?.executionId).toBe('exec-1')
      expect(results[1]?.executionId).toBe('exec-2')
      expect(baseStorage.getById).toHaveBeenCalledTimes(2)
    })
  })

  describe('getManyBySleepingTaskUniqueId', () => {
    beforeEach(async () => {
      const executions = [
        createMockStorageValue({
          executionId: 'sleep-1',
          isSleepingTask: true,
          sleepingTaskUniqueId: 'unique-1',
        }),
        createMockStorageValue({
          executionId: 'sleep-2',
          isSleepingTask: true,
          sleepingTaskUniqueId: 'unique-2',
        }),
      ]
      await baseStorage.insertMany(executions)
    })

    it('should get executions by sleeping task unique id', async () => {
      const requests = [
        { sleepingTaskUniqueId: 'unique-1' },
        { sleepingTaskUniqueId: 'unique-2' },
        { sleepingTaskUniqueId: 'non-existent' },
      ]
      const results = await batchingStorage.getManyBySleepingTaskUniqueId(requests)

      expect(results).toHaveLength(3)
      expect(results[0]?.executionId).toBe('sleep-1')
      expect(results[1]?.executionId).toBe('sleep-2')
      expect(results[2]).toBeUndefined()
      expect(baseStorage.getBySleepingTaskUniqueId).toHaveBeenCalledTimes(3)
    })

    it('should handle sequential requests when parallel requests are disabled', async () => {
      const sequentialStorage = new TaskExecutionsStorageWithBatching(baseStorage, true)
      const requests = [{ sleepingTaskUniqueId: 'unique-1' }, { sleepingTaskUniqueId: 'unique-2' }]
      const results = await sequentialStorage.getManyBySleepingTaskUniqueId(requests)

      expect(results).toHaveLength(2)
      expect(baseStorage.getBySleepingTaskUniqueId).toHaveBeenCalledTimes(2)
    })
  })

  describe('updateManyById', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      const executions = [
        createMockStorageValue({ executionId: 'exec-1', status: 'ready' }),
        createMockStorageValue({ executionId: 'exec-2', status: 'ready' }),
      ]
      await baseStorage.insertMany(executions)
    })

    it('should update single execution', async () => {
      const requests = [
        {
          executionId: 'exec-1',
          filters: {},
          update: { status: 'running', updatedAt: now } as TaskExecutionStorageUpdate,
        },
      ]
      await batchingStorage.updateManyById(requests)

      expect(baseStorage.updateById).toHaveBeenCalledWith({
        executionId: 'exec-1',
        filters: {},
        update: { status: 'running', updatedAt: now },
      })
      expect(baseStorage.updateById).toHaveBeenCalledTimes(1)
    })

    it('should update multiple executions in parallel', async () => {
      const requests = [
        {
          executionId: 'exec-1',
          filters: {},
          update: { status: 'running', updatedAt: now } as TaskExecutionStorageUpdate,
        },
        {
          executionId: 'exec-2',
          filters: {},
          update: { status: 'running', updatedAt: now } as TaskExecutionStorageUpdate,
        },
      ]
      await batchingStorage.updateManyById(requests)

      expect(baseStorage.updateById).toHaveBeenCalledTimes(2)
    })

    it('should handle filters correctly', async () => {
      const requests = [
        {
          executionId: 'exec-1',
          filters: { status: 'ready' as TaskExecutionStatus },
          update: {
            status: 'running' as TaskExecutionStatus,
            updatedAt: now,
          } as TaskExecutionStorageUpdate,
        },
        {
          executionId: 'exec-2',
          filters: { status: 'completed' as TaskExecutionStatus },
          update: {
            status: 'failed' as TaskExecutionStatus,
            updatedAt: now,
          } as TaskExecutionStorageUpdate,
        },
      ]
      await batchingStorage.updateManyById(requests)

      expect(baseStorage.updateById).toHaveBeenCalledTimes(2)

      const result1 = await baseStorage.getById({ executionId: 'exec-1' })
      expect(result1?.status).toBe('running')

      const result2 = await baseStorage.getById({ executionId: 'exec-2' })
      expect(result2?.status).toBe('ready')
    })

    it('should handle sequential updates when parallel requests are disabled', async () => {
      const sequentialStorage = new TaskExecutionsStorageWithBatching(baseStorage, true)
      const requests = [
        {
          executionId: 'exec-1',
          filters: {},
          update: { status: 'running', updatedAt: now } as TaskExecutionStorageUpdate,
        },
        {
          executionId: 'exec-2',
          filters: {},
          update: { status: 'running', updatedAt: now } as TaskExecutionStorageUpdate,
        },
      ]
      await sequentialStorage.updateManyById(requests)

      expect(baseStorage.updateById).toHaveBeenCalledTimes(2)
    })
  })

  describe('updateManyByIdAndInsertChildrenIfUpdated', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      const parent = createMockStorageValue({ executionId: 'parent-1', status: 'running' })
      await baseStorage.insertMany([parent])
    })

    it('should update execution and insert children', async () => {
      const children = [
        createMockStorageValue({ executionId: 'child-1' }),
        createMockStorageValue({ executionId: 'child-2' }),
      ]
      const requests = [
        {
          executionId: 'parent-1',
          filters: {},
          update: { status: 'waiting_for_children', updatedAt: now } as TaskExecutionStorageUpdate,
          childrenTaskExecutionsToInsertIfAnyUpdated: children,
        },
      ]
      await batchingStorage.updateManyByIdAndInsertChildrenIfUpdated(requests)

      expect(baseStorage.updateByIdAndInsertChildrenIfUpdated).toHaveBeenCalledWith({
        executionId: 'parent-1',
        filters: {},
        update: { status: 'waiting_for_children', updatedAt: now } as TaskExecutionStorageUpdate,
        childrenTaskExecutionsToInsertIfAnyUpdated: children,
      })
      expect(baseStorage.updateByIdAndInsertChildrenIfUpdated).toHaveBeenCalledTimes(1)

      const parent = await baseStorage.getById({ executionId: 'parent-1' })
      expect(parent?.status).toBe('waiting_for_children')

      const child1 = await baseStorage.getById({ executionId: 'child-1' })
      expect(child1).toBeDefined()

      const child2 = await baseStorage.getById({ executionId: 'child-2' })
      expect(child2).toBeDefined()
    })

    it('should handle multiple requests in parallel', async () => {
      const parent2 = createMockStorageValue({ executionId: 'parent-2', status: 'running' })
      await baseStorage.insertMany([parent2])

      const requests = [
        {
          executionId: 'parent-1',
          filters: {},
          update: { status: 'waiting_for_children', updatedAt: now } as TaskExecutionStorageUpdate,
          childrenTaskExecutionsToInsertIfAnyUpdated: [
            createMockStorageValue({ executionId: 'child-1' }),
          ],
        },
        {
          executionId: 'parent-2',
          filters: {},
          update: { status: 'waiting_for_children', updatedAt: now } as TaskExecutionStorageUpdate,
          childrenTaskExecutionsToInsertIfAnyUpdated: [
            createMockStorageValue({ executionId: 'child-2' }),
          ],
        },
      ]
      await batchingStorage.updateManyByIdAndInsertChildrenIfUpdated(requests)

      expect(baseStorage.updateByIdAndInsertChildrenIfUpdated).toHaveBeenCalledTimes(2)
    })

    it('should handle sequential updates when parallel requests are disabled', async () => {
      const sequentialStorage = new TaskExecutionsStorageWithBatching(baseStorage, true)
      const requests = [
        {
          executionId: 'parent-1',
          filters: {},
          update: { status: 'waiting_for_children', updatedAt: now } as TaskExecutionStorageUpdate,
          childrenTaskExecutionsToInsertIfAnyUpdated: [
            createMockStorageValue({ executionId: 'child-1' }),
          ],
        },
      ]
      await sequentialStorage.updateManyByIdAndInsertChildrenIfUpdated(requests)

      expect(baseStorage.updateByIdAndInsertChildrenIfUpdated).toHaveBeenCalledTimes(1)
    })
  })

  describe('getManyByParentExecutionId', () => {
    beforeEach(async () => {
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
      await baseStorage.insertMany([parent, child1, child2])
    })

    it('should get children by parent execution id', async () => {
      const requests = [{ parentExecutionId: 'parent-1' }]
      const results = await batchingStorage.getManyByParentExecutionId(requests)

      expect(results).toHaveLength(1)
      expect(results[0]).toHaveLength(2)
      expect(results[0]![0]?.executionId).toBe('child-1')
      expect(results[0]![1]?.executionId).toBe('child-2')
      expect(baseStorage.getByParentExecutionId).toHaveBeenCalledTimes(1)
    })

    it('should handle multiple parent ids in parallel', async () => {
      const parent2 = createMockStorageValue({ executionId: 'parent-2' })
      await baseStorage.insertMany([parent2])

      const requests = [{ parentExecutionId: 'parent-1' }, { parentExecutionId: 'parent-2' }]
      const results = await batchingStorage.getManyByParentExecutionId(requests)

      expect(results).toHaveLength(2)
      expect(results[0]).toHaveLength(2)
      expect(results[1]).toHaveLength(0)
      expect(baseStorage.getByParentExecutionId).toHaveBeenCalledTimes(2)
    })

    it('should handle sequential requests when parallel requests are disabled', async () => {
      const sequentialStorage = new TaskExecutionsStorageWithBatching(baseStorage, true)
      const requests = [{ parentExecutionId: 'parent-1' }]
      const results = await sequentialStorage.getManyByParentExecutionId(requests)

      expect(results).toHaveLength(1)
      expect(baseStorage.getByParentExecutionId).toHaveBeenCalledTimes(1)
    })
  })

  describe('updateManyByParentExecutionIdAndIsFinished', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
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
        isFinished: false,
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
        isFinished: true,
      })
      await baseStorage.insertMany([parent, child1, child2])
    })

    it('should update children by parent execution id and finished status', async () => {
      const requests = [
        {
          parentExecutionId: 'parent-1',
          isFinished: false,
          update: { closeStatus: 'ready', updatedAt: now } as TaskExecutionStorageUpdate,
        },
      ]
      await batchingStorage.updateManyByParentExecutionIdAndIsFinished(requests)

      expect(baseStorage.updateByParentExecutionIdAndIsFinished).toHaveBeenCalledWith({
        parentExecutionId: 'parent-1',
        isFinished: false,
        update: { closeStatus: 'ready', updatedAt: now },
      })
      expect(baseStorage.updateByParentExecutionIdAndIsFinished).toHaveBeenCalledTimes(1)

      const child1 = await baseStorage.getById({ executionId: 'child-1' })
      expect(child1!.closeStatus).toBe('ready')

      const child2 = await baseStorage.getById({ executionId: 'child-2' })
      expect(child2?.isFinished).toBe(true)
      expect(child2?.closeStatus).toBe('idle')
    })

    it('should handle multiple requests in parallel', async () => {
      const requests = [
        {
          parentExecutionId: 'parent-1',
          isFinished: false,
          update: { closeStatus: 'ready', updatedAt: now } as TaskExecutionStorageUpdate,
        },
        {
          parentExecutionId: 'parent-1',
          isFinished: true,
          update: { closeStatus: 'closed', updatedAt: now } as TaskExecutionStorageUpdate,
        },
      ]
      await batchingStorage.updateManyByParentExecutionIdAndIsFinished(requests)

      expect(baseStorage.updateByParentExecutionIdAndIsFinished).toHaveBeenCalledTimes(2)
    })

    it('should handle sequential updates when parallel requests are disabled', async () => {
      const sequentialStorage = new TaskExecutionsStorageWithBatching(baseStorage, true)
      const requests = [
        {
          parentExecutionId: 'parent-1',
          isFinished: false,
          update: { closeStatus: 'ready', updatedAt: now } as TaskExecutionStorageUpdate,
        },
      ]
      await sequentialStorage.updateManyByParentExecutionIdAndIsFinished(requests)

      expect(baseStorage.updateByParentExecutionIdAndIsFinished).toHaveBeenCalledTimes(1)
    })
  })

  describe('non-batched methods', () => {
    let now: number

    beforeEach(async () => {
      now = Date.now()
      const executions = [
        createMockStorageValue({ executionId: 'exec-1', status: 'ready', startAt: now - 1000 }),
        createMockStorageValue({ executionId: 'exec-2', status: 'ready', startAt: now + 1000 }),
        createMockStorageValue({ executionId: 'exec-3', closeStatus: 'ready' }),
        createMockStorageValue({
          executionId: 'sleep-1',
          isSleepingTask: true,
          expiresAt: now - 1000,
        }),
      ]
      await baseStorage.insertMany(executions)
    })

    it('should call updateByStatusAndStartAtLessThanAndReturn', async () => {
      const result = await batchingStorage.updateByStatusAndStartAtLessThanAndReturn({
        status: 'ready',
        startAtLessThan: now,
        update: { status: 'running', updatedAt: now },
        updateExpiresAtWithStartedAt: now,
        limit: 10,
      })

      expect(baseStorage.updateByStatusAndStartAtLessThanAndReturn).toHaveBeenCalledWith({
        status: 'ready',
        startAtLessThan: now,
        update: { status: 'running', updatedAt: now },
        updateExpiresAtWithStartedAt: now,
        limit: 10,
      })
      expect(result).toHaveLength(1)
      expect(result[0]?.executionId).toBe('exec-1')
    })

    it('should call updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn', async () => {
      const result =
        await batchingStorage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
          {
            status: 'waiting_for_children',
            onChildrenFinishedProcessingStatus: 'idle',
            update: { status: 'completed', updatedAt: now },
            limit: 10,
          },
        )

      expect(
        baseStorage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn,
      ).toHaveBeenCalledWith({
        status: 'waiting_for_children',
        onChildrenFinishedProcessingStatus: 'idle',
        update: { status: 'completed', updatedAt: now },
        limit: 10,
      })
      expect(result).toEqual([])
    })

    it('should call updateByCloseStatusAndReturn', async () => {
      const result = await batchingStorage.updateByCloseStatusAndReturn({
        closeStatus: 'ready',
        update: { closeStatus: 'closing', updatedAt: now },
        limit: 10,
      })

      expect(baseStorage.updateByCloseStatusAndReturn).toHaveBeenCalledWith({
        closeStatus: 'ready',
        update: { closeStatus: 'closing', updatedAt: now },
        limit: 10,
      })
      expect(result).toHaveLength(1)
      expect(result[0]?.executionId).toBe('exec-3')
    })

    it('should call updateByIsSleepingTaskAndExpiresAtLessThan', async () => {
      const count = await batchingStorage.updateByIsSleepingTaskAndExpiresAtLessThan({
        isSleepingTask: true,
        expiresAtLessThan: now,
        update: { status: 'timed_out', updatedAt: now },
        limit: 10,
      })

      expect(baseStorage.updateByIsSleepingTaskAndExpiresAtLessThan).toHaveBeenCalledWith({
        isSleepingTask: true,
        expiresAtLessThan: now,
        update: { status: 'timed_out', updatedAt: now },
        limit: 10,
      })
      expect(count).toBe(1)
    })

    it('should call updateByOnChildrenFinishedProcessingExpiresAtLessThan', async () => {
      const count = await batchingStorage.updateByOnChildrenFinishedProcessingExpiresAtLessThan({
        onChildrenFinishedProcessingExpiresAtLessThan: now,
        update: { onChildrenFinishedProcessingStatus: 'processed', updatedAt: now },
        limit: 10,
      })

      expect(
        baseStorage.updateByOnChildrenFinishedProcessingExpiresAtLessThan,
      ).toHaveBeenCalledWith({
        onChildrenFinishedProcessingExpiresAtLessThan: now,
        update: { onChildrenFinishedProcessingStatus: 'processed', updatedAt: now },
        limit: 10,
      })
      expect(count).toBe(0)
    })

    it('should call updateByCloseExpiresAtLessThan', async () => {
      const count = await batchingStorage.updateByCloseExpiresAtLessThan({
        closeExpiresAtLessThan: now,
        update: { closeStatus: 'closed', updatedAt: now },
        limit: 10,
      })

      expect(baseStorage.updateByCloseExpiresAtLessThan).toHaveBeenCalledWith({
        closeExpiresAtLessThan: now,
        update: { closeStatus: 'closed', updatedAt: now },
        limit: 10,
      })
      expect(count).toBe(0)
    })

    it('should call updateByExecutorIdAndNeedsPromiseCancellationAndReturn', async () => {
      const result = await batchingStorage.updateByExecutorIdAndNeedsPromiseCancellationAndReturn({
        executorId: 'executor-1',
        needsPromiseCancellation: true,
        update: { needsPromiseCancellation: false, updatedAt: now },
        limit: 10,
      })

      expect(
        baseStorage.updateByExecutorIdAndNeedsPromiseCancellationAndReturn,
      ).toHaveBeenCalledWith({
        executorId: 'executor-1',
        needsPromiseCancellation: true,
        update: { needsPromiseCancellation: false, updatedAt: now },
        limit: 10,
      })
      expect(result).toEqual([])
    })

    it('should call updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus', async () => {
      const parent = createMockStorageValue({
        executionId: 'parent-1',
        activeChildrenCount: 2,
      })
      const child = createMockStorageValue({
        executionId: 'child-1',
        parent: {
          taskId: 'parent-task',
          executionId: 'parent-1',
          indexInParentChildren: 0,
          isOnlyChildOfParent: false,
          isFinalizeOfParent: false,
        },
        isFinished: true,
        closeStatus: 'ready',
      })
      await baseStorage.insertMany([parent, child])

      const count =
        await batchingStorage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
          {
            isFinished: true,
            closeStatus: 'ready',
            update: { closeStatus: 'closing', updatedAt: now },
            limit: 10,
          },
        )

      expect(
        baseStorage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus,
      ).toHaveBeenCalledWith({
        isFinished: true,
        closeStatus: 'ready',
        update: { closeStatus: 'closing', updatedAt: now },
        limit: 10,
      })
      expect(count).toBe(1)

      const updatedParent = await baseStorage.getById({ executionId: 'parent-1' })
      expect(updatedParent?.activeChildrenCount).toBe(1)
    })

    it('should call deleteById', async () => {
      await batchingStorage.deleteById({ executionId: 'exec-1' })

      expect(baseStorage.deleteById).toHaveBeenCalledWith({ executionId: 'exec-1' })
      const result = await baseStorage.getById({ executionId: 'exec-1' })
      expect(result).toBeUndefined()
    })

    it('should call deleteAll', async () => {
      await batchingStorage.deleteAll()

      expect(baseStorage.deleteAll).toHaveBeenCalled()
      const result = await baseStorage.getById({ executionId: 'exec-1' })
      expect(result).toBeUndefined()
    })
  })

  describe('should handle errors', () => {
    it('should propagate errors from base storage', async () => {
      const errorStorage: TaskExecutionsStorageWithoutBatching = {
        ...createMockStorageWithoutBatching(),
        insertMany: vi.fn(() => {
          throw new Error('Storage error')
        }),
      }

      const errorBatchingStorage = new TaskExecutionsStorageWithBatching(errorStorage)
      const execution = createMockStorageValue()

      await expect(errorBatchingStorage.insertMany([execution])).rejects.toThrow('Storage error')
    })

    it('should handle errors in parallel operations', async () => {
      const errorStorage: TaskExecutionsStorageWithoutBatching = {
        ...createMockStorageWithoutBatching(),
        getById: vi.fn(({ executionId }: { executionId: string }) => {
          if (executionId === 'exec-2') {
            throw new Error('Get error')
          }
          return createMockStorageValue({ executionId })
        }),
      }

      const errorBatchingStorage = new TaskExecutionsStorageWithBatching(errorStorage)
      const requests = [
        { executionId: 'exec-1', filters: {} },
        { executionId: 'exec-2', filters: {} },
      ]

      await expect(errorBatchingStorage.getManyById(requests)).rejects.toThrow('Get error')
    })
  })
})
