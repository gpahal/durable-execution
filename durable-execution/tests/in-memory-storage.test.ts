import { DurableExecutor, InMemoryTaskExecutionsStorage } from '../src'
import {
  loadInMemoryTaskExecutionsStorageFromFile,
  saveInMemoryTaskExecutionsStorageToFile,
} from './in-memory-storage-utils'

describe('InMemoryTaskExecutionsStorage', () => {
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
    if (executor) {
      await executor.shutdown()
    }
  })

  it('should handle save and load operations', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    const handle = await executor.enqueueTask(testTask)
    await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })

    let savedData = ''
    await storage.save((s) => {
      savedData = s
      return Promise.resolve()
    })

    expect(savedData).toBeDefined()
    expect(typeof savedData).toBe('string')

    const newStorage = new InMemoryTaskExecutionsStorage()
    await newStorage.load(() => Promise.resolve(savedData))

    expect(newStorage).toBeDefined()
  })

  it('should handle empty load operations', async () => {
    const newStorage = new InMemoryTaskExecutionsStorage()
    await newStorage.load(() => Promise.resolve(''))

    expect(newStorage).toBeDefined()
  })

  it('should handle file save and load operations', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    const handle = await executor.enqueueTask(testTask)
    await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })
    const dbValues = await storage.getManyById([
      { executionId: handle.getExecutionId(), filters: {} },
    ])
    expect(dbValues).toBeDefined()
    assert(dbValues)
    expect(dbValues[0]?.status).toBe('completed')

    const tempFile = '/tmp/test-storage.json'
    await saveInMemoryTaskExecutionsStorageToFile(storage, tempFile)

    const newStorage = await loadInMemoryTaskExecutionsStorageFromFile(tempFile)
    expect(newStorage).toBeDefined()
    const newDbValues = await newStorage.getManyById([
      { executionId: handle.getExecutionId(), filters: {} },
    ])
    expect(newDbValues).toBeDefined()
    assert(newDbValues)
    expect(newDbValues[0]?.status).toBe('completed')
  })

  it('should handle task execution status transitions', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    const handle = await executor.enqueueTask(testTask)

    let execution = await handle.getExecution()
    expect(['ready', 'running', 'completed']).toContain(execution.status)

    await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })

    execution = await handle.getExecution()
    expect(['ready', 'running', 'completed']).toContain(execution.status)
  })

  it('should handle multiple task executions', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'result',
    })

    const handles = []
    for (let i = 1; i <= 3; i++) {
      handles.push(await executor.enqueueTask(testTask))
    }

    const results = await Promise.all(
      handles.map((handle) =>
        handle.waitAndGetFinishedExecution({
          pollingIntervalMs: 100,
        }),
      ),
    )

    expect(results).toHaveLength(3)
    for (const result of results) {
      expect(result.status).toBe('completed')
      assert(result.status === 'completed')
      expect(result.output).toBe('result')
    }
  })

  it('should handle deleteById for sleeping task', async () => {
    const sleepingTask = executor.sleepingTask<string>({
      id: 'sleepingTask',
      timeoutMs: 30_000,
    })

    const handle = await executor.enqueueTask(sleepingTask, 'unique-sleep-1')
    const execution = await handle.getExecution()

    const beforeDelete = await storage.getManyById([
      { executionId: execution.executionId, filters: {} },
    ])
    expect(beforeDelete).toHaveLength(1)
    expect(beforeDelete[0]).toBeDefined()

    const sleepingTaskExecution = await storage.getManyBySleepingTaskUniqueId([
      { sleepingTaskUniqueId: 'unique-sleep-1' },
    ])
    expect(sleepingTaskExecution).toHaveLength(1)
    expect(sleepingTaskExecution[0]).toBeDefined()

    await storage.deleteById({ executionId: execution.executionId })

    const afterDelete = await storage.getManyById([
      { executionId: execution.executionId, filters: {} },
    ])
    expect(afterDelete).toHaveLength(1)
    expect(afterDelete[0]).toBeUndefined()

    const afterDeleteSleeping = await storage.getManyBySleepingTaskUniqueId([
      { sleepingTaskUniqueId: 'unique-sleep-1' },
    ])
    expect(afterDeleteSleeping).toHaveLength(1)
    expect(afterDeleteSleeping[0]).toBeUndefined()
  })

  it('should handle deleteAll', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'result',
    })

    const sleepingTask = executor.sleepingTask<string>({
      id: 'sleepingTask',
      timeoutMs: 30_000,
    })

    const handle1 = await executor.enqueueTask(testTask)
    const handle2 = await executor.enqueueTask(sleepingTask, 'unique-sleep-2')

    const execution1 = await handle1.getExecution()
    const execution2 = await handle2.getExecution()

    const beforeDelete = await storage.getManyById([
      { executionId: execution1.executionId, filters: {} },
      { executionId: execution2.executionId, filters: {} },
    ])
    expect(beforeDelete.filter((e) => e !== undefined)).toHaveLength(2)

    await storage.deleteAll()

    const afterDelete = await storage.getManyById([
      { executionId: execution1.executionId, filters: {} },
      { executionId: execution2.executionId, filters: {} },
    ])
    expect(afterDelete.filter((e) => e !== undefined)).toHaveLength(0)

    const afterDeleteSleeping = await storage.getManyBySleepingTaskUniqueId([
      { sleepingTaskUniqueId: 'unique-sleep-2' },
    ])
    expect(afterDeleteSleeping.filter((e) => e !== undefined)).toHaveLength(0)
  })

  it('should handle getByFilterFnAndLimitInternal with limit <= 0', () => {
    const internalStorage = storage as unknown as {
      getByFilterFnAndLimitInternal: (fn: () => boolean, limit: number) => Array<unknown>
    }
    expect(internalStorage.getByFilterFnAndLimitInternal(() => true, 0)).toEqual([])
    expect(internalStorage.getByFilterFnAndLimitInternal(() => true, -1)).toEqual([])
  })

  it('should handle updateByOnChildrenFinishedProcessingExpiresAtLessThan', async () => {
    const parentTaskOptions = {
      id: 'parentTask',
      runParent: () => ({ output: 'parent output', children: [] }),
      timeoutMs: 10_000,
    }

    const parentTask = executor.parentTask(parentTaskOptions)
    const handle = await executor.enqueueTask(parentTask)
    await handle.waitAndGetFinishedExecution({ pollingIntervalMs: 100 })

    const futureTime = Date.now() + 60_000
    const count = await storage.updateByOnChildrenFinishedProcessingExpiresAtLessThan({
      onChildrenFinishedProcessingExpiresAtLessThan: futureTime,
      update: { status: 'cancelled', updatedAt: Date.now() },
      limit: 10,
    })

    expect(count).toBe(0)
  })

  it('should return empty array with limit <= 0', async () => {
    const resultZero = await storage.updateByCloseStatusAndReturn({
      closeStatus: 'ready',
      update: { updatedAt: Date.now() },
      limit: 0,
    })
    expect(resultZero).toEqual([])

    const resultNegative = await storage.updateByCloseStatusAndReturn({
      closeStatus: 'ready',
      update: { updatedAt: Date.now() },
      limit: -1,
    })
    expect(resultNegative).toEqual([])
  })
})
