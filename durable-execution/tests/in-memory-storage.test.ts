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

  it('should handle logAllTaskExecutions', async () => {
    let infoStr = ''
    const logger = {
      debug: () => {
        // Do nothing
      },
      info: (message: string) => {
        infoStr += message
      },
      error: () => {
        // Do nothing
      },
    }

    const newStorage = new InMemoryTaskExecutionsStorage({ logger })

    await executor.shutdown()
    executor = new DurableExecutor(newStorage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    executor.startBackgroundProcesses()

    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'result',
    })

    const handle = await executor.enqueueTask(testTask)
    const finishedExecution = await handle.waitAndGetFinishedExecution({
      pollingIntervalMs: 100,
    })

    await newStorage.logAllTaskExecutions()
    expect(infoStr).toContain(finishedExecution.taskId)
    expect(infoStr).toContain(finishedExecution.executionId)
  })
})
