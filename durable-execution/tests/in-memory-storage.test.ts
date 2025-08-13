import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { DurableExecutor } from '../src'
import { InMemoryStorage } from './in-memory-storage'

describe('inMemoryStorage', () => {
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
    await handle.waitAndGetFinishedExecution()

    let savedData = ''
    await storage.save((s) => {
      savedData = s
      return Promise.resolve()
    })

    expect(savedData).toBeDefined()
    expect(typeof savedData).toBe('string')

    const newStorage = new InMemoryStorage({ enableDebug: false })
    await newStorage.load(() => Promise.resolve(savedData))

    expect(newStorage).toBeDefined()
  })

  it('should handle file save and load operations', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    const handle = await executor.enqueueTask(testTask)
    await handle.waitAndGetFinishedExecution()

    const tempFile = '/tmp/test-storage.json'

    await storage.saveToFile(tempFile)

    const newStorage = new InMemoryStorage({ enableDebug: false })
    await newStorage.loadFromFile(tempFile)

    expect(newStorage).toBeDefined()
  })

  it('should handle task execution status transitions', async () => {
    const testTask = executor.task({
      id: 'test',
      timeoutMs: 10_000,
      run: () => 'test',
    })

    const handle = await executor.enqueueTask(testTask)

    let execution = await handle.getExecution()
    expect(execution.status).toBe('ready')

    await handle.waitAndGetFinishedExecution()

    execution = await handle.getExecution()
    expect(execution.status).toBe('completed')
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

    const results = await Promise.all(handles.map((handle) => handle.waitAndGetFinishedExecution()))

    expect(results).toHaveLength(3)
    for (const result of results) {
      expect(result.status).toBe('completed')
      assert(result.status === 'completed')
      expect(result.output).toBe('result')
    }
  })
})
