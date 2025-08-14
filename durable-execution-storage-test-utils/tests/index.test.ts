import { InMemoryTaskExecutionsStorage } from 'durable-execution'

import { cleanupTemporaryFiles, runStorageTest } from '../src'

describe('index', () => {
  afterAll(cleanupTemporaryFiles)

  it('should complete with in memory storage', { timeout: 120_000 }, async () => {
    const storage = new InMemoryTaskExecutionsStorage()
    await runStorageTest(storage)
  })
})
