import { InMemoryStorage } from 'durable-execution'
import { describe, it } from 'vitest'

import { cleanupTemporaryFiles, runStorageTest } from '../src'

describe('index', () => {
  afterAll(cleanupTemporaryFiles)

  it('should complete with in memory storage', { timeout: 120_000 }, async () => {
    const storage = new InMemoryStorage({ enableDebug: false })
    await runStorageTest(storage)
  })
})
