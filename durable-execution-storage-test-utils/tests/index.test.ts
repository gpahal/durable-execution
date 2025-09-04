import { existsSync } from 'node:fs'

import { InMemoryTaskExecutionsStorage } from 'durable-execution'

import { sleep } from '@gpahal/std/promises'

import {
  cleanupTemporaryFiles,
  runStorageBench,
  runStorageTest,
  withTemporaryDirectory,
  withTemporaryFile,
} from '../src'

describe('index', () => {
  afterAll(cleanupTemporaryFiles)

  it('should complete runStorageTest with in memory storage', { timeout: 60_000 }, async () => {
    const storage = new InMemoryTaskExecutionsStorage()
    await expect(runStorageTest(storage)).resolves.not.toThrow()
  })

  it('should complete runStorageBench with in memory storage', { timeout: 120_000 }, async () => {
    const test1 = async () => {
      const startTime = Date.now()
      await expect(
        runStorageBench('in memory', () => new InMemoryTaskExecutionsStorage(), {
          totalIterations: 1,
        }),
      ).resolves.not.toThrow()
      return Date.now() - startTime
    }

    const test2 = async () => {
      const startTime = Date.now()
      await expect(
        runStorageBench('in memory', () => new InMemoryTaskExecutionsStorage(), {
          storageSlowdownMs: 5,
          totalIterations: 1,
        }),
      ).resolves.not.toThrow()
      return Date.now() - startTime
    }

    const [duration1, duration2] = await Promise.all([test1(), test2()])
    expect(duration2).toBeGreaterThan(duration1)
  })

  it('should handle runStorageBench with invalid options', { timeout: 120_000 }, async () => {
    await expect(
      runStorageBench('in memory', () => new InMemoryTaskExecutionsStorage(), {
        executorsCount: 0,
      }),
    ).rejects.toThrow('Executors count must be at least 1')

    await expect(
      runStorageBench('in memory', () => new InMemoryTaskExecutionsStorage(), {
        totalIterations: 0,
      }),
    ).rejects.toThrow('Total iterations must be at least 1')
  })

  it('should handle withTemporaryDirectory', async () => {
    let dir: string | undefined
    await withTemporaryDirectory(async (d) => {
      dir = d
      await sleep(1)
      expect(d).toBeDefined()
    })

    expect(dir).toBeDefined()
    assert(dir)
    expect(existsSync(dir)).toBe(false)
  })

  it('should handle withTemporaryFile', async () => {
    let file: string | undefined
    await withTemporaryFile('x.txt', async (f) => {
      file = f
      await sleep(1)
      expect(f).toBeDefined()
      expect(f).toMatch(/x\.txt$/)
    })

    expect(file).toBeDefined()
    assert(file)
    expect(existsSync(file)).toBe(false)
  })

  it('should handle cleanupTemporaryFiles', async () => {
    await withTemporaryDirectory(async (d) => {
      await sleep(1)
      expect(d).toBeDefined()
      await cleanupTemporaryFiles()
      expect(existsSync(d)).toBe(false)
    })
  })
})
