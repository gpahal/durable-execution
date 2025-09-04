import { spawn } from 'node:child_process'
import { rm } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { sleep } from '@gpahal/std/promises'

import { DurableExecutor } from '../src'
import { loadInMemoryTaskExecutionsStorageFromFile } from './in-memory-storage-utils'

const testsDir = path.dirname(fileURLToPath(import.meta.url))

describe('executorCrash', () => {
  it('should handle executor crash', async () => {
    const storageFilePath = path.join(testsDir, 'test.json')
    await rm(storageFilePath, { force: true })

    const child = spawn('node', [path.join(testsDir, 'executor-crash-script.js'), storageFilePath])
    console.log(`Child ${child.pid} spawned`)

    // Wait for crash
    await new Promise((resolve) => {
      child.on('exit', (code) => {
        console.log(`Child ${child.pid} exited with code ${code}`)
        resolve(undefined)
      })
    })

    const storage = await loadInMemoryTaskExecutionsStorageFromFile(storageFilePath)
    const executor = await DurableExecutor.make(storage, {
      logLevel: 'error',
      expireLeewayMs: 1000,
      backgroundProcessIntraBatchSleepMs: 50,
    })

    let executionCount = 0
    const task = executor.task({
      id: 'test',
      timeoutMs: 100_000,
      run: async () => {
        executionCount++
        await sleep(1)
      },
    })

    console.log('Starting executor')
    await executor.start()
    try {
      for (let i = 0; i < 10; i++) {
        const runningTaskExecutionIds = executor.getRunningTaskExecutionIds()
        if (runningTaskExecutionIds.size === 0) {
          console.log('No running task executions, waiting for 5 seconds')
          await sleep(1000)
          break
        }

        for (const executionId of runningTaskExecutionIds) {
          const handle = await executor.getTaskExecutionHandle(task, executionId)
          const finishedExecution = await handle.waitAndGetFinishedExecution({
            pollingIntervalMs: 100,
          })
          console.log('Task finished', finishedExecution)

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
        }
      }
    } finally {
      await executor.shutdown()
    }
  })
})
