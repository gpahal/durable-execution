import { sleep } from '@gpahal/std/promises'

import { DurableExecutor, InMemoryTaskExecutionsStorage } from '../src'

describe('backpressure', () => {
  describe('maxConcurrentExecutions', () => {
    let storage: InMemoryTaskExecutionsStorage
    let executor: DurableExecutor

    beforeEach(() => {
      storage = new InMemoryTaskExecutionsStorage()
      executor = new DurableExecutor(storage, {
        logLevel: 'error',
        backgroundProcessIntraBatchSleepMs: 50,
        maxConcurrentTaskExecutions: 3,
        maxTaskExecutionsPerBatch: 2,
      })
      executor.startBackgroundProcesses()
    })

    afterEach(async () => {
      await executor?.shutdown()
    })

    it('should respect maxConcurrentTaskExecutions limit', { timeout: 15_000 }, async () => {
      let runningCount = 0
      let maxObservedRunning = 0
      const completedPromises: Array<(value: void) => void> = []

      const longRunningTask = executor.task({
        id: 'long_running_task',
        timeoutMs: 10_000,
        run: async () => {
          runningCount++
          maxObservedRunning = Math.max(maxObservedRunning, runningCount)
          await new Promise<void>((resolve) => {
            completedPromises.push(() => {
              runningCount--
              resolve()
            })
          })

          return 'completed'
        },
      })

      const handles = await Promise.all([
        executor.enqueueTask(longRunningTask),
        executor.enqueueTask(longRunningTask),
        executor.enqueueTask(longRunningTask),
        executor.enqueueTask(longRunningTask),
        executor.enqueueTask(longRunningTask),
      ])

      await sleep(250)

      expect(runningCount).toBe(3)
      expect(executor.getRunningTaskExecutionIds().size).toBe(3)

      const stats = executor.getExecutorStats()
      expect(stats.currConcurrentTaskExecutions).toBe(3)
      expect(stats.maxConcurrentTaskExecutions).toBe(3)
      completedPromises.shift()!()
      completedPromises.shift()!()

      await sleep(250)

      expect(runningCount).toBe(3)
      expect(executor.getRunningTaskExecutionIds().size).toBe(3)

      while (completedPromises.length > 0) {
        completedPromises.shift()!()
      }

      await Promise.all(handles.map((handle) => handle.waitAndGetFinishedExecution()))

      expect(maxObservedRunning).toBe(3)
      expect(executor.getRunningTaskExecutionIds().size).toBe(0)
    })

    it('should respect maxTaskExecutionsPerBatch limit', async () => {
      const task = executor.task({
        id: 'quick_task',
        timeoutMs: 1000,
        run: async () => {
          await sleep(10)
          return 'completed'
        },
      })

      const handles = await Promise.all(
        Array.from({ length: 10 }, () => executor.enqueueTask(task)),
      )

      await sleep(250)

      const stats = executor.getExecutorStats()
      expect(stats.currConcurrentTaskExecutions).toBeLessThanOrEqual(3)
      expect(stats.maxTaskExecutionsPerBatch).toBe(2)

      await Promise.all(handles.map((handle) => handle.waitAndGetFinishedExecution()))
      expect(executor.getRunningTaskExecutionIds().size).toBe(0)
    })
  })

  describe('backpressureAtConcurrencyLimit', () => {
    let storage: InMemoryTaskExecutionsStorage
    let executor: DurableExecutor

    beforeEach(() => {
      storage = new InMemoryTaskExecutionsStorage()
      executor = new DurableExecutor(storage, {
        logLevel: 'error',
        backgroundProcessIntraBatchSleepMs: 50,
        maxConcurrentTaskExecutions: 2,
        maxTaskExecutionsPerBatch: 5,
      })
      executor.startBackgroundProcesses()
    })

    afterEach(async () => {
      await executor?.shutdown()
    })

    it('should apply backpressure when at concurrency limit', async () => {
      const taskStartTimes: Array<number> = []
      const taskCompletionSignals: Array<() => void> = []

      const monitoredTask = executor.task({
        id: 'monitored_task',
        timeoutMs: 5000,
        run: async () => {
          taskStartTimes.push(Date.now())
          await new Promise<void>((resolve) => {
            taskCompletionSignals.push(resolve)
          })
          return 'completed'
        },
      })

      const handles = await Promise.all([
        executor.enqueueTask(monitoredTask),
        executor.enqueueTask(monitoredTask),
        executor.enqueueTask(monitoredTask),
        executor.enqueueTask(monitoredTask),
      ])

      await sleep(250)

      expect(taskStartTimes.length).toBe(2)
      expect(executor.getRunningTaskExecutionIds().size).toBe(2)

      taskCompletionSignals.shift()!()

      await sleep(250)

      expect(taskStartTimes.length).toBe(3)
      expect(executor.getRunningTaskExecutionIds().size).toBe(2)

      while (taskCompletionSignals.length > 0) {
        taskCompletionSignals.shift()!()
        await sleep(50)
      }

      await Promise.all(handles.map((handle) => handle.waitAndGetFinishedExecution()))

      expect(taskStartTimes.length).toBe(4)
      expect(executor.getRunningTaskExecutionIds().size).toBe(0)
    })
  })

  describe('adaptiveBatchSizing', () => {
    let storage: InMemoryTaskExecutionsStorage
    let executor: DurableExecutor

    beforeEach(() => {
      storage = new InMemoryTaskExecutionsStorage()
      executor = new DurableExecutor(storage, {
        logLevel: 'error',
        backgroundProcessIntraBatchSleepMs: 50,
        maxConcurrentTaskExecutions: 5,
        maxTaskExecutionsPerBatch: 3,
      })
      executor.startBackgroundProcesses()
    })

    afterEach(async () => {
      await executor?.shutdown()
    })

    it('should adapt batch size based on available capacity', async () => {
      let currentlyRunning = 0
      const taskCompletionSignals: Array<() => void> = []
      const adaptiveTask = executor.task({
        id: 'adaptive_task',
        timeoutMs: 5000,
        run: async () => {
          currentlyRunning++
          await new Promise<void>((resolve) => {
            taskCompletionSignals.push(() => {
              currentlyRunning--
              resolve()
            })
          })
          return 'completed'
        },
      })

      await Promise.all([
        executor.enqueueTask(adaptiveTask),
        executor.enqueueTask(adaptiveTask),
        executor.enqueueTask(adaptiveTask),
      ])

      await sleep(250)
      expect(currentlyRunning).toBe(3)

      await Promise.all([
        executor.enqueueTask(adaptiveTask),
        executor.enqueueTask(adaptiveTask),
        executor.enqueueTask(adaptiveTask),
        executor.enqueueTask(adaptiveTask),
        executor.enqueueTask(adaptiveTask),
      ])

      await sleep(250)

      expect(currentlyRunning).toBe(5)
      expect(executor.getRunningTaskExecutionIds().size).toBe(5)

      while (taskCompletionSignals.length > 0) {
        taskCompletionSignals.shift()!()
        await sleep(50)
      }

      await sleep(500)
      expect(executor.getRunningTaskExecutionIds().size).toBe(0)
    })
  })

  describe('executorStats', () => {
    let storage: InMemoryTaskExecutionsStorage
    let executor: DurableExecutor

    beforeEach(() => {
      storage = new InMemoryTaskExecutionsStorage()
      executor = new DurableExecutor(storage, {
        logLevel: 'error',
        expireMs: 60_000,
        backgroundProcessIntraBatchSleepMs: 50,
        maxConcurrentTaskExecutions: 4,
        maxTaskExecutionsPerBatch: 2,
      })
      executor.startBackgroundProcesses()
    })

    afterEach(async () => {
      await executor?.shutdown()
    })

    it('should provide accurate executor statistics', () => {
      executor.task({
        id: 'stats_task',
        timeoutMs: 1000,
        run: () => 'result',
      })

      const stats = executor.getExecutorStats()

      expect(stats).toEqual({
        expireMs: 60_000,
        backgroundProcessIntraBatchSleepMs: 50,
        currConcurrentTaskExecutions: 0,
        maxConcurrentTaskExecutions: 4,
        maxTaskExecutionsPerBatch: 2,
        maxChildrenPerTaskExecution: 1000,
        maxSerializedInputDataSize: 1024 * 1024,
        maxSerializedOutputDataSize: 1024 * 1024,
        registeredTasksCount: 1,
        isShutdown: false,
      })
    })

    it('should update current executions in stats', async () => {
      const completionSignals: Array<() => void> = []
      const trackedTask = executor.task({
        id: 'tracked_task',
        timeoutMs: 5000,
        run: async () => {
          await new Promise<void>((resolve) => {
            completionSignals.push(resolve)
          })
          return 'completed'
        },
      })

      await Promise.all([
        executor.enqueueTask(trackedTask),
        executor.enqueueTask(trackedTask),
        executor.enqueueTask(trackedTask),
      ])

      await sleep(250)

      let stats = executor.getExecutorStats()
      expect(stats.currConcurrentTaskExecutions).toBe(3)

      completionSignals.shift()!()
      completionSignals.shift()!()

      await sleep(250)

      stats = executor.getExecutorStats()
      expect(stats.currConcurrentTaskExecutions).toBe(1)

      completionSignals.shift()!()

      await sleep(250)

      stats = executor.getExecutorStats()
      expect(stats.currConcurrentTaskExecutions).toBe(0)
    })
  })
})
