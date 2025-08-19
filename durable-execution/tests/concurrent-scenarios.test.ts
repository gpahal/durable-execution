import { sleep } from '@gpahal/std/promises'

import { ChildTask, DurableExecutor, InMemoryTaskExecutionsStorage } from '../src'

describe('concurrentScenarios', () => {
  let storage: InMemoryTaskExecutionsStorage
  let executor1: DurableExecutor
  let executor2: DurableExecutor

  beforeEach(() => {
    storage = new InMemoryTaskExecutionsStorage()
    executor1 = new DurableExecutor(storage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    executor2 = new DurableExecutor(storage, {
      logLevel: 'error',
      backgroundProcessIntraBatchSleepMs: 50,
    })
    executor1.startBackgroundProcesses()
    executor2.startBackgroundProcesses()
  })

  afterEach(async () => {
    await executor1?.shutdown()
    await executor2?.shutdown()
  })

  it(
    'should handle concurrent parent task child completions correctly',
    { timeout: 15_000 },
    async () => {
      const child1 = executor1.task({
        id: 'concurrent_child1',
        timeoutMs: 1000,
        run: async () => {
          await sleep(100)
          return 'child1_result'
        },
      })

      const child2 = executor1.task({
        id: 'concurrent_child2',
        timeoutMs: 1000,
        run: async () => {
          await sleep(100)
          return 'child2_result'
        },
      })

      const child3 = executor1.task({
        id: 'concurrent_child3',
        timeoutMs: 1000,
        run: async () => {
          await sleep(100)
          return 'child3_result'
        },
      })

      executor2.task({
        id: 'concurrent_child1',
        timeoutMs: 1000,
        run: async () => {
          await sleep(100)
          return 'child1_result'
        },
      })
      executor2.task({
        id: 'concurrent_child2',
        timeoutMs: 1000,
        run: async () => {
          await sleep(100)
          return 'child2_result'
        },
      })
      executor2.task({
        id: 'concurrent_child3',
        timeoutMs: 1000,
        run: async () => {
          await sleep(100)
          return 'child3_result'
        },
      })

      const parentTask = executor1.parentTask({
        id: 'concurrent_parent',
        timeoutMs: 2000,
        runParent: () => {
          return {
            output: 'parent_output',
            children: [new ChildTask(child1), new ChildTask(child2), new ChildTask(child3)],
          }
        },
      })

      executor2.parentTask({
        id: 'concurrent_parent',
        timeoutMs: 2000,
        runParent: () => ({
          output: 'parent_output',
          children: [new ChildTask(child1), new ChildTask(child2), new ChildTask(child3)],
        }),
      })

      const handle = await executor1.enqueueTask(parentTask)

      const result = await handle.waitAndGetFinishedExecution()
      expect(result.status).toBe('completed')
      assert(result.status === 'completed')
      expect(result.output.children).toHaveLength(3)
      const outputs = result.output.children.map((c) =>
        c.status === 'completed' ? c.output : undefined,
      )
      expect(outputs).toEqual(['child1_result', 'child2_result', 'child3_result'])
    },
  )

  it(
    'should handle multiple executors processing different tasks simultaneously',
    { timeout: 10_000 },
    async () => {
      const task1 = executor1.task({
        id: 'distributed_task1',
        timeoutMs: 1000,
        run: async () => {
          await sleep(100)
          return 'result1'
        },
      })

      const task2 = executor1.task({
        id: 'distributed_task2',
        timeoutMs: 1000,
        run: async () => {
          await sleep(150)
          return 'result2'
        },
      })

      executor2.task({
        id: 'distributed_task1',
        timeoutMs: 1000,
        run: async () => {
          await sleep(100)
          return 'result1'
        },
      })
      executor2.task({
        id: 'distributed_task2',
        timeoutMs: 1000,
        run: async () => {
          await sleep(150)
          return 'result2'
        },
      })

      const handles = await Promise.all([
        executor1.enqueueTask(task1),
        executor1.enqueueTask(task2),
        executor1.enqueueTask(task1),
        executor2.enqueueTask(task2),
        executor2.enqueueTask(task1),
      ])

      const results = await Promise.all(
        handles.map((handle) => handle.waitAndGetFinishedExecution()),
      )

      const outputs: Array<string> = []
      for (const result of results) {
        expect(result.status).toBe('completed')
        assert(result.status === 'completed')
        expect(['result1', 'result2']).toContain(result.output)
        outputs.push(result.output)
      }

      const result1Count = outputs.filter((o) => o === 'result1').length
      const result2Count = outputs.filter((o) => o === 'result2').length
      expect(result1Count).toBe(3)
      expect(result2Count).toBe(2)
    },
  )

  it(
    'should handle version conflicts during concurrent updates gracefully',
    { timeout: 60_000 },
    async () => {
      const fastChild = executor1.task({
        id: 'fast_child',
        timeoutMs: 10_000,
        run: async (_, index: number) => {
          await sleep(100)
          return index
        },
      })

      executor2.task({
        id: 'fast_child',
        timeoutMs: 10_000,
        run: async (_, index: number) => {
          await sleep(100)
          return index
        },
      })

      const parentTask = executor1.parentTask({
        id: 'conflict_parent',
        timeoutMs: 5000,
        runParent: () => {
          return {
            output: 'parent_result',
            children: Array.from({ length: 1000 }, (_, index) => ({
              task: fastChild,
              input: index,
            })),
          }
        },
      })

      executor2.parentTask({
        id: 'conflict_parent',
        timeoutMs: 1000,
        runParent: () => {
          return {
            output: 'parent_result',
            children: Array.from({ length: 1000 }, (_, index) => ({
              task: fastChild,
              input: index,
            })),
          }
        },
      })

      const handle = await executor1.enqueueTask(parentTask)

      const result = await handle.waitAndGetFinishedExecution()
      expect(result.status).toBe('completed')
      assert(result.status === 'completed')
      expect(result.output.children).toHaveLength(1000)

      const outputs = result.output.children.map((c) =>
        c.status === 'completed' ? c.output : undefined,
      )
      expect(outputs.some((o) => o == null)).toBe(false)

      const uniqueOutputs = new Set(outputs)
      expect(uniqueOutputs.size).toBe(1000)
    },
  )
})
