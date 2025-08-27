import { ConvexHttpClient } from 'convex/browser'
import { DurableExecutor } from 'durable-execution'
import { ConvexTaskExecutionsStorage } from 'durable-execution-storage-convex'

import { sleep } from '@gpahal/std/promises'

import { api } from '../convex/_generated/api'

async function main() {
  const convexClient = new ConvexHttpClient(process.env.VITE_CONVEX_URL!)
  const storage = new ConvexTaskExecutionsStorage(
    convexClient,
    'SUPER_SECRET',
    api.taskExecutionsStorage,
    {
      enableTestMode: true,
    },
  )

  try {
    await runWithStorage(storage)
  } finally {
    await storage.deleteAll()
  }
}

async function runWithStorage(storage: ConvexTaskExecutionsStorage) {
  const executor = new DurableExecutor(storage, {
    enableStorageBatching: true,
  })
  executor.startBackgroundProcesses()
  try {
    await runWithExecutor(executor)
  } finally {
    await executor.shutdown()
  }
}

async function runWithExecutor(executor: DurableExecutor) {
  const task = executor.task({
    id: 'task',
    timeoutMs: 1000,
    run: async () => {
      await sleep(100)
      return 'output'
    },
  })

  const handle = await executor.enqueueTask(task)

  const finishedExecution = await handle.waitAndGetFinishedExecution()
  console.log(finishedExecution)
}

await main()
