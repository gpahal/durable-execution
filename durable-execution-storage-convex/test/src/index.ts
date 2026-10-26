import { ConvexHttpClient } from 'convex/browser'
import { DurableExecutor } from 'durable-execution'
import { ConvexTaskExecutionsStorage } from 'durable-execution-storage-convex'

import { sleep } from '@gpahal/std/promises'

import { api } from '../convex/_generated/api'

async function main() {
  const convexUrl = process.env.VITE_CONVEX_URL
  if (convexUrl === undefined || convexUrl === '') {
    throw new Error('VITE_CONVEX_URL is not set')
  }
  const convexClient = new ConvexHttpClient(convexUrl)
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
  const executor = await DurableExecutor.make(storage, {
    enableStorageBatching: true,
  })
  await executor.start()
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
  // eslint-disable-next-line no-console -- this dev script prints the finished execution
  console.log(finishedExecution)
}

await main()
