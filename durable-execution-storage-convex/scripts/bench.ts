import { ConvexHttpClient } from 'convex/browser'
import { ConvexTaskExecutionsStorage } from 'durable-execution-storage-convex'
import { runStorageBench } from 'durable-execution-storage-test-utils'

import { waitForExitOrLogActiveHandles } from '@gpahal/std-node/process'

import { api } from '../test/convex/_generated/api'

async function benchConvex() {
  await runStorageBench(
    'convex',
    () => {
      const convexClient = new ConvexHttpClient(process.env.VITE_CONVEX_URL!)
      const storage = new ConvexTaskExecutionsStorage(
        convexClient,
        'SUPER_SECRET',
        api.taskExecutionsStorage,
        {
          totalShards: 1,
          enableTestMode: true,
        },
      )
      return storage
    },
    {
      backgroundProcessesCount: 1,
      enableStorageBatching: true,
    },
  )
}

async function main() {
  await benchConvex()
}

await main()

waitForExitOrLogActiveHandles(10_000)
