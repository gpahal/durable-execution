import { ConvexHttpClient } from 'convex/browser'
import { ConvexTaskExecutionsStorage } from 'durable-execution-storage-convex'
import { runStorageBench } from 'durable-execution-storage-test-utils'

import { waitForExitOrLogActiveHandles } from '@gpahal/std-node/process'

import { api } from '../test/convex/_generated/api'

async function benchConvex() {
  await runStorageBench(
    'convex',
    (index) => {
      const convexClient = new ConvexHttpClient(process.env.VITE_CONVEX_URL!)
      const storage = new ConvexTaskExecutionsStorage(
        convexClient,
        'SUPER_SECRET',
        api.taskExecutionsStorage,
        {
          totalShards: 1,
          shards: [index],
          enableTestMode: true,
          logLevel: 'error',
        },
      )
      storage.startBackgroundProcesses()
      return storage
    },
    {
      storageCleanup: async (storage) => {
        await storage.shutdown()

        const timingStats = storage.getTimingStats()
        const timingStatsArr = Object.entries(timingStats).map(([key, value]) => ({
          key,
          count: value.count,
          meanMs: value.meanMs,
        }))
        timingStatsArr.sort((a, b) => b.count - a.count)
        console.log(`=> Timing stats for convex storage:`)
        for (const timingStat of timingStatsArr) {
          console.log(
            `  ${timingStat.key}:\n    [${timingStat.count}] ${timingStat.meanMs.toFixed(2)}ms`,
          )
        }

        const perSecondConvexCallsStats = storage.getPerSecondConvexCallsStats()
        console.log(
          `=> Per second convex calls stats:\n     total: ${perSecondConvexCallsStats.total}\n      mean: ${perSecondConvexCallsStats.mean.toFixed(2)}\n       min: ${perSecondConvexCallsStats.min.toFixed(2)}\n       max: ${perSecondConvexCallsStats.max.toFixed(2)}\n    median: ${perSecondConvexCallsStats.median.toFixed(2)}\n\n`,
        )
      },
      executorsCount: 1,
      backgroundProcessesCount: 1,
    },
  )
}

async function main() {
  await benchConvex()
}

await main()

waitForExitOrLogActiveHandles(10_000)
