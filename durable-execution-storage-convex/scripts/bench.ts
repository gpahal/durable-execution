import { execSync } from 'node:child_process'
import fs from 'node:fs/promises'
import path from 'node:path'

import { ConvexHttpClient } from 'convex/browser'
import { ConvexTaskExecutionsStorage } from 'durable-execution-storage-convex'
import { runStorageBench, withTemporaryFile } from 'durable-execution-storage-test-utils'
import { GenericContainer } from 'testcontainers'

import { sleep } from '@gpahal/std/promises'

import { api } from '../test/convex/_generated/api'

async function benchConvex() {
  const container = await new GenericContainer(
    'ghcr.io/convex/convex-backend:65a94913ee82308e7c6f295c4f964eaa34c0c6d7',
  )
    .withEnvironment({
      INSTANCE_NAME: 'anonymous-convex-self-hosted',
      INSTANCE_SECRET: '345afcdc2e8260c0e3fd393097dfa54166281806cf29561669602b8aad0b5fdc',
    })
    .withExposedPorts(3210, 3211)
    .start()
  const convexUrl = `http://127.0.0.1:${container.getMappedPort(3210)}`
  try {
    await withTemporaryFile('.env.local', async (tmpEnvFile) => {
      await fs.writeFile(
        tmpEnvFile,
        `CONVEX_SELF_HOSTED_URL='${convexUrl}'\nCONVEX_SELF_HOSTED_ADMIN_KEY='anonymous-convex-self-hosted|01b75b9e3720fcaa15d96e38984ad8afe972ab6b056b0d89c437020d541a83dd1bff4ce905'\n`,
      )

      const tmpEnvFileAbsPath = path.resolve(tmpEnvFile)
      const cmd = `pnpm convex deploy --env-file ${tmpEnvFileAbsPath} --yes`
      execSync(cmd, { stdio: 'inherit' })

      await sleep(1000)
      await runStorageBench(
        'convex',
        () => {
          const convexClient = new ConvexHttpClient(convexUrl)
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
          enableStorageBatching: true,
        },
      )
    })
  } finally {
    await container.stop()
  }
}

async function main() {
  await benchConvex()
}

/**
 * Exit with an error, listing the resources that keep the process alive, when it hasn't exited
 * `timeoutMs` after the bench finished.
 */
function waitForExitOrLogActiveHandles(timeoutMs: number): void {
  setTimeout(() => {
    console.error(
      `Process did not exit within ${timeoutMs}ms. Active resources:`,
      process.getActiveResourcesInfo(),
    )
    // eslint-disable-next-line unicorn/no-process-exit -- the bench script exits when leftover resources keep it alive
    process.exit(1)
  }, timeoutMs).unref()
}

await main()

waitForExitOrLogActiveHandles(10_000)
