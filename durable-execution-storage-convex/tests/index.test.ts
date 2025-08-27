import { execSync } from 'node:child_process'
import fs from 'node:fs/promises'
import path from 'node:path'

import { ConvexHttpClient } from 'convex/browser'
import { ConvexTaskExecutionsStorage } from 'durable-execution-storage-convex'
import {
  cleanupTemporaryFiles,
  runStorageTest,
  withTemporaryFile,
} from 'durable-execution-storage-test-utils'
import { GenericContainer } from 'testcontainers'

import { sleep } from '@gpahal/std/promises'

import { api } from '../test/convex/_generated/api'

describe('index', () => {
  afterAll(cleanupTemporaryFiles)

  it('should complete with convex storage', { timeout: 300_000 }, async () => {
    const container = await new GenericContainer(
      'ghcr.io/get-convex/convex-backend:33cef775a8a6228cbacee4a09ac2c4073d62ed13',
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
        await runStorageTest(storage, {
          enableStorageBatching: true,
        })
      })
    } finally {
      await container.stop()
    }
  })
})
