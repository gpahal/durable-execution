import { sleep } from '@gpahal/std/promises'

import { DurableExecutor, InMemoryTaskExecutionsStorage } from '../src'

async function main() {
  if (process.argv.length < 3) {
    console.error('Usage: node tests/executor-crash-script.ts <filename> <should_crash>')
    return
  }

  const filename = process.argv[2]
  const storage = new InMemoryTaskExecutionsStorage()
  const onCrash = () => {
    void storage.saveToFile(filename).then(() => {
      // eslint-disable-next-line unicorn/no-process-exit
      process.exit(1)
    })
  }
  process.on('SIGINT', onCrash)
  process.on('SIGTERM', onCrash)

  const executor = new DurableExecutor(storage, {
    backgroundProcessIntraBatchSleepMs: 50,
  })
  executor.start()

  try {
    const task = executor.task({
      id: 'test',
      timeoutMs: 100_000,
      run: async () => {
        console.log('Task running')
        await sleep(10_000)
      },
    })

    const handle = await executor.enqueueTask(task)
    console.log('Task enqueued', handle.executionId)

    await sleep(1000)
    // eslint-disable-next-line unicorn/no-process-exit
    process.exit(1)
  } finally {
    await executor.shutdown()
  }
}

await main()
