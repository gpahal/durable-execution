import { InMemoryTaskExecutionsStorage } from 'durable-execution'

import { runStorageBench } from '../src'

async function benchInMemory() {
  await runStorageBench('in memory', () => new InMemoryTaskExecutionsStorage())
}

async function main() {
  await benchInMemory()
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

waitForExitOrLogActiveHandles(5000)
