import { InMemoryTaskExecutionsStorage } from 'durable-execution'

import { waitForExitOrLogActiveHandles } from '@gpahal/std-node/process'

import { runStorageBench } from '../src'

async function benchInMemory() {
  await runStorageBench('in memory', () => new InMemoryTaskExecutionsStorage())
}

async function main() {
  await benchInMemory()
}

await main()

waitForExitOrLogActiveHandles(5000)
