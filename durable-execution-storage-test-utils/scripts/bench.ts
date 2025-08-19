import { InMemoryTaskExecutionsStorage } from 'durable-execution'

import { runStorageBench } from '../src'

async function benchInMemory() {
  const storage = new InMemoryTaskExecutionsStorage()
  await runStorageBench('in memory', storage)
}

async function main() {
  await benchInMemory()
}

await main()
