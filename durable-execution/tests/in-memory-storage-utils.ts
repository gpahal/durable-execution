import { readFile, writeFile } from 'node:fs/promises'

import { InMemoryTaskExecutionsStorage } from '../src'

export async function saveInMemoryTaskExecutionsStorageToFile(
  storage: InMemoryTaskExecutionsStorage,
  path: string,
): Promise<void> {
  await storage.save(async (s) => writeFile(path, s))
}

export async function loadInMemoryTaskExecutionsStorageFromFile(
  path: string,
): Promise<InMemoryTaskExecutionsStorage> {
  const storage = new InMemoryTaskExecutionsStorage()
  await storage.load(async () => readFile(path, 'utf8'))
  return storage
}
