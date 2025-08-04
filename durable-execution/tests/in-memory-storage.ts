import { readFile, writeFile } from 'node:fs/promises'

import { createInMemoryStorage, type DurableStorage, type DurableStorageTx } from '../src'

export class InMemoryStorage implements DurableStorage {
  private readonly internalStorage: ReturnType<typeof createInMemoryStorage>

  constructor({ enableDebug = false }: { enableDebug?: boolean } = {}) {
    this.internalStorage = createInMemoryStorage({ enableDebug })
  }

  async withTransaction<T>(fn: (tx: DurableStorageTx) => Promise<T>): Promise<T> {
    return this.internalStorage.withTransaction(fn)
  }

  async saveToFile(path: string): Promise<void> {
    await this.internalStorage.save((s) => writeFile(path, s))
  }

  async loadFromFile(path: string): Promise<void> {
    await this.internalStorage.load(() => readFile(path, 'utf8'))
  }
}
