import { readFile, writeFile } from 'node:fs/promises'

import { InMemoryStorage as InternalInMemoryStorage } from '../src'

export class InMemoryStorage extends InternalInMemoryStorage {
  async saveToFile(path: string): Promise<void> {
    await this.save((s) => writeFile(path, s))
  }

  async loadFromFile(path: string): Promise<void> {
    await this.load(() => readFile(path, 'utf8'))
  }
}
