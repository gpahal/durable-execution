/**
 * A pool for managing multiple promises with automatic cleanup and settling.
 *
 * @example
 * ```ts
 * const pool = new PromisePool()
 *
 * // Add promises that auto-remove when complete
 * pool.addPromise(fetchData())
 * pool.addPromise(processTask())
 *
 * // Wait for all promises to complete
 * await pool.allSettled()
 * ```
 */
export class PromisePool {
  private readonly promises: Set<Promise<void>>

  constructor() {
    this.promises = new Set()
  }

  /**
   * Add a promise to the pool. The promise is automatically removed from the pool when it settles
   * (either resolves or rejects).
   *
   * @param promise - The promise to add to the pool
   */
  addPromise(promise: Promise<void>) {
    this.promises.add(promise)
    promise
      .finally(() => {
        this.promises.delete(promise)
      })
      .catch(() => {
        // Silently catch to prevent unhandled rejection warnings. The actual error handling should
        // be done by the caller.
      })
  }

  /**
   * Add multiple promises to the pool. The promises are automatically removed from the pool when
   * they settle (either resolves or rejects).
   *
   * @param promises - The promises to add to the pool
   */
  addPromises(promises: ReadonlyArray<Promise<void>>) {
    for (const promise of promises) {
      this.addPromise(promise)
    }
  }

  /**
   * Wait for all promises in the pool to settle. Returns when all promises have either resolved
   * or rejected similarly to `Promise.allSettled`.
   *
   * @returns A promise that resolves when all promises in the pool have settled
   */
  async allSettled(): Promise<void> {
    const currentPromises = [...this.promises]
    if (currentPromises.length === 0) {
      return
    }

    await Promise.allSettled(currentPromises)
  }

  /**
   * Get the current number of pending promises in the pool.
   *
   * @returns The number of promises currently in the pool
   */
  get size(): number {
    return this.promises.size
  }
}
