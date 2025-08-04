import { DurableTaskCancelledError } from 'src'
import { describe, it } from 'vitest'

import { sleep } from '@gpahal/std/promises'

import {
  createCancellablePromise,
  createCancelSignal,
  createTimeoutCancelSignal,
} from '../src/cancel'

describe('cancel signal', () => {
  it('should create a cancel signal', () => {
    const [cancelSignal, cancel] = createCancelSignal()
    let executed = 0
    cancelSignal.onCancelled(() => {
      executed++
    })
    cancel()
    expect(executed).toBe(1)
  })

  it('should create a cancel signal with an abort signal', () => {
    const abortController = new AbortController()
    const [cancelSignal, _] = createCancelSignal({ abortSignal: abortController.signal })
    let executed = 0
    cancelSignal.onCancelled(() => {
      executed++
    })
    abortController.abort()
    expect(executed).toBe(1)
  })

  it('should create a cancel signal with a timeout', async () => {
    const cancelSignal = createTimeoutCancelSignal(10)
    let executed = 0
    cancelSignal.onCancelled(() => {
      executed++
    })
    expect(executed).toBe(0)
    await sleep(100)
    expect(executed).toBe(1)
  })
})

describe('cancellable promise', () => {
  it('should create a cancellable promise that completes', async () => {
    const [cancelSignal, _] = createCancelSignal()
    const cancellablePromise = createCancellablePromise(sleep(10), cancelSignal)
    expect(await cancellablePromise).toBe(undefined)
  })

  it('should create a cancellable promise that is cancelled', async () => {
    const [cancelSignal, cancel] = createCancelSignal()
    const cancellablePromise = createCancellablePromise(sleep(500), cancelSignal)
    cancel()
    await expect(cancellablePromise).rejects.toThrow(DurableTaskCancelledError)
  })
})
