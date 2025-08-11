import { createCancellablePromise, type CancelSignal } from '@gpahal/std/cancel'

import { DurableExecutionCancelledError } from './errors'

export async function createCancellablePromiseCustom<T>(
  promise: Promise<T>,
  signal?: CancelSignal,
  error?: Error,
): Promise<T> {
  return await createCancellablePromise(
    promise,
    signal,
    error ?? new DurableExecutionCancelledError(),
  )
}
