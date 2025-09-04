import type { StandardSchemaV1 } from '@standard-schema/spec'
import { getDotPath } from '@standard-schema/utils'
import { Deferred, Duration, Effect } from 'effect'
import { customAlphabet } from 'nanoid'

/**
 * Make a sleep with wakeup controller. The wakeup method is not sticky - if there is no active
 * sleep, the wakeup will be ignored.
 */
export const makeSleepWithWakeupController = Effect.fn(() =>
  Effect.acquireRelease(
    Effect.gen(function* () {
      // Mutex makes sure that only one sleep can be active at a time.
      const mutex = yield* Effect.makeSemaphore(1)
      // This is to make sure wakeup call in between Deferred.make and wakeupSignal = signal is not
      // lost. It is ignored everywhere else as the `wakeupSignal` would be set if needed.
      let pendingWakeup = false
      // This is used to wakeup the sleep.
      let wakeupSignal: Deferred.Deferred<void> | undefined = undefined

      const sleep = Effect.fn(
        (duration: Duration.DurationInput, options?: { jitterRatio?: number }) =>
          Effect.gen(function* () {
            // --- `pendingWakeup` used to wakeup the sleep ---
            pendingWakeup = false

            const signal = yield* Deferred.make<void>()
            const cleanup = Effect.gen(function* () {
              yield* Deferred.succeed(signal, undefined)
              pendingWakeup = false
              wakeupSignal = undefined
            })

            wakeupSignal = signal
            if (pendingWakeup) {
              yield* cleanup
              return
            }

            // --- `pendingWakeup` ignored, `wakeupSignal` used to wakeup the sleep ---
            if (options?.jitterRatio != null) {
              const jitterRatio = Math.max(0, Math.min(1, options.jitterRatio))
              duration = Duration.millis(
                Duration.toMillis(duration) * (1 + jitterRatio * (Math.random() * 2 - 1)),
              )
            }

            yield* Effect.race(Effect.sleep(duration), Deferred.await(signal)).pipe(
              Effect.ensuring(cleanup),
            )
          }).pipe(mutex.withPermits(1)),
      )

      const wakeup = Effect.gen(function* () {
        if (wakeupSignal) {
          // If wakeupSignal is already set, we can just succeed it.
          yield* Deferred.succeed(wakeupSignal, undefined)
          return
        }
        // In between `Deferred.make` and `wakeupSignal` is set, we can set `pendingWakeup` to
        // true. If that's not the case, this is a no-op and there is no active sleep.
        pendingWakeup = true
      })

      return {
        sleep,
        wakeup,
      }
    }),
    (controller) => controller.wakeup,
  ),
)

export type SleepWithWakeupController = Effect.Effect.Success<
  ReturnType<typeof makeSleepWithWakeupController>
>

const _ALPHABET = '0123456789ABCDEFGHJKMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz'

/**
 * Generate a random id.
 *
 * @param size - The length of the id. If not provided, it will be 24.
 * @returns A random id.
 */
export const generateId = customAlphabet(_ALPHABET, 24)

/**
 * Summarize standard schema issues.
 *
 * @param issues - The issues to summarize.
 * @returns A summary of the issues.
 */
export function summarizeStandardSchemaIssues(
  issues: ReadonlyArray<StandardSchemaV1.Issue>,
): string {
  let summary = ''
  for (const issue of issues) {
    if (summary) {
      summary += '\n'
    }

    summary += `× ${issue.message}`
    const dotPath = getDotPath(issue)
    if (dotPath) {
      summary += `\n  → at ${dotPath}`
    }
  }

  return summary
}
