---
'durable-execution': minor
'durable-execution-orpc-utils': minor
'durable-execution-storage-test-utils': minor
'durable-execution-storage-drizzle': minor
'durable-execution-storage-convex': minor
---

Upgrade every dependency. Breaking changes:

- Require Node.js 24.22 or later
- Require `effect` 4 instead of `effect` 3. Effect-typed exports, such as `AnySchema`, `EffectDurableExecutorOptionsSchema`, `DurableExecutionError`'s schema statics and the `EffectDurableExecutor` methods, now use Effect 4 types
- `Mutex`, re-exported from `@gpahal/std`, now has `runExclusive` in place of `acquire` and `release`
- Convex: `TaskExecutionsStorageComponent` is now the `ComponentApi` type that Convex 1.48 generates, which the package exports as `durable-execution-storage-convex/_generated/component.js`
- Export `TaskExecutionsStorageService`, which provides the storage to `makeEffectDurableExecutor`
