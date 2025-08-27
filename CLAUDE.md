# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Monorepo Commands

### Essential Commands

```bash
# Install dependencies and build all packages
pnpm install
pnpm build

# Run tests across all packages
pnpm test
pnpm test-coverage

# Quality checks
pnpm type-check       # TypeScript checking + docs validation
pnpm lint             # ESLint across all packages
pnpm lint-fix         # Auto-fix linting issues
pnpm fmt              # Format code with Prettier
pnpm fmt-check        # Check formatting

# Package-specific commands
pnpm -F durable-execution build
pnpm -F durable-execution test
pnpm -F durable-execution-storage-drizzle test

# Convex storage (tests run from parent package)
pnpm -F durable-execution-storage-convex test
pnpm -F durable-execution-storage-convex bench
pnpm -F durable-execution-storage-convex type-check

# Pre-commit validation
pnpm pre-commit       # Full validation pipeline

# Documentation and releases
pnpm build-docs       # Generate docs for main package
pnpm cs               # Create changeset and version
pnpm cs-publish       # Publish packages

# Benchmarking
pnpm bench            # Run performance benchmarks across all packages
```

### Testing Commands

```bash
# Run specific test files
pnpm -F durable-execution test tests/executor.test.ts
pnpm test -t "should handle timeout"    # Pattern matching

# Watch mode for development
pnpm -F durable-execution test -w
```

## Architecture Overview

This is a TypeScript monorepo for a **durable execution engine** - a system that runs tasks resiliently with automatic recovery from failures, retries, and process crashes.

### Core Concepts

**DurableExecutor**: Central orchestrator managing task lifecycles, background processing, and failure recovery. Creates tasks via fluent API with automatic type inference.

**Task Types**:

- **Simple Tasks**: Single function execution with retry/timeout logic
- **Parent Tasks**: Spawn parallel child tasks with optional finalization step for combining outputs
- **Sequential Tasks**: Chain dependent tasks where output flows to next input
- **Sleeping Tasks**: Tasks that wait for external events/webhooks to wake them up. Remain in `running` state until explicitly woken via `wakeupSleepingTaskExecution()` with a completion status and output
- **Polling Tasks**: Tasks that repeatedly check for a condition until met or timed out
- **Client Tasks**: Type-safe remote execution via `DurableExecutorClient`

**Storage Layer**: Abstract `TaskExecutionsStorage` interface requiring ACID transactions. Production uses Drizzle ORM implementations (PostgreSQL/MySQL/SQLite), testing uses `InMemoryTaskExecutionsStorage`. For storage implementations that don't support batch operations natively, use `TaskExecutionsStorageWithBatching` wrapper to convert individual operations into efficient batch requests (DataLoader pattern). The executor internally uses `TaskExecutionsStorageInternal` with `BatchRequester` to optimize API calls by batching multiple operations when `enableBatching` is enabled.

**Execution Statuses**: Tasks progress through `ready` → `running` → `completed`/`failed`/`timed_out`/`cancelled`. Parent tasks include `waiting_for_children` → `waiting_for_finalize` states. Sleeping tasks are enqueued directly in `running` state and wait until woken up externally.

### Package Structure

**Core Packages**:

- `durable-execution/`: Main execution engine with comprehensive TypeScript API
- `durable-execution-orpc-utils/`: oRPC integration for remote execution servers
- `durable-execution-storage-drizzle/`: Production-ready database storage (PostgreSQL, MySQL, SQLite)
- `durable-execution-storage-convex/`: Convex database storage implementation
- `durable-execution-storage-test-utils/`: Test utilities for validating storage implementations

**Package-Specific Architecture Notes**:

- `durable-execution-storage-convex/`: Uses Convex components with a test subdirectory as a separate workspace (`durable-execution-storage-convex/test`) for Convex component setup. Tests and benchmarks are run from the parent package.

**Monorepo Technology**:

- **pnpm workspaces** with catalog-based dependency management
- **Turbo** for parallel builds and task orchestration
- **TypeScript strict mode** with ESM-only modules (Node.js ≥20)
- **Vitest** for testing with 120s timeout for storage tests
- **Changesets** for coordinated releases

### Task Definition Patterns

```ts
// Simple task with validation
const emailTask = executor
  .inputSchema(z.object({ to: z.string(), subject: z.string() }))
  .task({
    id: 'sendEmail',
    timeoutMs: 30000,
    retryOptions: { maxAttempts: 3, baseDelayMs: 1000 },
    run: async (ctx, input) => ({ messageId: 'msg_123' })
  })

// Parent task with children and finalization
const workflowTask = executor.parentTask({
  id: 'processWorkflow',
  timeoutMs: 60000,
  runParent: (ctx, input) => ({
    output: `Processing ${input.name}`,
    children: [
      new ChildTask(childTaskA, { name: input.name }),
      new ChildTask(childTaskB, { name: input.name })
    ]
  }),
  finalize: {
    id: 'workflowFinalize',
    timeoutMs: 30000,
    run: (ctx, { output, children }) => ({
      parentResult: output,
      childResults: children.map(c => c.output)
    })
  }
})

// Sequential task chain
const pipeline = executor.sequentialTasks('seq', [taskA, taskB, taskC])

// Sleeping task for webhooks/events
const waitTask = executor.sleepingTask<WebhookData>({
  id: 'waitForWebhook',
  timeoutMs: 60000
})
// Wake up with: executor.wakeupSleepingTaskExecution(waitTask, entityId, { status: 'completed', output: data })

// Polling task for condition checking
const pollTask = executor.pollingTask('poll', checkConditionTask, maxAttempts, sleepMsBeforeRun)
```

### Error Handling Patterns

```ts
// Use DurableExecutionError for domain-specific errors
throw DurableExecutionError.nonRetryable('Invalid input data')
throw DurableExecutionError.retryable('Network timeout, please retry')

// Check child task status in finalize
if (child.status !== 'completed') {
  throw DurableExecutionError.nonRetryable(`Child task failed: ${child.error?.message}`)
}

// Handle sleeping task wake-up failures
try {
  await executor.wakeupSleepingTaskExecution(task, id, { status: 'completed', output })
} catch (error) {
  // Handle wake-up errors (task not found, already woken, etc.)
}
```

### Testing Approach

**Storage Testing**: Use `durable-execution-storage-test-utils.runStorageTest()` for comprehensive storage implementation validation.

**Executor Testing**: Focus on integration scenarios with realistic concurrency (250+ tasks), parent-child hierarchies, retry mechanisms, timeout handling, and graceful shutdown.

**Database Testing**: Testcontainers for PostgreSQL, Testcontainers for MySQL, LibSQL for SQLite.

**Testing Configuration**: Vitest with 120s timeout for storage tests, globals enabled, Node.js environment. Coverage reports use V8 provider.

**Key Test Files**:

- `examples.test.ts`: End-to-end patterns matching documentation
- `executor-crash.test.ts`: Process failure recovery scenarios
- `concurrent-scenarios.test.ts`: Race conditions and high concurrency
- `parent-task.test.ts`: Parent-child orchestration patterns
- `task.test.ts`: Sleeping and polling task behaviors

### Development Standards

**TypeScript**: Strict configuration with ESM modules only. Export types explicitly using `export type`. Use Zod for runtime validation with Standard Schema spec.

**Code Style**: ESLint with `@gpahal/eslint-config/base`, Prettier with import sorting. No comments unless documenting complex domain logic.

**Serialization**: Use `createSuperjsonSerializer()` for complex objects. Ensure all task inputs/outputs are JSON-serializable.

**Concurrency**: Tasks execute concurrently by default. Use storage mutex for atomic state updates. Implement proper cancellation signal handling.

### Important Conventions

- All packages follow ESM-only with `"type": "module"`
- Use workspace dependencies for inter-package references
- Export public APIs only through `src/index.ts` files
- Task IDs should be unique and descriptive
- Always test with multiple executor instances for concurrency
- Use `InMemoryTaskExecutionsStorage` for fast, deterministic tests
- Follow fluent API patterns when extending task types
- Maintain backward compatibility for storage interface changes
- Sleeping tasks require external wake-up calls via `wakeupSleepingTaskExecution`
- Polling tasks automatically retry with delays until condition is met
- Parent task finalize functions receive all child outputs including failed ones (Promise.allSettled pattern)

### Package-Specific Notes

**durable-execution-storage-drizzle**: Requires peer dependencies `drizzle-orm` and `durable-execution`. Database implementations in separate files (`pg.ts`, `mysql.ts`, `sqlite.ts`) with shared logic in `common.ts`.

**durable-execution-storage-convex**: Requires peer dependencies `convex` and `durable-execution`. Uses Convex components with auth-protected public API.

**durable-execution-storage-test-utils**: Provides `runStorageTest()` function for comprehensive storage validation. Includes utilities for temporary files and directories with automatic cleanup.

### Benchmarking

Each storage package includes benchmark scripts to measure performance:

**Benchmark Scenario**: By default, creates 100 parent tasks (each with 50 child tasks), 100 sequential tasks, and 100 polling tasks with 1 executor, measuring execution time for approximately 5,300 total task executions.

**Storage Benchmarks**:

- **durable-execution-storage-drizzle**: Tests PostgreSQL, MySQL, and SQLite using Testcontainers
- **durable-execution-storage-convex**: Tests Convex storage with performance metrics
- **durable-execution-storage-test-utils**: Tests InMemoryStorage baseline performance

**Running Benchmarks**:

```bash
# All storage implementations
pnpm bench

# Specific package benchmarks
pnpm -F durable-execution-storage-drizzle bench
pnpm -F durable-execution-storage-convex bench
pnpm -F durable-execution-storage-test-utils bench
```

**Benchmark Output**: Reports execution time for 3 iterations (including 1 warmup) with graceful shutdown handling.
