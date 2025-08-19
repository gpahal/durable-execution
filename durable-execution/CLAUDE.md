# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Essential Commands

```bash
# Build the library
pnpm build

# Run tests
pnpm test              # Run all tests
pnpm test-coverage     # Run tests with coverage report

# Type checking and linting
pnpm type-check        # TypeScript type checking + docs validation
pnpm lint              # Run ESLint
pnpm lint-fix          # Auto-fix linting issues

# Documentation
pnpm build-docs        # Generate TypeDoc documentation

# Clean build artifacts
pnpm clean
```

### Testing Specific Files

```bash
# Run a specific test file
pnpm test tests/executor.test.ts

# Run tests in watch mode
pnpm test -w

# Run tests matching a pattern
pnpm test -t 'should handle timeout'
```

## Architecture Overview

### Core Components

**DurableExecutor** (`src/executor.ts`): Central orchestrator that manages task execution, providing enqueueing, background processing, state management, and failure recovery.

**Task System** (`src/task.ts`, `src/task-internal.ts`):

- Simple tasks: Single function execution with retry/timeout
- Parent tasks: Spawn parallel child tasks with optional finalization
- Sequential tasks: Chain dependent tasks where output flows to input
- Sleeping tasks: Wait for external events/webhooks to wake them
- Polling tasks: Repeatedly check conditions until met or timeout
- Finalize tasks: Post-processing after parent+children complete

**Storage Layer** (`src/storage.ts`, `src/in-memory-storage.ts`): Abstract storage interface requiring ACID transactions. InMemoryStorage for testing, external packages provide production storage (Drizzle ORM).

**Error System** (`src/errors.ts`): Hierarchical error system with `DurableExecutionError` base class. Errors explicitly control retry behavior via `retryable()` and `nonRetryable()` static methods.

### Task Definition Patterns

Tasks use a fluent API with optional schema validation:

```ts
// Simple task
const task = executor
  .inputSchema(z.object({ /* schema */ }))  // Optional validation
  .task({
    id: 'uniqueId',
    timeoutMs: 30_000,
    retryOptions: { maxAttempts: 3 },
    run: async (ctx, input) => { /* logic */ }
  })

// Parent task with finalization
const parentTask = executor.parentTask({
  id: 'parent',
  runParent: (ctx, input) => ({
    output: parentOutput,
    children: [new ChildTask(childTask, childInput)]
  }),
  finalize: {
    id: 'finalize',
    run: (ctx, { output, children }) => generateCombinedOutput(output, children)
  }
})

// Sleeping task for external events
const sleepTask = executor.sleepingTask<OutputType>({
  id: 'waitForWebhook',
  timeoutMs: 60_000
})
// Wake up: executor.wakeupSleepingTaskExecution(sleepTask, id, { status: 'completed', output })

// Polling task
const pollTask = executor.pollingTask('poll', checkTask, maxAttempts, delayMs)
```

### Execution State Machine

Tasks progress through statuses: `ready` → `running` → `completed`/`failed`/`timed_out`. Parent tasks add `waiting_for_children` and `waiting_for_finalize` statuses. Sleeping tasks are enqueued directly in `running` state and wait until woken externally. All terminal statuses transition through `closing` → `closed` for cleanup.

### Key Design Principles

- **Resilience**: Automatic recovery from process failures via task expiration and re-queueing
- **Type Safety**: Full TypeScript with strict mode, generic type inference across task chains
- **Testability**: Comprehensive test suite focusing on concurrency, failure scenarios, and integration testing
- **Modularity**: Clean separation of concerns with dependency injection for storage, serialization, and logging

### Testing Approach

Tests in `tests/` directory focus on integration scenarios. Use `InMemoryTaskExecutionsStorage` for fast, deterministic testing. Key test files:

- `examples.test.ts`: End-to-end patterns from documentation
- `executor-crash.test.ts`: Process failure recovery
- `concurrent-scenarios.test.ts`: Race conditions
- `parent-task.test.ts`: Parent-child orchestration

### Important Conventions

- All public APIs exported through `src/index.ts`
- Use existing error types from `src/errors.ts` for consistency
- Follow fluent API pattern for new task types
- Maintain backward compatibility for storage interface changes
- Test concurrency scenarios thoroughly with multiple executor instances
- Sleeping tasks require unique IDs for wake-up correlation
- Polling tasks use `sleepMsBeforeRun` for delay between attempts
- Parent task finalize receives all child outputs (Promise.allSettled pattern)
