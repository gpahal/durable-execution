# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Building and Development

```bash
# Build the library
pnpm build

# Clean build directory
pnpm clean

# Build documentation
pnpm build-docs
```

### Testing

```bash
# Run all tests
pnpm test

# Run tests with coverage
pnpm test-coverage

# Run a specific test file
npx vitest run tests/[test-file].test.ts
```

### Code Quality

```bash
# Type checking
pnpm type-check

# Lint code
pnpm lint

# Fix linting issues
pnpm lint-fix
```

## Architecture Overview

This is a **durable execution engine** for TypeScript that provides resilient, fault-tolerant task processing. Tasks can be simple functions or complex workflows with parent-child relationships, retries, timeouts, and graceful cancellation.

### Core Components

- **`DurableExecutor`** (`src/index.ts`): Central orchestrator managing task lifecycle and background processes
- **Task System** (`src/task.ts`, `src/task-internal.ts`): Defines and manages task execution with state machine
- **Storage Abstraction** (`src/storage.ts`): Pluggable persistence layer with transaction support
- **Serialization** (`src/serializer.ts`): Pluggable serialization (defaults to superjson)
- **Error Handling** (`src/errors.ts`): Durable error system with retry classification
- **Cancellation** (`src/cancel.ts`): Hierarchical cancellation system with timeout support
- **Logging** (`src/logger.ts`): Simple logging interface for observability

### Key Architectural Patterns

#### Task State Machine

Tasks follow: `ready` → `running` → `[completed|failed|timed_out|cancelled]`
Parent tasks additionally: `waiting_for_children_tasks` → `waiting_for_finalize_task` → `completed`

#### Background Processing

The executor runs 4 continuous background processes:

- Close finished tasks and update parents
- Retry expired running tasks (crash recovery)
- Cancel tasks requiring promise cancellation
- Process ready tasks for execution

#### Parent-Child Task Relationships

- **Parent tasks** execute `runParent()` then spawn children in parallel
- **Children execute independently** with their own retries/timeouts
- **Optional `finalizeTask`** combines parent + children outputs
- **Error propagation**: child failure → parent `children_tasks_failed`, parent failure → children cancelled

### Storage Requirements

Implementations must support:

- **Atomic transactions** for consistency
- **Query patterns**: by execution IDs, statuses, timestamps
- **Suggested indexes**: `execution_id` (unique), `(status, isClosed, expiresAt)`, `(status, startAt)`

The included `createInMemoryStorage()` is for testing only. Production needs persistent storage like the `durable-execution-storage-drizzle` package.

### Durability Features

- **Process crash recovery**: Running tasks have expiration times and are automatically retried
- **Configurable retries**: Exponential backoff with jitter for transient failures
- **Timeout handling**: Per-task timeouts with graceful cancellation signals
- **Graceful shutdown**: Tasks receive shutdown signals and complete before executor stops

## Development Guidelines

### Task Definition Patterns

Tasks are defined on the executor instance:

```typescript
// Simple task
const taskA = e_xecutor.task({
  id: 'taskA',
  timeoutMs: 5000,
  run: async (ctx, input) => { /* ... */ }
})

// Parent task with children
const parentTask = executor.parentTask({
  id: 'parent',
  timeoutMs: 10_000,
  runParent: async (ctx, input) => ({
    output: parentResult,
    childrenTasks: [
      { task: childTaskA, input: childInputA },
      { task: childTaskB, input: childInputB }
    ]
  }),
  finalizeTask: {
    id: 'finalize',
    run: async (ctx, { input, output, childrenTasksOutputs }) => {
      // Combine results
    }
  }
})
```

### Input Validation

Use schema validation or custom validation:

```typescript
// With zod schema
const task = executor
  .inputSchema(z.object({ name: z.string() }))
  .task({ /* task options */ })

// With custom validation
const task = executor
  .validateInput(input => { /* validation logic */ })
  .task({ /* task options */ })
```

### Error Handling

- **Retryable errors**: Throw regular `Error` or `DurableExecutionError` with `isRetryable: true`
- **Non-retryable errors**: Use `DurableExecutionError` with `isRetryable: false`
- **Task context** provides `prevError` for retry scenarios

### Testing Patterns

Tests use the in-memory storage:

```typescript
const storage = createInMemoryStorage()
const executor = new DurableExecutor(storage)
// Define tasks and test execution
```

Most tests are integration tests that exercise the full executor lifecycle with task enqueuing, execution, and result verification.

## Project Structure

- **`src/`**: Main library source code
- **`tests/`**: Integration and unit tests
- **`build/`**: Compiled JavaScript output (generated)
- **`docs/`**: Generated TypeDoc documentation (generated)
- **`coverage/`**: Test coverage reports (generated)

The library is published as an ESM-only package with TypeScript declarations included.
