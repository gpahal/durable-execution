# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Monorepo Management

```bash
# Install dependencies for all packages
pnpm install

# Build all packages
pnpm build

# Run tests across all packages
pnpm test
pnpm test-coverage

# Type checking and linting
pnpm type-check
pnpm lint
pnpm lint-fix

# Code formatting
pnpm fmt
pnpm fmt-check

# Pre-commit validation (runs all checks)
pnpm pre-commit

# Generate documentation
pnpm build-docs

# Changeset management for publishing
pnpm cs          # Create changeset
pnpm cs-publish  # Publish packages
```

### Package-Specific Development

```bash
# Run commands in specific package
cd durable-execution && pnpm test
cd storage-drizzle && pnpm build

# Run specific test files
pnpm test tests/executor.test.ts
pnpm test tests/parent-child.test.ts

# Run tests matching pattern
pnpm test -- --grep "parent task"
pnpm test -- --grep "cancellation"
```

## Architecture Overview

This monorepo implements a **durable task execution engine** for TypeScript that provides resilient, transactional task execution with automatic retry, cancellation, and parent-child task relationships.

### Core Design Principles

- **State Machine Pattern**: Tasks transition through well-defined states

```text
ready → running → [completed|failed|timed_out|waiting_for_children|cancelled]
```

- **Transactional Consistency**: All state changes are atomic via storage transactions

- **Process Recovery**: Automatic recovery from crashes using expiration timestamps

- **Distributed Execution**: Multiple executors can share the same storage backend

### Package Structure

#### durable-execution (Core Package)

- **DurableExecutor** (`src/index.ts`): Main orchestrator managing task lifecycle
- **Task System** (`src/task.ts`): Task definitions with `task()`, `parentTask()`, and schema variants
- **Storage Interface** (`src/storage.ts`): Abstract `DurableStorage` interface
- **Cancellation** (`src/cancel.ts`): Custom promise-based cancellation signals
- **Background Processes**: Task execution, recovery, cleanup, and cancellation

#### storage-drizzle (Storage Implementation)

- **Database Adapters**: PostgreSQL (`pg.ts`), MySQL (`mysql.ts`), SQLite (`sqlite.ts`)
- **Schema**: `durable_task_executions` table with indexes for performance
- **Conversion Utilities** (`common.ts`): Database ↔ runtime object mapping

### Task Implementation Patterns

#### Simple Tasks

```ts
const task = executor.schemaTask({
  id: 'taskId',
  timeoutMs: 30_000,
  inputSchema: v.object({ /* schema */ }),
  run: async (ctx, input) => {
    // Check cancellation in long-running tasks
    if (ctx.cancelSignal.isCancelled) return
    // Task logic here
    return output
  }
})
```

#### Parent-Child Tasks

```ts
const parentTask = executor.parentTask({
  id: 'parent',
  timeoutMs: 60_000,
  runParent: (ctx, input) => ({
    output: parentOutput,
    children: [
      { task: childTask1, input: childInput1 },
      { task: childTask2, input: childInput2 }
    ]
  }),
  onRunAndChildrenComplete: {
    id: 'combineResults',
    timeoutMs: 30_000,
    run: (ctx, { output, childrenOutputs }) => {
      // Combine parent and children outputs
      return combinedOutput
    }
  }
})
```

### Storage Implementation Requirements

When implementing custom storage:

1. **Transaction Support**: All operations must be within transactions
2. **Pessimistic Locking**: Use `getExecutionForUpdate` for concurrent access
3. **Required Operations**:
   - `createExecution`, `updateExecution`, `getExecution`
   - `getReadyExecutions`, `getExpiredExecutions`
   - `getFinishedExecutions`, `getExecutionsNeedingPromiseCancellation`

### Critical Implementation Notes

1. **Idempotency**: Tasks MUST be idempotent - they may execute multiple times on failure

2. **Transaction Boundaries**: Never perform I/O operations within storage transactions

3. **Expiration Handling**: Set `expiresAt = now + timeoutMs + leeway` when marking as running

4. **Cancellation Flow**:
   - Set `status = 'cancelled'` and `needsPromiseCancellation = true`
   - Background process handles actual promise cancellation
   - Propagates to children tasks

5. **Parent-Child Coordination**:
   - Parents wait for all children before completing
   - Child failure causes parent failure
   - Cancellation cascades to all descendants

### Testing Strategy

- **In-Memory Storage** (`tests/in-memory-storage.ts`): Reference implementation for testing
- **Test Categories**:
  - Unit tests: Individual component behavior
  - Integration tests: Full executor workflows
  - Crash recovery tests: Process failure simulation
  - Schema validation tests: Input/output validation

### Development Workflow

1. **Making Changes**: Run `pnpm pre-commit` before committing
2. **Adding Features**: Update tests, add schema validation if needed
3. **Database Changes**: Update all three storage implementations
4. **Publishing**: Use changesets (`pnpm cs`) for version management
