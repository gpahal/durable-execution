# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a TypeScript monorepo containing a **durable execution engine** and related packages for running resilient, fault-tolerant tasks. The project enables workflow-style task processing with parent-child relationships, retries, timeouts, and crash recovery.

### Repository Structure

- **`durable-execution/`** - Core durable task execution engine
- **`storage-drizzle/`** - Drizzle ORM storage implementation for the core package
- **Root workspace** - Monorepo configuration with pnpm workspace and Turbo

## Common Development Commands

### Root Level (Monorepo Commands)

```bash
# Build all packages
pnpm build

# Run all tests across packages
pnpm test

# Run tests with coverage
pnpm test-coverage

# Type checking across all packages
pnpm type-check

# Lint all packages
pnpm lint

# Fix linting issues
pnpm lint-fix

# Format code with Prettier
pnpm fmt

# Check formatting
pnpm fmt-check

# Pre-commit checks (type-check + lint + fmt-check + build)
pnpm pre-commit
```

### Package-Specific Commands

Run from package directory or use `pnpm -F <package-name>` from root:

```bash
# durable-execution package
pnpm -F durable-execution test
pnpm -F durable-execution build-docs

# storage-drizzle package
pnpm -F storage-drizzle test-coverage

# Run single test file
cd durable-execution && npx vitest run tests/executor.test.ts
```

### Development Tooling

- Uses **pnpm workspaces** with **Turbo** for monorepo management
- **Vitest** for testing with v8 coverage provider
- **ESLint** and **Prettier** for code quality
- **TypeScript** with strict configuration
- **Simple Git Hooks** for pre-commit validation

## Core Architecture

### Durable Execution Engine (`durable-execution/`)

The main package provides a workflow engine for resilient task processing:

#### Key Components

1. **`DurableExecutor`** (`src/index.ts`) - Central orchestrator managing task lifecycle
   - Background processes for task processing, retry, and cleanup
   - Task registration and execution management
   - Storage abstraction with transaction support

2. **Task System** (`src/task.ts`, `src/task-internal.ts`)
   - Task state machine: `ready → running → [completed|failed|timed_out|cancelled]`
   - Parent tasks with children: `waiting_for_children_tasks → waiting_for_finalize_task → completed`
   - Support for both simple tasks and complex parent-child workflows

3. **Storage Layer** (`src/storage.ts`)
   - Pluggable persistence with atomic transactions
   - Includes in-memory implementation for testing
   - Query patterns by execution IDs, statuses, and timestamps

4. **Error Handling** (`src/errors.ts`)
   - Durable error classification (retryable vs non-retryable)
   - Custom error types for timeouts and cancellation

5. **Cancellation System** (`src/cancel.ts`)
   - Hierarchical cancellation with timeout support
   - Graceful shutdown handling

#### Task Execution Patterns

**Simple Task:**

```typescript
const task = executor.task({
  id: 'processFile',
  timeoutMs: 30_000,
  run: async (ctx, input) => { /* ... */ }
})
```

**Parent Task with Children:**

```typescript
const parentTask = executor.parentTask({
  id: 'uploadFile',
  runParent: async (ctx, input) => ({
    output: { /* parent result */ },
    childrenTasks: [
      { task: childTaskA, input: childInputA },
      { task: childTaskB, input: childInputB }
    ]
  }),
  finalizeTask: {
    id: 'finalize',
    run: async (ctx, { input, output, childrenTasksOutputs }) => {
      // Combine parent + children results
    }
  }
})
```

#### Background Processing

The executor runs 4 continuous background processes:

1. **Close finished tasks** - Update parent-child relationships and propagate results
2. **Retry expired tasks** - Handle crashed processes by retrying expired running tasks
3. **Cancel tasks** - Process cancellation requests with promise cancellation
4. **Process ready tasks** - Execute tasks that are ready to run

### Storage Implementation (`storage-drizzle/`)

Database persistence layer supporting PostgreSQL, MySQL, and SQLite:

#### Architecture

- **Database-specific implementations** (`src/pg.ts`, `src/mysql.ts`, `src/sqlite.ts`)
- **Common transformations** (`src/common.ts`) between storage objects and DB values
- **Proper indexing** for performance: unique on `execution_id`, composite on `(status, is_closed, expires_at)`

#### Key Features

- **Atomic transactions** for consistency
- **Hierarchical task support** with root/parent task tracking
- **JSON columns** for complex data (retry options, children tasks, errors)
- **Database-specific optimizations** (PostgreSQL timezone support, MySQL constraints, SQLite transaction mutex)

## Key Implementation Details

### Durability Features

1. **Process Crash Recovery** - Running tasks have expiration times and are automatically retried
2. **Configurable Retries** - Exponential backoff with jitter for transient failures
3. **Timeout Handling** - Per-task timeouts with graceful cancellation signals
4. **Graceful Shutdown** - Tasks receive shutdown signals and complete before executor stops

### Storage Requirements

Production deployments require persistent storage. The included `createInMemoryStorage()` is for testing only. Use `durable-execution-storage-drizzle` for production.

### Input Validation

Tasks support schema validation or custom validation:

```typescript
// With schema validation
const task = executor
  .inputSchema(zodSchema)
  .task({ /* options */ })

// With custom validation
const task = executor
  .validateInput(input => { /* validation */ })
  .task({ /* options */ })
```

## Testing Guidelines

### Test Structure

- **Integration tests** - Exercise full executor lifecycle with task enqueuing and execution
- **In-memory storage** - Fast testing with `createInMemoryStorage()`
- **Real databases** - `storage-drizzle` tests use PGlite, Testcontainers MySQL, and SQLite

### Test Patterns

```typescript
const storage = createInMemoryStorage()
const executor = new DurableExecutor(storage)
// Define tasks and test execution
await executor.start()
const handle = await executor.enqueueTask(task, input)
const result = await handle.waitAndGetTaskFinishedExecution()
await executor.shutdown()
```

### Coverage Requirements

- Each package maintains `coverage/coverage-final.json`
- CI expects this exact path for Codecov upload
- Run `pnpm test-coverage` from root for full coverage

## Development Workflow

### Making Changes

1. **Follow existing patterns** - Check neighboring files for conventions
2. **Update both packages** when making storage interface changes
3. **Test against all databases** in `storage-drizzle`
4. **Run pre-commit checks** before committing

### Performance Considerations

- Storage implementations are optimized with proper indexes
- Background processes use configurable batch sizes and sleep intervals
- Be mindful of query patterns when modifying storage interface methods

### Dependency Management

- Uses **pnpm catalog** for consistent versioning across packages
- **Peer dependencies** in `storage-drizzle` for `drizzle-orm` and `durable-execution`
- **Workspace dependencies** with `workspace:*` for local packages
