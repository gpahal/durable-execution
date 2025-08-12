# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Installation and Setup

```bash
pnpm install   # Install dependencies (requires pnpm@10, Node >= 20)
```

### Build Commands

```bash
pnpm build          # Build all packages using Turbo
pnpm clean          # Clean all build artifacts
pnpm build-docs     # Generate TypeDoc documentation for core package
```

### Testing Commands

```bash
pnpm test           # Run all tests across packages
pnpm test-coverage  # Run tests with coverage reporting

# Run tests for specific packages
pnpm -F durable-execution test
pnpm -F durable-execution-storage-drizzle test
pnpm -F durable-execution-orpc-utils test
pnpm -F durable-execution-storage-test-utils test

# Run specific test files
pnpm vitest run tests/executor.test.ts           # Core executor tests
pnpm vitest run tests/parent-task.test.ts        # Parent-child task tests
pnpm vitest run tests/concurrent-scenarios.test.ts # Concurrency tests
```

### Code Quality Commands

```bash
pnpm type-check     # Type check all packages
pnpm lint           # Lint all packages
pnpm lint-fix       # Lint with automatic fixes
pnpm fmt            # Format code with Prettier
pnpm fmt-check      # Check code formatting
```

### Git Hooks and Publishing

```bash
pnpm pre-commit     # Run type-check, lint, fmt-check, and build (used by git hooks)
pnpm cs             # Create changesets for versioning
pnpm cs-publish     # Publish packages after running tests
```

## Architecture Overview

This is a monorepo containing a durable execution engine for running resilient tasks that survive process failures, network issues, and transient errors.

### Package Structure

**Core Package - `durable-execution/`**

- Main durable execution engine and task orchestration
- Exports: `DurableExecutor`, task types, storage interface, error types
- Key files: `executor.ts`, `task.ts`, `storage.ts`, `in-memory-storage.ts`

**Storage Implementation - `durable-execution-storage-drizzle/`**

- Drizzle ORM-based storage for PostgreSQL, MySQL, and SQLite
- Production-ready storage implementations with proper indexing and transactions
- Exports: `createPgStorage()`, `createMySqlStorage()`, `createSQLiteStorage()`, table schema creators

**oRPC Integration - `durable-execution-orpc-utils/`**

- Utilities for running durable executor as a separate server process
- Type-safe client-server communication via oRPC
- Exports: server router creation, client handles, procedure conversion

**Test Utilities - `durable-execution-storage-test-utils/`**

- Comprehensive test suite for validating storage implementations
- Exports: `runStorageTest()` function and test utilities

### Core Architecture Patterns

#### DurableExecutor Lifecycle

1. Create executor with Storage implementation
2. Call `startBackgroundProcesses()` to begin task processing
3. Enqueue tasks via `enqueueTask(task, input)`
4. Manage task execution through handles
5. Call `shutdown()` when done

#### Task Types and Execution Flow

- **Simple Tasks**: Single function execution with retry logic
- **Parent Tasks**: Spawn parallel children tasks, optional finalize task for coordination
- **Sequential Tasks**: Chain tasks where output of one becomes input of next

#### Task State Machine

- `ready` → `running` → `completed`/`failed`/`timed_out`
- Parent tasks: `waiting_for_children_tasks` → `waiting_for_finalize_task`
- Failed tasks can retry based on retry configuration

#### Storage Contract

- Abstract `Storage` interface for persistence
- Must support atomic transactions and concurrent access
- InMemoryStorage for testing, Drizzle implementations for production

### Key Design Principles

**Durability**: Tasks survive process crashes through expiration monitoring and state persistence

**Idempotency**: Tasks should be idempotent as they may execute multiple times on failure/retry

**Type Safety**: Full TypeScript support with input/output type inference across task chains

**Composability**: Tasks compose into complex workflows through parent-child relationships

**Scalability**: Configurable concurrency limits and batch processing for high-throughput scenarios

### Testing Strategy

Each package contains comprehensive tests in `tests/*.test.ts`:

- Core functionality in `durable-execution/tests/`
- Storage implementation validation via shared test utils
- oRPC integration testing with mock procedures
- Concurrent execution and failure scenario testing

Run package-specific tests with:

```bash
pnpm vitest run tests/executor.test.ts           # Core executor tests
pnpm vitest run tests/parent-task.test.ts        # Parent-child task tests
pnpm vitest run tests/concurrent-scenarios.test.ts # Concurrency tests
```

## Development Workflow

### Adding New Features

1. Implement in appropriate package (`durable-execution` for core features)
2. Add comprehensive tests covering success and failure scenarios
3. Update TypeDoc documentation if public API changes
4. Run full test suite: `pnpm test`
5. Ensure type safety: `pnpm type-check`

### Creating a New Storage Implementation

1. Implement `Storage` interface from `durable-execution/src/storage.ts`
2. Use `durable-execution-storage-test-utils` to validate implementation
3. Reference Drizzle implementations for patterns and indexing strategies

### Important Configuration Limits

- `maxConcurrentTaskExecutions`: default 1000
- `maxChildrenTasksPerParent`: default 1000
- `maxSerializedInputDataSize`/`maxSerializedOutputDataSize`: default 1MB each
- Background process timing via `backgroundProcessIntraBatchSleepMs` and `expireMs`

## Documentation

- Main documentation: <https://gpahal.github.io/durable-execution>
- Repository: <https://github.com/gpahal/durable-execution>
