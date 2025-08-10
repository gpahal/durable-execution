# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Monorepo-Level Commands (from root)

- `pnpm build` - Build all packages using Turbo
- `pnpm test` - Run tests across all packages
- `pnpm test-coverage` - Run tests with coverage reporting
- `pnpm type-check` - Type check all packages
- `pnpm lint` / `pnpm lint-fix` - Lint all packages, optionally fixing issues
- `pnpm fmt` / `pnpm fmt-check` - Format code or check formatting
- `pnpm pre-commit` - Run type-check, lint, fmt-check, and build (used by git hooks)
- `pnpm clean` - Clean build artifacts across packages
- `pnpm build-docs` - Generate TypeDoc documentation for core package

### Package-Specific Commands (within each package directory)

- `pnpm build` - Build the specific package
- `pnpm test` - Run tests for the package
- `pnpm test -t "test-name"` - Run a specific test by name
- `pnpm type-check` - Type check the package
- `pnpm lint` / `pnpm lint-fix` - Lint the package

## Architecture Overview

This is a pnpm workspace monorepo containing three interconnected packages that provide a complete durable execution framework:

### Core Package: `durable-execution`

**Purpose**: The main durable execution engine for running tasks durably and resiliently.

**Key Components**:

- `DurableExecutor` - Main orchestrator managing task lifecycles and background processes
- Task system supporting simple functions, complex parent tasks with parallel children, sequential workflows
- Storage abstraction with in-memory implementation for testing
- Resilience mechanisms including cancellation, retries, and process failure recovery
- Background processes for cleanup, expiration handling, and concurrency management

**Key Files**:

- `src/index.ts` - DurableExecutor class and main exports
- `src/task.ts` - Task definitions and APIs
- `src/storage.ts` - Storage interface and in-memory implementation
- `src/cancel.ts` - Cancellation utilities
- `src/errors.ts` - Error classification and handling

### Storage Package: `durable-execution-storage-drizzle`

**Purpose**: Production-ready Drizzle ORM storage implementations supporting PostgreSQL and SQLite.

**Key Components**:

- `createPgStorage()` - PostgreSQL storage with proper indexing and concurrency
- `createSQLiteStorage()` - SQLite storage with transaction mutex handling
- Schema definitions optimized for task execution queries
- Data transformation utilities between storage and database formats

**Database Schema**: Includes execution lifecycle fields, parent-child relationships, retry configuration, and performance indexes.

### RPC Package: `durable-execution-orpc-utils`

**Purpose**: oRPC utilities enabling client-server separation where a long-running durable executor server manages tasks for serverless client applications.

**Key Components**:

- Server: `createTasksRouter()` creates oRPC router with enqueue and execution retrieval procedures
- Client: `createTaskClientHandles()` provides type-safe task handles for enqueueing and polling
- `convertClientProcedureToTask()` wraps client RPC procedures as durable tasks

**Architecture Pattern**: Web app → Durable executor server (executes tasks, may call back to web app) → Web app polls for results.

## Key Concepts and Workflows

### Task Execution Lifecycle

Tasks progress through states: `ready` → `running` → `completed`/`failed`/`timed_out`/`cancelled`

Parent tasks have additional states: `waiting_for_children_tasks` → `waiting_for_finalize_task`

### Task Types

1. **Simple Tasks**: Basic functions with input/output
2. **Parent Tasks**: Spawn parallel children, optional finalize task for combining outputs
3. **Sequential Tasks**: Chain tasks where output of one becomes input of next
4. **Recursive Tasks**: Self-referencing tasks with proper type annotations

### Background Processes

The executor runs four concurrent processes:

- Close finished executions and update parent/child relationships
- Retry expired running tasks (process failure recovery)
- Cancel tasks marked for promise cancellation
- Process ready tasks respecting concurrency limits

### Resilience Features

- **Process Failure Recovery**: Tasks marked as running beyond timeout are automatically retried
- **Cancellation**: Graceful cancellation with promise cancellation support
- **Retries**: Configurable exponential backoff with jitter
- **Concurrency Control**: Configurable limits and backpressure management

## Testing Approach

### Test Organization

- Uses Vitest with `tests/**/*.test.ts` pattern across all packages
- Tests organized by functional areas (executor, tasks, cancellation, storage, etc.)
- Integration tests using in-memory storage or temporary databases
- Process crash simulation for resilience validation

### Key Test Categories

- **Unit tests**: Individual component behavior
- **Integration tests**: Full workflow testing
- **Concurrency tests**: Multi-task parallel execution scenarios
- **Failure tests**: Process crashes, timeouts, cancellation handling
- **Storage tests**: Database-specific implementations with real DB instances

## Important Implementation Guidelines

### Task Registration and Input Validation

- Tasks must be registered with unique IDs before enqueueing
- Use `inputSchema()` with Standard Schema (e.g., zod) or `validateInput()` for custom validation
- Task logic should be idempotent as tasks may execute multiple times

### Concurrency and Performance

- Configure `maxConcurrentTaskExecutions` and `maxTasksPerBatch` appropriately
- Storage implementations must support concurrent transactions for multi-executor deployments
- Use optimistic locking patterns to prevent race conditions

### Error Handling and Cancellation

- Classify errors as retryable vs non-retryable
- Respect `cancelSignal` and `shutdownSignal` in long-running operations
- Parent task failures propagate to cancel all children

### Storage Requirements

Storage implementations must:

- Support concurrent transactions across multiple executor instances
- Provide indexes on execution_id, status+isClosed+expiresAt, and status+startAt
- Handle hierarchical task relationships (root/parent references)
- Support atomic operations for state transitions

This framework is designed for complex, long-running workflows that survive process failures and coordinate parallel execution across distributed deployments.
