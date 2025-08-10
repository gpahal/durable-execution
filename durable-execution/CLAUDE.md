# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Core Commands

- `pnpm build` - Build the package using tsup
- `pnpm test` - Run all tests with Vitest
- `pnpm test -t "test-name"` - Run a specific test by name
- `pnpm test-coverage` - Run tests with coverage reporting
- `pnpm type-check` - Type check with TypeScript and generate docs
- `pnpm lint` - Lint code with ESLint
- `pnpm lint-fix` - Fix linting issues automatically

### Documentation

- `pnpm build-docs` - Generate TypeDoc documentation

### Monorepo Commands (from root)

- `pnpm pre-commit` - Run type-check, lint, format check, and build (used by git hooks)
- `turbo build` - Build all packages in the monorepo
- `turbo test` - Test all packages in the monorepo

### Single Test Execution

- `pnpm test examples.test.ts` - Run a specific test file
- `pnpm test --reporter=verbose` - Run tests with verbose output

## Architecture Overview

### Core Components

**DurableExecutor** (`src/index.ts`)

- Main orchestrator class that manages task lifecycles and background processes
- Handles task registration, enqueueing, execution, retries, and cancellation
- Manages concurrency limits and backpressure via configurable parameters
- Coordinates with storage layer for persistence and cross-process coordination

**Task System** (`src/task.ts`, `src/task-internal.ts`)

- Tasks can be simple functions or complex parent tasks with parallel children
- Support for sequential task chains via `sequentialTasks()` method
- Parent tasks can spawn children and use finalize tasks to combine outputs
- Input validation via schema validation or custom validation functions
- Configurable retry policies, timeouts, and delay mechanisms

**Storage Abstraction** (`src/storage.ts`)

- Interface for persisting task executions with transaction support
- In-memory implementation provided for testing and simple use cases
- External implementations available (durable-execution-storage-drizzle)
- Support for optimistic locking and concurrent access patterns

**Resilience Mechanisms** (`src/cancel.ts`, `src/errors.ts`)

- Process failure recovery via task expiration and automatic retry
- Graceful cancellation with promise cancellation support
- Background processes for cleanup, expiration handling, and status updates
- Detailed error classification for proper retry logic

### Task Execution State Machine

Tasks progress through states: `ready` → `running` → `completed`/`failed`/`timed_out`/`cancelled`
Parent tasks have additional states: `waiting_for_children_tasks` → `waiting_for_finalize_task`

### Background Processes

The executor runs four concurrent background processes:

- Close finished task executions and update parent/children states
- Retry expired running tasks (process failure recovery)
- Cancel tasks marked for promise cancellation
- Process ready tasks respecting concurrency limits

## Testing Approach

### Test Structure

- Uses Vitest with `tests/**/*.test.ts` pattern
- Tests organized by functional areas (executor, tasks, cancellation, etc.)
- Comprehensive integration tests covering concurrent scenarios
- Process crash simulation tests for resilience validation

### Test Categories

- **Unit tests**: Individual component behavior
- **Integration tests**: Full workflow testing with in-memory storage
- **Concurrency tests**: Multi-task parallel execution scenarios
- **Failure tests**: Process crashes, timeouts, cancellation
- **Example tests**: Real-world usage patterns

### Key Test Files

- `executor.test.ts` - Core executor functionality
- `concurrent-scenarios.test.ts` - Complex parallel task scenarios
- `executor-crash.test.ts` - Process failure recovery
- `examples.test.ts` - Usage pattern examples

## Key Implementation Details

### Task Registration

- Tasks must be registered with unique IDs before enqueueing
- Task definitions are stored in `taskInternalsMap` for execution lookup
- Input validation happens at registration time via schemas or functions

### Concurrency Management

- `maxConcurrentTaskExecutions` controls active task limit
- `maxTasksPerBatch` controls batch processing size
- Background processes respect shutdown signals for graceful termination

### Storage Requirements

- Must support concurrent transactions for multi-executor deployments
- Requires indexes on execution_id, status+isClosed+expiresAt, and status+startAt
- Version-based optimistic locking prevents race conditions

### Error Handling

- Retryable vs non-retryable error classification
- Exponential backoff with jitter for retry delays
- Parent task failure propagates to cancel all children

This durable execution engine is designed for complex, long-running workflows that need to survive process failures and coordinate parallel execution across multiple nodes.
