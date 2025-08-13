# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Build

```bash
pnpm build          # Clean build directory and compile TypeScript with tsup
pnpm build-docs     # Generate documentation using TypeDoc
pnpm clean          # Remove build directory
```

### Testing

```bash
pnpm test           # Run all tests with Vitest
pnpm test-coverage  # Run tests with coverage reporting

# Run specific test files
pnpm vitest run tests/executor.test.ts
pnpm vitest run tests/executor-client.test.ts
pnpm vitest run tests/parent-task.test.ts
pnpm vitest run tests/concurrent-scenarios.test.ts
```

### Type Checking and Linting

```bash
pnpm type-check     # Type check with TypeScript and validate TypeDoc
pnpm lint           # Run ESLint on the codebase
pnpm lint-fix       # Run ESLint with automatic fixes
```

### Git Hooks

The project uses simple-git-hooks for pre-commit and pre-push hooks that run `pnpm pre-commit`.

## Architecture Overview

### Core Components

**DurableExecutor** (`src/executor.ts`)
The main class that orchestrates task execution. It manages:

- Task lifecycle (enqueue, execution, cancellation)
- Background processes for task picking and monitoring
- Storage interactions through the Storage interface
- Serialization of task inputs/outputs

**DurableExecutorClient** (`src/executor-client.ts`)
A lightweight client for enqueuing tasks without background processes:

- Allows task enqueuing from separate processes/services
- Shares the same storage as DurableExecutor
- Useful for distributed architectures where task submission and execution are separated

**Task System** (`src/task.ts`, `src/task-internal.ts`)

- Tasks can be simple functions or complex workflows with parent-child relationships
- Three task types: regular tasks, parent tasks (with children), and sequential tasks
- Tasks support retries, timeouts, and cancellation
- Parent tasks can spawn parallel children and use finalizeTask for coordination

**Storage Interface** (`src/storage.ts`, `src/in-memory-storage.ts`)

- Abstract Storage interface for persisting task execution state
- InMemoryStorage provides a simple implementation for testing
- Storage must support async transactions with isolation
- Production implementations available in separate packages (e.g., drizzle-based)

**Serialization** (`src/serializer.ts`)

- Handles serialization/deserialization of task inputs and outputs
- Default implementation uses superjson for rich type support
- Configurable through DurableExecutor options

**Error Handling** (`src/errors.ts`)

- Custom error types for different failure scenarios
- Errors are serialized and stored for debugging

### Task Execution Flow

1. Tasks are enqueued with status=ready
2. Background process picks ready tasks and marks them as running
3. Task run function executes with timeout and cancellation support
4. On completion:
   - Simple tasks → completed
   - Parent tasks → waiting_for_children_tasks → completed/failed
   - Tasks with finalizeTask → waiting_for_finalize_task → completed/failed
5. Failed tasks can be retried based on retry options
6. All task state transitions are persisted to storage

### Key Design Patterns

- **Idempotency**: Tasks should be idempotent as they may retry on failure
- **Resilience**: Tasks survive process crashes through expiration monitoring
- **Composability**: Tasks can be composed into trees through parent-child relationships
- **Type Safety**: Full TypeScript support with input/output type inference

## Testing Approach

Tests are located in `tests/` and use Vitest. Key test files:

- `executor.test.ts` - Core executor functionality
- `executor-client.test.ts` - DurableExecutorClient functionality
- `task.test.ts` - Task creation and execution
- `parent-task.test.ts` - Parent-child task relationships
- `concurrent-scenarios.test.ts` - Concurrency and race conditions
- `executor-crash.test.ts` - Process failure resilience

## Important Notes

- The library is designed for durable execution of tasks that survive process failures
- Tasks must be idempotent as they may be executed multiple times
- The storage implementation is critical for production use - InMemoryStorage is only for testing
- Maximum limits are enforced for serialized data sizes and number of children tasks
