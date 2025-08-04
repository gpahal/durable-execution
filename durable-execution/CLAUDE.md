# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Build, Test, and Development

```bash
# Build the project
pnpm build

# Run tests
pnpm test
pnpm test-watch     # Watch mode
pnpm test-coverage  # With coverage

# Type checking and linting
pnpm type-check
pnpm lint
pnpm lint-fix

# Code formatting
pnpm fmt        # Format code
pnpm fmt-check  # Check formatting

# Pre-commit validation (runs all checks)
pnpm pre-commit

# Generate documentation
pnpm build-docs
```

### Testing Single Files

```bash
# Run specific test file
pnpm test tests/examples.test.ts

# Run tests matching pattern
pnpm test -- --grep "parent task"
```

## Architecture Overview

This is a **durable task execution engine** for TypeScript that provides resilient, transactional task execution with automatic retry, cancellation, and parent-child task relationships.

### Core Architecture Pattern

The system follows a **state machine pattern** for task execution:

1. Tasks transition through well-defined states: `ready → running → [completed|failed|timed_out]`
2. Parent tasks can spawn children and wait for completion
3. All state transitions are transactional via the storage layer
4. Background processes handle recovery, retries, and cleanup

### Key Components

1. **DurableExecutor** (`src/index.ts`): Main orchestrator managing task lifecycle and background processes
   - Enqueues and executes tasks
   - Runs background processes for recovery and cleanup
   - Manages executor lifecycle (start/shutdown)

2. **Task System** (`src/task.ts`): Task definitions and execution logic
   - Simple tasks: `task()`, `schemaTask()`
   - Parent tasks: `parentTask()`, `parentSchemaTask()`
   - Completion handlers: `onRunAndChildrenComplete`

3. **Storage Interface** (`src/storage.ts`): Transactional storage abstraction
   - Must implement `DurableStorage` interface
   - All operations are transactional
   - Reference implementation in `tests/in-memory-storage.ts`

4. **Cancellation** (`src/cancel.ts`): Custom cancellation signal system
   - Promise-based cancellation propagation
   - Graceful shutdown support

### Important Design Considerations

1. **Idempotency Required**: Tasks must be idempotent as they may execute multiple times on failure
2. **Transactional Storage**: Storage implementation must support transactions for consistency
3. **Process Recovery**: Uses expiration timestamps to detect and recover from crashed processes
4. **Distributed Execution**: Multiple executors can share the same storage backend

### Testing Approach

- **Framework**: Vitest with Node.js environment
- **Test Categories**: Unit tests, integration tests, crash recovery tests, parent-child tests
- **Test Infrastructure**: In-memory storage implementation for testing
- **Coverage**: Comprehensive test suite in `tests/` directory

### Common Development Patterns

When implementing tasks:

- Use `schemaTask` variants for input validation with Standard Schema v1
- Return children array from `runParent` for parallel execution
- Use `onRunAndChildrenComplete` to combine parent and children outputs
- Handle cancellation via `ctx.cancelSignal` in long-running tasks

When working with storage:

- All operations must be within transactions
- Use `getExecutionForUpdate` for pessimistic locking
- Convert between storage and runtime objects via provided utilities
