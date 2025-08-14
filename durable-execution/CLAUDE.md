# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Essential Commands

```bash
# Build the library
npm run build

# Run tests
npm test                    # Run all tests
npm run test-coverage      # Run tests with coverage report

# Type checking and linting
npm run type-check         # TypeScript type checking + docs validation
npm run lint              # Run ESLint
npm run lint-fix          # Auto-fix linting issues

# Documentation
npm run build-docs        # Generate TypeDoc documentation

# Clean build artifacts
npm run clean
```

### Testing Specific Files

```bash
# Run a specific test file
npx vitest run tests/executor.test.ts

# Run tests in watch mode
npx vitest

# Run tests matching a pattern
npx vitest run -t "should handle timeout"
```

## Architecture Overview

### Core Components

**DurableExecutor** (`src/executor.ts`): Central orchestrator that manages task execution, providing enqueueing, background processing, state management, and failure recovery.

**Task System** (`src/task.ts`, `src/task-internal.ts`):

- Simple tasks: Single function execution
- Parent tasks: Spawn parallel child tasks
- Sequential tasks: Chain dependent tasks
- Finalize tasks: Post-processing after parent+children complete

**Storage Layer** (`src/storage.ts`, `src/in-memory-storage.ts`): Abstract storage interface requiring ACID transactions. InMemoryStorage for testing, external packages provide production storage (Drizzle ORM).

**Error System** (`src/errors.ts`): Hierarchical error system with `DurableExecutionError` base class. Errors explicitly control retry behavior via `retryable()` and `nonRetryable()` factory methods.

### Task Definition Pattern

Tasks use a fluent API with optional schema validation:

```typescript_
const task = executor
  .inputSchema(z.object({ /* schema */ }))  // Optional validation
  .task({
    id: 'uniqueId',
    timeoutMs: 30000,
    retryOptions: { maxAttempts: 3 },
    run: async (ctx, input) => { /* logic */ }
  })
```

### Execution State Machine

Tasks progress through states: `ready` → `running` → `completed`/`failed`/`timed_out`. Parent tasks add `waiting_for_children` and `waiting_for_finalize` states. All terminal states transition through `closing` → `closed` for cleanup.

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
