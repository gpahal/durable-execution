# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Monorepo Commands (run from root)

```bash
# Install dependencies
pnpm install

# Build all packages
pnpm build
pnpm build-docs           # Build TypeDoc documentation for main package

# Testing
pnpm test                 # Run all tests across packages
pnpm test-coverage        # Run tests with coverage
turbo test               # Alternative via Turbo

# Code quality
pnpm type-check          # TypeScript checking across packages
pnpm lint                # ESLint all packages
pnpm lint-fix            # Auto-fix linting issues
pnpm fmt                 # Format code with Prettier
pnpm fmt-check           # Check formatting

# Development workflow
pnpm pre-commit          # Full pre-commit checks (type-check, lint, fmt-check, build)

# Publishing
pnpm cs                  # Create changeset and version
pnpm cs-publish          # Publish packages with git tagging
```

### Package-specific Commands

Navigate to individual package directories to run package-specific commands:

```bash
# Core package (durable-execution/)
cd durable-execution
pnpm test                # Run core tests
pnpm build-docs          # Generate TypeDoc docs
npx vitest run tests/executor.test.ts  # Run specific test

# Storage implementations
cd durable-execution-storage-drizzle
pnpm test                # Test all database implementations

# oRPC utilities
cd durable-execution-orpc-utils
pnpm test                # Test remote execution utilities

# Test utilities
cd durable-execution-storage-test-utils
pnpm test                # Test storage validation suite
```

## Architecture Overview

This is a **TypeScript monorepo** for a durable task execution framework that provides resilient, idempotent task orchestration with failure recovery.

### Package Structure

**Core Package**: `durable-execution/`

- Main framework for durable task execution
- Provides `DurableExecutor`, task definitions, storage interfaces
- Comprehensive test suite and TypeDoc documentation

**Storage Implementations**: `durable-execution-storage-drizzle/`

- Database storage using Drizzle ORM
- Supports PostgreSQL, MySQL, SQLite
- Production-ready with transaction support

**Remote Execution**: `durable-execution-orpc-utils/`

- oRPC integration for distributed task execution
- Type-safe client/server communication
- Separates business logic from execution orchestration

**Testing Infrastructure**: `durable-execution-storage-test-utils/`

- Comprehensive test suite for storage implementations
- Test utilities and factories for integration testing

### Core Design Principles

**Durable Execution Model**:

- Tasks are idempotent and can be safely retried
- Process failure recovery through task expiration and re-queueing
- Hierarchical task relationships (parent-child, sequential, finalize)
- ACID storage requirements for state consistency

**Task Types**:

- **Simple tasks**: Single function execution
- **Parent tasks**: Spawn parallel child tasks with optional finalize step
- **Sequential tasks**: Chained dependent execution
- **Finalize tasks**: Post-processing after parent+children complete

**State Management**:

- Task execution state machine: `ready` → `running` → `completed`/`failed`/`timed_out`
- Parent tasks add `waiting_for_children` and `waiting_for_finalize` states
- Comprehensive error handling with retryable/non-retryable classification

### Key Components

**DurableExecutor** (`durable-execution/src/executor.ts`):
Central orchestrator managing task enqueueing, background processing, and failure recovery.

**Task System** (`durable-execution/src/task.ts`, `task-internal.ts`):
Fluent API for task definition with optional schema validation using Zod.

**Storage Layer** (`durable-execution/src/storage.ts`):
Abstract interface requiring ACID transactions. Production implementations in storage packages.

**Error Hierarchy** (`durable-execution/src/errors.ts`):
Structured error system with explicit retry control via `retryable()` and `nonRetryable()` factories.

## Development Workflow

### Task Definition Pattern

```typescript
const task = executor
  .inputSchema(z.object({ /* validation schema */ }))  // Optional
  .task({
    id: 'uniqueTaskId',
    timeoutMs: 30000,
    retryOptions: { maxAttempts: 3 },
    run: async (ctx, input) => {
      // Idempotent task logic
      return result
    }
  })
```

### Testing Approach

- **Integration focus**: Test real scenarios with multiple executor instances
- **Storage testing**: Use comprehensive test suite from `durable-execution-storage-test-utils`
- **Concurrency validation**: Test race conditions and failure recovery
- **Mock-free when possible**: Prefer real implementations for reliability

### Key Test Files by Scenario

- `examples.test.ts`: End-to-end patterns from documentation
- `executor-crash.test.ts`: Process failure recovery scenarios
- `concurrent-scenarios.test.ts`: Race conditions and parallelism
- `parent-task.test.ts`: Parent-child task orchestration

## Monorepo Configuration

### Build System

- **pnpm workspaces**: Package management and dependency resolution
- **Turbo**: Orchestrates builds, tests, and caching across packages
- **TypeScript**: Strict mode with project references for fast builds
- **tsup**: Fast bundling for package builds

### Code Quality

- **ESLint**: `@gpahal/eslint-config` for consistent styling
- **Prettier**: Automated formatting with import sorting
- **Git hooks**: Pre-commit validation with `simple-git-hooks`
- **Changesets**: Semantic versioning and coordinated releases

### Package Dependencies

Packages use pnpm workspace protocol (`workspace:*`) for internal dependencies and catalog references for external dependencies to ensure version consistency.

## Important Conventions

- **API Exports**: All public APIs exported through package `index.ts`
- **Type Safety**: Full TypeScript with generic type inference across task chains
- **Error Types**: Use existing error classes from `durable-execution/src/errors.ts`
- **Backward Compatibility**: Maintain storage interface compatibility across versions
- **Test Coverage**: Thoroughly test concurrency scenarios and edge cases
- **Documentation**: Maintain comprehensive JSDoc for public APIs

## Common Operations

### Adding New Task Types

1. Extend task definition interfaces in `durable-execution/src/task.ts`
2. Update executor implementation in `durable-execution/src/executor.ts`
3. Add comprehensive tests covering concurrency and failure scenarios
4. Update TypeDoc documentation and examples

### Storage Implementation

1. Implement `TaskExecutionsStorage` interface with ACID guarantees
2. Use test suite from `durable-execution-storage-test-utils`
3. Handle all storage operations atomically
4. Support concurrent access patterns

### Remote Execution Setup

1. Use `durable-execution-orpc-utils` for client/server separation
2. Define tasks on server with `createTasksRouter()`
3. Create type-safe client handles with `createTaskClientHandles()`
4. Handle network failures and retries appropriately
