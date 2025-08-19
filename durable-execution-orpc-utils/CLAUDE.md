# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Essential Commands

```bash
# Build the library
pnpm build

# Run tests
pnpm test              # Run all tests
pnpm test-coverage     # Run tests with coverage report

# Type checking and linting
pnpm type-check        # TypeScript type checking + typedoc validation
pnpm lint              # Run ESLint
pnpm lint-fix          # Auto-fix linting issues

# Clean build artifacts
pnpm clean
```

### Testing Specific Files

```bash
# Run a specific test file
pnpm test tests/index.test.ts

# Run tests in watch mode
pnpm test -w

# Run tests matching a pattern
pnpm test -t 'should handle remote execution'
```

## Architecture Overview

This package provides **oRPC integration utilities** for the durable-execution framework, enabling separation of task orchestration from business logic through RPC-style communication.

### Core Components

**Main Module** (`src/index.ts`):

- `createTasksRouter()`: Creates an oRPC router exposing task operations
- `convertProcedureClientToTask()`: Converts remote oRPC procedures into durable tasks
- Error mapping between oRPC and durable-execution domains

### Architecture Patterns

**Separation of Concerns**:

- **Executor Server**: Long-running process managing task orchestration, retries, and persistence
- **Application Server**: Business logic implementation exposed via oRPC procedures
- **Client Applications**: Frontend or backend services that enqueue and monitor tasks

**Remote Task Execution Flow**:

1. Client enqueues task via oRPC client handle
2. Executor server receives request and persists task
3. Executor optionally calls back to application server for business logic
4. Application server executes logic and returns result
5. Executor handles retries, timeouts, and state management
6. Client queries execution status and retrieves results

**Sleeping Task Flow**:

1. Parent task creates sleeping task child with unique entity ID
2. Sleeping task enters `running` state and waits
3. External webhook/event handler calls `wakeupSleepingTaskExecution` via oRPC
4. Sleeping task completes with provided output
5. Parent task finalize function processes the result

### Router API

The `createTasksRouter` function creates three oRPC procedures:

- `enqueueTask`: Queue task for execution (returns execution ID)
- `getTaskExecution`: Retrieve execution status and output
- `wakeupSleepingTaskExecution`: Wake up sleeping tasks with output (for webhook/event handlers)

Each procedure includes:

- Type-safe input/output validation
- Automatic error mapping (404, 408, 429, 500-504 → appropriate DurableExecutionError)
- Task existence validation

### Key Design Decisions

**Type Safety**: Full TypeScript type inference from server tasks to client handles, ensuring compile-time safety across RPC boundaries.

**Error Mapping**: Automatic conversion between HTTP status codes and durable execution errors:

- 404 → `DurableExecutionNotFoundError`
- 408, 429, 500-504 → Retryable errors
- Other 5xx → Non-retryable internal errors

**Procedure Conversion**: The `convertProcedureClientToTask()` function enables any oRPC procedure to become a durable task, preserving input/output types.

### Implementation Details

**Router Creation**:

```ts
createTasksRouter(osBuilder, executor, tasks)
```

Creates typed procedures for task operations with proper error handling and validation.

**Remote Procedure Tasks**:

```ts
convertProcedureClientToTask(executor, taskOptions, procedureClient)
```

Wraps remote procedures with durable execution semantics, handling RPC errors and retry logic.

### Testing Approach

Tests (`tests/index.test.ts`) verify:

- End-to-end task execution via oRPC
- Error handling for invalid task/execution IDs
- Type safety across client-server boundary
- Remote procedure conversion with proper error mapping
- Sleeping task wake-up operations
- DurableExecutionError propagation and classification

Test setup uses:

- `InMemoryTaskExecutionsStorage` for fast, deterministic tests
- `createRouterClient` for direct router testing without HTTP layer
- Background process intervals of 50ms for rapid test execution

### Important Conventions

- All public APIs exported through `src/index.ts`
- Maintain type inference chains for developer experience
- Follow oRPC conventions for middleware and error handling
- Test with mock oRPC servers for isolation
- Use `type<T>()` helper for runtime type definitions
- Error messages should be descriptive and actionable
