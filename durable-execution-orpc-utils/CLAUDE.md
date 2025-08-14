# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Development

- `pnpm test` - Run all tests
- `pnpm test-coverage` - Run tests with coverage reporting
- `pnpm type-check` - Run TypeScript type checking and generate documentation
- `pnpm lint` - Run ESLint
- `pnpm lint-fix` - Run ESLint with auto-fix
- `pnpm build` - Build the project (cleans build directory first)
- `pnpm clean` - Remove build directory

### Testing

- Single test files are run with vitest, but there's only one test file: `tests/index.test.ts`
- The project uses vitest with v8 coverage reporting
- Tests require proper cleanup with `executor.shutdown()` in `afterEach` blocks
- Task IDs in tests must be alphanumeric with underscores only (no hyphens)

## Architecture

This is an oRPC utilities library for the durable-execution framework that enables separation of business logic from execution orchestration.

### Core Components

**Server (`src/server.ts`)**:

- `createTasksRouter()` - Creates oRPC router with `enqueueTask` and `getTaskExecution` procedures
- `convertProcedureClientToTask()` - Converts oRPC procedure clients into durable tasks for remote execution
- Error mapping from oRPC errors to DurableExecutionError types
- HTTP status code classification (retryable: 408, 429, 500-504; not found: 404)

**Client (`src/client.ts`)**:

- `createTaskClientHandles()` - Creates type-safe handles for tasks with `enqueue` and `getExecution` methods
- Complex TypeScript generics for full type inference from task definitions
- Handles client context propagation for authenticated requests

### Key Patterns

**Type System**:

- Heavy use of TypeScript conditional types and inference
- `convertProcedureClientToTask` uses `any` constraint for `ClientContext` to enable flexible type inference
- Task input/output types are inferred from oRPC procedure schemas

**Error Handling**:

- Three-tier error classification: retryable, internal, not found
- Automatic retry logic based on HTTP status codes
- DurableExecutionError wrapping with proper error metadata

**Testing Strategy**:

- Comprehensive edge case coverage including all HTTP status codes
- Mock-based testing for error scenarios using vi.spyOn
- Real execution testing with InMemoryTaskExecutionsStorage
- Type safety verification through test compilation

### Dependencies

This package is part of a monorepo and depends on:

- `durable-execution` (workspace dependency) - Core task execution framework
- `@orpc/*` packages - RPC framework for client/server communication
- `@gpahal/std` - Standard utilities library

Configuration files inherit from parent directory (`../durable-execution/`) for tsup and vitest configs.
