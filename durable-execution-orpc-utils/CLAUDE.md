# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

- `pnpm build` - Build the package using tsup (ESM output with TypeScript declarations)
- `pnpm test` - Run Vitest tests
- `pnpm test-coverage` - Run tests with coverage reporting
- `pnpm type-check` - Run TypeScript type checking
- `pnpm lint` - Lint code with ESLint | `pnpm lint-fix` - Auto-fix linting issues
- `pnpm clean` - Remove build artifacts

## Package Overview

This package provides oRPC utilities for the `durable-execution` library, enabling separation of task execution state management from the web application. The key pattern is:

- **Durable Executor Server**: Long-running process that manages task state and orchestration
- **Web Application**: Stateless/serverless handlers that execute the actual task logic
- **oRPC Bridge**: Type-safe communication layer between the two processes

## Architecture

### Core Components

- **`createDurableTasksRouter()`** (src/server.ts:187) - Creates server-side oRPC procedures for task management
  - `enqueueTask` - Accepts task enqueue requests and returns execution IDs
  - `getTaskExecution` - Retrieves task execution status and results

- **`createDurableTaskClientHandles()`** (src/client.ts:80) - Creates type-safe client handles for task operations
  - Provides `enqueue()` and `getExecution()` methods for each registered task

- **`convertClientProcedureToDurableTask()`** (src/server.ts:241) - Wraps oRPC procedure calls as durable tasks
  - Converts oRPC errors to appropriate `DurableExecutionError` types
  - Maps HTTP status codes: `NOT_FOUND` → `DurableExecutionNotFoundError`, `INTERNAL_SERVER_ERROR` → internal error

### Error Handling

The package maps oRPC errors to durable execution error types:

- `NOT_FOUND` → `DurableExecutionNotFoundError`
- `INTERNAL_SERVER_ERROR` → `DurableExecutionError` (internal=true)
- Other errors → `DurableExecutionError` (internal=false)

## Key Usage Patterns

1. **Server Setup**: Create executor with storage, register tasks, expose oRPC procedures
2. **Client Integration**: Create handles for type-safe task enqueueing and status checking
3. **Task Wrapping**: Use `convertClientProcedureToDurableTask()` to make web app procedures callable as durable tasks

## Dependencies

- **Peer Dependencies**: `@orpc/client`, `@orpc/contract`, `@orpc/server`, `durable-execution`
- **Runtime Dependency**: `@gpahal/std` for error utilities

## Testing

Tests cover:

- Basic task enqueueing and execution via oRPC procedures
- Error handling for invalid task IDs and execution IDs
- All error scenarios for `convertClientProcedureToDurableTask()` wrapper
- Integration with in-memory storage for fast test execution
