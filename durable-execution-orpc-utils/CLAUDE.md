# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

```bash
# Installation (from monorepo root)
pnpm install   # Requires pnpm@10, Node >= 20

# Build and Development
pnpm build          # Build package with tsup
pnpm clean          # Remove build artifacts
pnpm type-check     # TypeScript type checking + typedoc validation

# Testing
pnpm test           # Run all tests
pnpm test-coverage  # Run tests with coverage report

# Code Quality
pnpm lint           # Run ESLint
pnpm lint-fix       # Auto-fix linting issues
```

## Architecture Overview

This package provides oRPC utilities for the durable-execution library, enabling separation of task execution into a dedicated server process with type-safe client-server communication.

### Package Structure

#### Entry Points

- `/server` - Server-side utilities for creating oRPC procedures
- `/client` - Client-side utilities for interacting with server procedures

#### Core Modules

- `src/server.ts` - Server utilities
  - `createTasksRouter()` - Creates oRPC router with task procedures
  - `convertClientProcedureToTask()` - Wraps client procedures as durable tasks
- `src/client.ts` - Client utilities
  - `TasksRouterClient<>` - Type definition for client procedures
  - `createTaskClientHandles()` - Creates type-safe task handles

### Key Patterns

**Task Router Creation**
The server exposes tasks via oRPC procedures that handle:

- Task enqueueing with `enqueueTask` procedure
- Execution state queries with `getTaskExecution` procedure
- Error mapping from DurableExecutionError to oRPC error codes

**Client Procedure Conversion**
Enables web apps to expose business logic as oRPC procedures that the durable executor server can call:

1. Web app defines and exposes oRPC procedures
2. Server wraps these as durable tasks using `convertClientProcedureToTask()`
3. Task execution triggers RPC calls back to the web app
4. Execution state managed by durable executor server

**Type-Safe Client Handles**
`createTaskClientHandles()` generates strongly-typed client handles from task definitions, providing:

- `enqueue()` method with proper input typing
- `getExecution()` method returning typed execution results

### Testing Approach

Tests validate:

- Direct task execution through router procedures
- Client procedure conversion and execution
- Error handling and propagation
- Type safety across client-server boundaries

Run tests with `pnpm test` from package directory or `pnpm vitest run tests/index.test.ts` for specific test file.

### Usage Flow

1. **Server Setup**: Create executor, register tasks, build router with `createTasksRouter()`
2. **Client Setup**: Create oRPC client for server router
3. **Task Execution**: Use client handles to enqueue tasks and query execution state
4. **Remote Tasks**: Optionally wrap client procedures as tasks for remote execution
