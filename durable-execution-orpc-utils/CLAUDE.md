# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

- **Build**: `pnpm build` (uses tsup with config from parent directory)
- **Clean**: `pnpm clean` (removes build directory)
- **Test**: `pnpm test` (runs vitest)
- **Test with coverage**: `pnpm test-coverage` (runs vitest with coverage)
- **Type checking**: `pnpm type-check` (tsc --noEmit + typedoc validation)
- **Lint**: `pnpm lint`
- **Lint fix**: `pnpm lint-fix`

## Architecture Overview

This is an oRPC utilities package for the durable-execution library that enables separating durable task execution into a dedicated server process. The package provides client-server communication via oRPC procedures.

### Core Components

- **Server module** (`src/server.ts`): Creates oRPC procedures for task enqueuing and execution querying
  - `createTasksRouter()`: Creates router with `enqueueTask` and `getTaskExecution` procedures
  - `convertClientProcedureToTask()`: Wraps client oRPC procedures as durable tasks

- **Client module** (`src/client.ts`): Type-safe client handles for interacting with server procedures
  - `TasksRouterClient<>`: Type for client procedures
  - `createTaskClientHandles()`: Creates type-safe task handles from task definitions

### Key Design Patterns

1. **Dual-Entry Points**: Package exports separate `/server` and `/client` entry points for server and client code respectively

2. **Type Safety**: Heavy use of TypeScript generics to ensure type safety between task definitions and client handles

3. **oRPC Integration**: Built on top of oRPC for type-safe RPC communication with full schema validation

4. **Error Mapping**: Converts DurableExecutionError types to appropriate oRPC error codes

### Dependencies

- **Peer Dependencies**: Requires oRPC packages (`@orpc/client`, `@orpc/contract`, `@orpc/server`) and `durable-execution`
- **Utilities**: Uses `@gpahal/std` for error handling utilities

### Testing

Tests use vitest with in-memory storage and validate both direct task execution and client procedure conversion scenarios.
