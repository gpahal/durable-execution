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

### Monorepo Commands (from root)

- `turbo build` - Build all packages in the monorepo
- `turbo test` - Test all packages in the monorepo

## Architecture Overview

This package provides oRPC utilities for durable-execution, enabling separation of a long-running durable executor server from client web applications (e.g., serverless Next.js apps).

### Package Structure

**Server Module** (`src/server.ts`)

- `createTasksRouter()` - Creates oRPC router with `enqueueTask` and `getTaskExecution` procedures
- `convertClientProcedureToTask()` - Wraps client oRPC procedures as durable tasks
- Error handling converts between DurableExecutionError and ORPCError types
- Supports type-safe task execution with proper input/output schemas

**Client Module** (`src/client.ts`)

- `TasksRouterClient` - Type-safe client interface for task procedures
- `createTaskClientHandles()` - Creates task handles for type-safe enqueueing and execution fetching
- `TaskClientHandle` - Provides `enqueue()` and `getExecution()` methods per task

### Key Architecture Patterns

#### Client-Server Task Flow

1. Web app calls `tasksRouterClient.enqueueTask()` → Durable executor server
2. Server executes task (may call back to web app via `convertClientProcedureToTask`)
3. Web app polls `tasksRouterClient.getTaskExecution()` for results

#### Type Safety

- Task definitions shared between server and client ensure type safety
- `AnyTasks` record type constrains available tasks on client
- Input/output types inferred from durable-execution Task definitions

#### Error Mapping

- Server maps DurableExecutionError → ORPCError (NOT_FOUND, BAD_REQUEST, INTERNAL_SERVER_ERROR)
- Client procedures map ORPCError → DurableExecutionError for task execution

## Dependencies

### Peer Dependencies

- `@orpc/client`, `@orpc/contract`, `@orpc/server` - oRPC framework for type-safe RPC
- `durable-execution` - Core durable execution engine (workspace package)

### Key Exports

#### Server exports

- `createTasksRouter` - Main server router factory
- `convertClientProcedureToTask` - Client procedure wrapper utility
- `AnyTasks` - Type constraint for task records

#### Client exports

- `TasksRouterClient` - Client interface type
- `createTaskClientHandles` - Task handle factory
- `TaskClientHandle` - Individual task client interface

## Testing Approach

Tests use in-memory storage with DurableExecutor and createRouterClient for full integration testing. Key test scenarios:

- Basic task enqueueing and execution retrieval
- Error handling for invalid task/execution IDs
- Client procedure conversion with various error types (NOT_FOUND, INTERNAL_SERVER_ERROR, generic errors)
- Type safety verification across client-server boundaries

Test setup includes background process polling with short intervals for fast test execution.
