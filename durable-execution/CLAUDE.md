# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Root Level (Turborepo orchestration)

- `pnpm build` - Build all packages in dependency order using Turbo
- `pnpm test` - Run tests across all packages
- `pnpm test-coverage` - Run tests with coverage reporting
- `pnpm type-check` - Type check all packages
- `pnpm lint` - Lint all packages | `pnpm lint-fix` - Auto-fix linting issues
- `pnpm fmt` - Format code | `pnpm fmt-check` - Check formatting
- `pnpm clean` - Clean build artifacts
- `pnpm build-docs` - Build TypeDoc documentation for core package
- `pnpm pre-commit` - Run type-check, lint, format-check, and build (used in git hooks)

### Package Level Commands

Each package (`durable-execution`, `durable-execution-storage-drizzle`, `durable-execution-orpc-utils`) supports:

- `pnpm build` - Build using tsup
- `pnpm test` - Run Vitest tests
- `pnpm test-coverage` - Run tests with coverage
- `pnpm type-check` - TypeScript type checking
- `pnpm lint` / `pnpm lint-fix` - ESLint

## Architecture Overview

### Monorepo Structure

This is a pnpm workspace with three main packages:

- **`durable-execution`** - Core durable task execution engine
- **`durable-execution-storage-drizzle`** - Drizzle ORM storage implementations (PostgreSQL, MySQL, SQLite)
- **`durable-execution-orpc-utils`** - oRPC integration utilities for HTTP APIs

### Core Package Architecture (`durable-execution`)

**Key Components:**

- `DurableExecutor` (src/index.ts) - Main orchestrator managing task lifecycle and background processing
- Task System (src/task.ts, src/task-internal.ts) - Type-safe task definitions with parent-child relationships
- Storage Interface (src/storage.ts) - Abstract persistence layer with in-memory implementation
- Serialization (src/serializer.ts) - Pluggable serialization (defaults to SuperJSON)
- Error System (src/errors.ts) - Custom error hierarchy for different failure modes
- Cancellation (src/cancel.ts) - Promise cancellation with timeout and shutdown signals

**Execution Model:**

- State machine: Ready → Running → Completed/Failed/TimedOut
- Parent tasks spawn children that execute in parallel
- Sequential task chains where output feeds next input
- Optional finalize tasks for cleanup/aggregation after children complete
- Exponential backoff retry logic with configurable options

**Key Patterns:**

- All tasks are idempotent and resilient to process failures
- Storage operations use transactions for consistency
- Background processes handle automatic task lifecycle management
- Standard Schema support for input validation (Zod, etc.)
- Task IDs must be unique within an executor instance

### Storage Package (`durable-execution-storage-drizzle`)

Provides persistent storage implementations using Drizzle ORM:

- PostgreSQL adapter (src/pg.ts) with `createPgDurableStorage()`
- MySQL adapter (src/mysql.ts) with `createMySQLDurableStorage()`
- SQLite adapter (src/sqlite.ts) with `createSQLiteDurableStorage()`

Each adapter includes optimized table schemas with proper indexes for performance on frequently queried columns like `(status, isClosed, expiresAt)` and `(status, startAt)`.

### oRPC Utils Package (`durable-execution-orpc-utils`)

Bridges durable execution with oRPC for type-safe HTTP APIs:

- `createDurableTaskORPCRouter()` - Server-side procedures for task management
- `createDurableTaskORPCHandles()` - Type-safe client handles
- `procedureClientTask()` - Wraps oRPC calls as durable tasks

## Build System and Tooling

- **Turborepo** - Task orchestration with dependency-aware caching
- **TypeScript** - Strict typing with dual tsconfig setup (dev + build)
- **tsup** - Fast bundling with ESM output and TypeScript declarations
- **Vitest** - Testing with v8 coverage provider
- **ESLint** - Uses `@gpahal/eslint-config/base` configuration
- **Prettier** - Code formatting with import sorting via `@ianvs/prettier-plugin-sort-imports`
- **pnpm** - Package manager with workspace and catalog support
- **Changesets** - Release management and versioning

## Important Development Guidelines

- Multiple executors can share storage for load distribution
- Process failures are handled via task expiration and retry mechanisms
- Background processing runs continuously until executor shutdown
- Register all tasks before enqueueing them
- Each package has individual CLAUDE.md files with package-specific details
- Pre-commit hooks ensure code quality (type-check, lint, format, build)
- Use workspace references (`workspace:*`) for internal package dependencies

## Testing Strategy

- Unit tests in each package's `tests/` directory
- Integration tests with real database backends (Testcontainers for MySQL, PGlite for PostgreSQL)
- Coverage reporting available via `pnpm test-coverage`
- Tests cover complex scenarios like parent-child hierarchies, concurrent execution, and failure modes
