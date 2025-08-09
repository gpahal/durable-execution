# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Drizzle ORM storage implementation for the durable-execution library. It provides persistent storage backends for durable task executions using PostgreSQL and SQLite databases.

## Development Commands

### Build and Development

- `pnpm build` - Build the TypeScript source code using tsup
- `pnpm clean` - Remove build artifacts

### Testing

- `pnpm test` - Run all tests using Vitest
- `pnpm test-coverage` - Run tests with coverage report
- Tests are located in `tests/index.test.ts` and include comprehensive integration tests for all database backends

### Code Quality

- `pnpm type-check` - Run TypeScript type checking without emitting files
- `pnpm lint` - Run ESLint on all files
- `pnpm lint-fix` - Run ESLint with automatic fixes

## Architecture

### Core Components

**Database Adapters** (`src/pg.ts`, `src/sqlite.ts`):

- Each file provides database-specific table schemas and storage implementations
- Exports table creation functions: `createDurableTaskExecutionsPgTable()`, `createDurableTaskExecutionsSQLiteTable()`
- Exports storage factory functions: `createPgDurableStorage()`, `createSQLiteDurableStorage()`

**Common Layer** (`src/common.ts`):

- Contains shared type definitions and data transformation functions
- `DurableTaskExecutionDbValue` - Database row structure
- `storageObjectToInsertValue()` - Converts storage objects to DB insert format
- `selectValueToStorageObject()` - Converts DB rows back to storage objects
- `storageUpdateToUpdateValue()` - Handles partial updates

**Storage Implementation Pattern**:
Each database adapter follows the same pattern:

1. Table schema definition with proper indexes for performance
2. Storage class implementing the `DurableStorage` interface
3. Transaction class implementing `DurableStorageTx` interface
4. Query building functions for different where conditions

### Database Schema

All implementations share the same logical schema with database-specific type mappings:

- Primary key with auto-increment
- Task hierarchy fields (root, parent relationships)
- Execution state and metadata
- JSON fields for complex data (retry options, children tasks, errors)
- Optimized indexes on frequently queried columns

### Testing Strategy

The test suite (`tests/index.test.ts`) runs comprehensive integration tests against all three database backends:

- **SQLite**: Uses file-based database with schema migration via drizzle-kit
- **PostgreSQL**: Uses PGlite (in-memory Postgres) for fast testing

Tests cover:

- Complex parent-child task hierarchies
- Sequential task execution
- Concurrent task execution (100 tasks)
- Retry mechanisms
- Error handling and failure scenarios

## Dependencies

- **Peer Dependencies**: Requires `drizzle-orm` and `durable-execution` to be installed
- **Development Dependencies**: Various database drivers for testing (`@libsql/client`, `@electric-sql/pglite`)

## Monorepo Context

This package is part of a larger durable-execution monorepo. Configuration files (`tsup.config.ts`, `vitest.config.ts`) reference shared configs from the parent `../durable-execution/` directory.
