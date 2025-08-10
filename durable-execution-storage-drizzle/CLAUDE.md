# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Core Commands

- `pnpm build` - Build the package using tsup
- `pnpm test` - Run all tests with Vitest
- `pnpm test-coverage` - Run tests with coverage reporting
- `pnpm type-check` - Type check with TypeScript and generate TypeDoc
- `pnpm lint` - Lint code with ESLint
- `pnpm lint-fix` - Fix linting issues automatically

### Testing Specific Components

- `pnpm test -- --reporter=verbose` - Run tests with detailed output
- Test timeout is set to 60 seconds for integration tests

### Monorepo Commands (from root)

- `turbo build` - Build all packages in the monorepo
- `turbo test` - Test all packages in the monorepo

## Architecture Overview

This package provides Drizzle ORM storage implementations for the durable-execution framework, supporting both PostgreSQL and SQLite databases.

### Core Components

**Storage Implementations** (`src/pg.ts`, `src/sqlite.ts`)

- `createPgStorage()` - PostgreSQL storage implementation using Drizzle ORM
- `createSQLiteStorage()` - SQLite storage implementation with transaction mutex
- Both implement the `Storage` interface from durable-execution
- Support for concurrent transactions with proper locking mechanisms

**Schema Definitions** (`src/pg.ts`, `src/sqlite.ts`)

- `createTaskExecutionsPgTable()` - PostgreSQL table schema with proper indexes
- `createTaskExecutionsSQLiteTable()` - SQLite table schema with compatibility adaptations
- Tables include all fields required by the durable-execution storage interface
- Optimized indexes for execution lookups and status-based queries

**Data Transformation** (`src/common.ts`)

- `storageValueToInsertValue()` - Converts storage format to database format
- `selectValueToStorageValue()` - Converts database rows back to storage format
- `storageValueToUpdateValue()` - Handles partial updates with proper null handling
- Handles hierarchical task relationships (root/parent task references)

### Database Schema Structure

**Key Fields:**

- `executionId` - Unique identifier with unique index
- `taskId` - Task type identifier
- Status and lifecycle fields (`status`, `isClosed`, `startedAt`, `finishedAt`)
- Parent-child relationships (`rootTaskId`, `parentTaskId`, `isFinalizeTask`)
- Execution data (JSON fields for complex objects, text for serialized data)
- Retry and timeout configuration fields

**Performance Indexes:**

- Unique index on `executionId` for fast lookups
- Composite index on `status`, `isClosed`, `expiresAt` for batch processing
- Index on `status`, `startAt` for task scheduling queries

### Database-Specific Adaptations

**PostgreSQL Features:**

- Native JSON column types for complex data
- Proper timestamp with timezone support
- Row-level locking with `FOR UPDATE SKIP LOCKED`
- Identity columns for auto-incrementing IDs

**SQLite Adaptations:**

- JSON stored as TEXT with mode annotations
- Transaction mutex to handle SQLite's serialization requirements
- Boolean fields stored as INTEGER
- Timestamps stored as INTEGER in timestamp mode

### Testing Approach

**Test Structure:**

- Integration tests using temporary databases (PGlite and libsql file)
- Comprehensive workflow testing including concurrent scenarios
- Tests all major task patterns: simple, sequential, parent-child, error handling
- Schema creation via Drizzle Kit push API for test setup

**Test Scenarios:**

- Complex parent-child task hierarchies with finalize tasks
- Concurrent execution of 100+ tasks
- Retry logic and failure handling
- Large-scale concurrent parent tasks (250 children)
- Task execution state transitions and error propagation

### Storage Interface Implementation

Both storage implementations provide:

- Transaction support via `withTransaction()`
- Batch operations for task execution CRUD
- Query operations with filtering and limiting
- Optimistic locking through version fields
- Proper handling of concurrent access patterns

### Integration with durable-execution

This package implements the `Storage` interface from the core durable-execution library:

- `StorageTx` transaction interface for atomic operations
- Support for all task execution states and transitions
- Compatible with the executor's background processes and concurrency controls
- Handles task expiration, cancellation, and cleanup operations
