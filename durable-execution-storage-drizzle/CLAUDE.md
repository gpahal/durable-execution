# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `durable-execution-storage-drizzle`, a storage implementation for the durable-execution framework using Drizzle ORM. It provides database storage adapters for PostgreSQL, MySQL, and SQLite to persist task execution state in durable workflow systems.

## Development Commands

### Build and Test

- `npm run build` - Build the TypeScript code using tsup
- `npm run clean` - Remove build artifacts
- `npm run test` - Run tests using Vitest
- `npm run test-coverage` - Run tests with coverage report
- `npm run type-check` - Type check with TypeScript and generate docs
- `npm run lint` - Lint code with ESLint
- `npm run lint-fix` - Auto-fix linting issues

### Testing

Tests use Vitest and include integration tests with real database containers (PostgreSQL via PGlite, MySQL via Testcontainers, SQLite in-memory). The test configuration inherits from the parent durable-execution workspace.

## Architecture

### Core Structure

The package follows a database-per-file organization:

- `src/index.ts` - Main exports for all database implementations
- `src/common.ts` - Shared type definitions and data transformation utilities
- `src/pg.ts` - PostgreSQL implementation with Drizzle schema and storage adapter
- `src/mysql.ts` - MySQL implementation
- `src/sqlite.ts` - SQLite implementation

### Data Model

The storage layer implements the `TaskExecutionsStorage` interface from durable-execution. Key concepts:

- **Task Executions** - Individual task runs with state, hierarchy, and metadata
- **Parent-Child Relationships** - Tasks can spawn child tasks and finalize tasks
- **Status Tracking** - Comprehensive state management (pending, running, completed, failed, etc.)
- **Retry Logic** - Built-in retry mechanisms with configurable options
- **Timeouts and Expiration** - Automatic cleanup of expired executions

### Database Schema

Each database implementation provides:

- Table creation functions (`createPgTaskExecutionsTable`, etc.)
- Storage adapter functions (`createPgTaskExecutionsStorage`, etc.)
- Optimized indexes for performance queries
- Transaction support for atomic operations

### Key Data Transformations

- `taskExecutionStorageValueToInsertValue` - Converts durable-execution format to DB format
- `taskExecutionSelectValueToStorageValue` - Converts DB format back to durable-execution format
- `taskExecutionStorageValueToUpdateValue` - Handles partial updates and special cases

## Database Support

### PostgreSQL

- Uses `drizzle-orm/pg-core` with standard PostgreSQL types
- Supports JSONB for complex data structures
- Includes optimized indexes for common query patterns

### MySQL

- Uses `drizzle-orm/mysql-core` with MySQL-specific types
- Requires affectedRows callback for proper update handling
- JSON column support for complex data

### SQLite

- Uses `drizzle-orm/sqlite-core`
- Lightweight option suitable for development and testing
- Full feature parity with other database implementations

## Dependencies

### Peer Dependencies

- `drizzle-orm` - The ORM framework (from catalog)
- `durable-execution` - Core durable execution framework (workspace dependency)

### Development/Testing

- Database drivers for testing: `@electric-sql/pglite`, `@libsql/client`, `@testcontainers/mysql`
- `durable-execution-storage-test-utils` - Shared test utilities (workspace dependency)
- `drizzle-kit` - Schema management and migrations

## Integration Notes

This package is designed to be used with the broader durable-execution ecosystem. Users typically:

1. Create a database table using the provided schema functions
2. Export the table from their schema file for Drizzle Kit discovery
3. Run migrations using Drizzle Kit
4. Create a storage instance and pass it to `DurableExecutor`

The storage implementations handle all the complexity of mapping between the durable-execution interfaces and database-specific operations.
