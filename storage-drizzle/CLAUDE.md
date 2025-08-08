# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `durable-execution-storage-drizzle`, a Drizzle ORM storage implementation for the `durable-execution` package. It provides database persistence for durable task executions across PostgreSQL, MySQL, and SQLite databases using type-safe Drizzle ORM.

## Development Commands

### Building and Testing

- `npm run build` - Build the library using tsup
- `npm run clean` - Clean build artifacts
- `npm run test` - Run tests with Vitest
- `npm run test-coverage` - Run tests with coverage report
- `npm run type-check` - Type check with TypeScript compiler
- `npm run lint` - Lint code with ESLint
- `npm run lint-fix` - Lint and auto-fix issues

### Configuration Files

- TypeScript config extends `@gpahal/tsconfig/base.json`
- ESLint config uses `@gpahal/eslint-config/base`
- Vitest and tsup configs import from parent `../durable-execution/` directory
- Uses catalog dependencies for consistent versioning

## Architecture

### Core Structure

The package exports three database-specific implementations:

1. **PostgreSQL** (`src/pg.ts`):
   - `createDurableTaskExecutionsPgTable()` - Creates PostgreSQL table schema
   - `createPgDurableStorage()` - Creates storage implementation
   - Uses `pgTable` with proper indexes and timestamp handling

2. **MySQL** (`src/mysql.ts`):
   - `createDurableTaskExecutionsMySQLTable()` - Creates MySQL table schema
   - `createMySQLDurableStorage()` - Creates storage implementation
   - Uses `mysqlTable` with varchar constraints and auto-increment

3. **SQLite** (`src/sqlite.ts`):
   - `createDurableTaskExecutionsSQLiteTable()` - Creates SQLite table schema
   - `createSQLiteDurableStorage()` - Creates storage implementation
   - Uses `sqliteTable` with transaction mutex for safe concurrent access

### Common Components (`src/common.ts`)

- Type definitions for database value objects (`DurableTaskExecutionDbValue`)
- Transformation functions between storage objects and database values:
  - `storageObjectToInsertValue()` - Converts storage object for database insertion
  - `selectValueToStorageObject()` - Converts database row to storage object
  - `storageUpdateToUpdateValue()` - Converts update object for database operations

### Database Schema Features

All implementations include:

- Primary key with auto-increment/identity
- Unique index on `execution_id`
- Composite indexes on `(status, is_closed, expires_at)` and `(status, start_at)`
- JSON columns for complex data (retry options, children tasks, errors)
- Proper timestamp handling with timezone support (PostgreSQL)
- Support for hierarchical task relationships (root/parent task tracking)

### Storage Interface Implementation

Each storage class implements the `DurableStorage` interface:

- `withTransaction()` - Executes operations within database transactions
- Storage transaction classes implement `DurableStorageTx`:
  - `insertTaskExecutions()` - Bulk insert operations
  - `getTaskExecutionIds()` / `getTaskExecutions()` - Query operations with filtering
  - `updateTaskExecutions()` - Update operations with where conditions

### Key Implementation Details

- **PostgreSQL**: Uses `generatedAlwaysAsIdentity()` for primary keys and full timezone support
- **MySQL**: Uses `autoincrement()` and requires special handling for updates (select-for-update pattern)
- **SQLite**: Uses transaction mutex to prevent concurrent transaction issues and `returning()` clause support

## Testing

Tests are comprehensive and use real database instances:

- **In-memory**: Uses `durable-execution`'s built-in in-memory storage for baseline testing
- **SQLite**: Creates temporary database files for testing
- **PostgreSQL**: Uses PGlite (in-memory PostgreSQL) with Drizzle schema pushing
- **MySQL**: Uses Testcontainers to spin up real MySQL instances with migration generation

Test suite covers:

- Complex hierarchical task execution (parent/child tasks)
- Sequential and parallel task execution
- Retry logic and failure scenarios
- 100+ concurrent task execution
- Error handling for failed child tasks and finalize tasks

## Key Dependencies

### Peer Dependencies

- `drizzle-orm` ^0.44.4 - Database ORM
- `durable-execution` ^0.10.0 - Core durable execution framework

### Development Dependencies

- Database drivers: `@electric-sql/pglite`, `@libsql/client`, `mysql2`
- Testing: `@testcontainers/mysql`, `testcontainers` for real database testing
- Build tools: `drizzle-kit` for schema management

## Usage Patterns

When working with this codebase:

1. **Adding new database support**: Follow the pattern in existing `src/{db}.ts` files:
   - Create table schema function with proper types and indexes
   - Implement storage class with database-specific transaction handling
   - Add comprehensive tests following the existing test structure

2. **Schema changes**: Update the common types in `src/common.ts` and ensure all three database implementations are updated consistently

3. **Testing changes**: Always test against all three database types. The test suite is designed to run the same test logic against different storage implementations.

4. **Performance considerations**: The implementations are optimized with proper indexes. Be mindful of query patterns when making changes to the storage interface methods.
