# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Essential Commands

```bash
# Build the library
pnpm build

# Run tests
pnpm test              # Run all tests
pnpm test-coverage     # Run tests with coverage report

# Type checking and linting
pnpm type-check        # TypeScript type checking + typedoc validation
pnpm lint              # Run ESLint
pnpm lint-fix          # Auto-fix linting issues

# Clean build artifacts
pnpm clean

# Run benchmarks
pnpm bench              # Performance benchmarks for storage implementations
```

### Testing Specific Files

```bash
# Run a specific test file
pnpm test tests/index.test.ts

# Run tests in watch mode
pnpm test -w

# Run tests matching a pattern
pnpm test -t 'should handle PostgreSQL'
```

## Architecture Overview

This package provides **production-ready database storage implementations** for the durable-execution framework using Drizzle ORM, supporting PostgreSQL, MySQL, and SQLite.

### Core Components

**PostgreSQL Implementation** (`src/pg.ts`):

- `createPgTaskExecutionsTable()`: Creates PostgreSQL table schema
- `createPgTaskExecutionsStorage()`: Creates PostgreSQL storage instance
- Optimized with JSONB columns and compound indexes

**MySQL Implementation** (`src/mysql.ts`):

- `createMySqlTaskExecutionsTable()`: Creates MySQL table schema
- `createMySqlTaskExecutionsStorage()`: Creates MySQL storage instance
- Requires update count extractor function for affected rows

**SQLite Implementation** (`src/sqlite.ts`):

- `createSQLiteTaskExecutionsTable()`: Creates SQLite table schema
- `createSQLiteTaskExecutionsStorage()`: Creates SQLite storage instance
- Lightweight option for development and testing

**Common Utilities** (`src/common.ts`):

- Shared logic for all database implementations
- Transaction handling and batch operations
- Query builders and update strategies

### Database Schema Design

**Table Structure**:

- `executionId`: Primary key (VARCHAR 32)
- `taskId`: Task identifier with index
- `status`: Execution status with compound indexes
- `input`/`output`: JSON/JSONB columns for data
- `error`: Structured error information
- Timestamps: `startedAt`, `finishedAt`, `updatedAt`
- Parent-child tracking: `parentExecutionId`, `activeChildrenCount`
- Timeout management: `expiresAt`, `ocfpExpiresAt`, `closeExpiresAt`

**Optimized Indexes**:

- Status queries: `idx_status_startedAt`
- Parent relationships: `idx_parentExecutionId_isFinished`
- Background processing: `idx_closeStatus_updatedAt`
- Timeout handling: `idx_expiresAt`, `idx_ocfpExpiresAt`
- Executor management: `idx_executorId_needsPromiseCancellation`

### Key Implementation Details

**Transaction Support**: All operations use database transactions to ensure ACID properties, critical for concurrent task execution.

**Batch Operations**: Each database implementation (PostgreSQL, MySQL, SQLite) implements the `TaskExecutionsStorageWithoutBatching` interface and is wrapped with `TaskExecutionsStorageWithBatching` from the core package. This wrapper provides automatic batching of operations using a DataLoader pattern, converting individual operations into efficient batch requests.

**Update Strategies**:

- Conditional updates with WHERE clauses for optimistic concurrency
- Atomic counter updates for parent active children count
- Status transition validation at database level

**Error Handling**: Follows durable-execution error patterns with proper serialization of error details into JSON columns.

### Database-Specific Considerations

**PostgreSQL**:

- Uses JSONB for better query performance
- Native UUID support available but uses VARCHAR for consistency
- Connection pooling recommended for production

**MySQL**:

- Requires explicit affected rows extraction
- JSON column type for structured data
- Consider connection limits in serverless environments

**SQLite**:

- In-memory option for testing
- File-based persistence for development
- Limited concurrent write performance

### Testing Approach

Tests verify:

- Complete storage interface implementation
- Transaction isolation and rollback
- Concurrent access patterns
- Database-specific features
- Performance under load (250+ concurrent tasks)

Uses:

- Testcontainers for PostgreSQL integration tests
- Testcontainers for MySQL integration tests
- SQLite in-memory for fast unit tests

### Migration Strategy

- Create table schema in your Drizzle schema file

```ts
export const taskExecutions = createPgTaskExecutionsTable('task_executions')
```

- Generate migration files

```bash
pnpx drizzle-kit generate
```

- Apply migrations

```bash
pnpx drizzle-kit migrate
```

### Performance Optimization

**Connection Pooling**:

```ts
const db = drizzle(pool, {
  logger: true // Enable query logging in development
})
```

**Query Optimization**:

- Use indexes for frequent query patterns
- Batch operations to reduce round trips
- Consider read replicas for query-heavy workloads

**Transaction Isolation**:

- Default isolation level sufficient for most cases
- Consider READ COMMITTED for high concurrency
- Monitor for deadlocks in production

### Important Conventions

- All public APIs exported through `src/index.ts`
- Database-specific implementations in separate files
- Shared logic extracted to `src/common.ts`
- Follow Drizzle ORM best practices for schema definition
- Test with realistic data volumes and concurrency levels
