# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Build and Development

```bash
pnpm build          # Build with tsup, generates ESM output
pnpm clean          # Remove build artifacts
pnpm type-check     # TypeScript type checking + typedoc validation
```

### Testing

```bash
pnpm test           # Run tests with Vitest
pnpm test-coverage  # Run tests with coverage reporting

# Run specific tests
pnpm vitest run tests/index.test.ts
```

### Code Quality

```bash
pnpm lint           # Run ESLint
pnpm lint-fix       # Run ESLint with auto-fix
```

## Architecture

### Package Structure

This is a Drizzle ORM storage implementation for the durable-execution framework, providing PostgreSQL, MySQL, and SQLite storage backends.

**Core Files**:

- `src/pg.ts` - PostgreSQL storage with `FOR UPDATE SKIP LOCKED` row locking
- `src/mysql.ts` - MySQL storage with similar transaction support
- `src/sqlite.ts` - SQLite storage with mutex-based transaction serialization
- `src/common.ts` - Shared data transformation utilities between storage types and DB rows

### Storage Implementation Pattern

Each storage backend provides:

1. **Table Creation Functions**:
   - `createTaskExecutions[Pg|MySql|SQLite]Table()` - Main task execution table
   - `createFinishedChildTaskExecutions[Pg|MySql|SQLite]Table()` - Completed child tasks table

2. **Storage Factory**:
   - `create[Pg|MySql|SQLite]Storage(db, taskTable, finishedChildTable)` - Returns Storage implementation
   - MySQL requires additional `getAffectedRows` callback for result parsing

3. **Key Operations** (all implement `Storage` interface from `durable-execution`):
   - `enqueueTaskExecution()` - Add new task
   - `getTaskExecution()` - Fetch by execution ID
   - `getTaskExecutions()` - Batch fetch with filtering
   - `claimTaskExecutions()` - Atomic claim for processing
   - `updateTaskExecutions()` - Update task state
   - `getFinishedChildTaskExecutions()` - Get completed children
   - `addFinishedChildTaskExecutions()` - Record child completion

### Database Schema

All implementations share the same logical schema:

**Task Executions Table**:

- Identity: `id`, `taskId`, `executionId`
- Hierarchy: `rootTaskId`, `parentTaskId`, `parentExecutionId`
- State: `status`, `retryAttempts`, `error`
- Timing: `startAt`, `expiresAt`, `closingExpiresAt`
- Data: `input`, `runOutput`, `output` (JSON)
- Child coordination: `childTaskProcessingOrder`, `finishedChildTaskExecutionIds`

**Finished Child Tasks Table** (for parent tasks):

- Links to parent via `parentTaskId` + `parentExecutionId`
- Stores child result data for finalize task processing

### Transaction Patterns

- **PostgreSQL/MySQL**: Native transactions with row-level locking
- **SQLite**: All operations wrapped in mutex to ensure serialization (SQLite doesn't support concurrent writes)

Key pattern for claim operations:

```sql
SELECT ... FOR UPDATE SKIP LOCKED  -- PostgreSQL/MySQL
-- SQLite uses mutex instead
```

### Testing Approach

Tests in `tests/index.test.ts` use:

- `@electric-sql/pglite` for PostgreSQL testing
- `@libsql/client` for SQLite testing
- `@testcontainers/mysql` for MySQL testing (via Docker)
- `durable-execution-storage-test-utils` for comprehensive storage validation

Each storage implementation is tested with the full test suite from test utils, ensuring consistent behavior across all backends.
