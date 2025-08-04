# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Build

```bash
pnpm build  # Build the package using tsup
```

### Testing

```bash
pnpm test               # Run all tests
pnpm test-coverage      # Run tests with coverage
pnpm test [test-file]   # Run a specific test file
```

### Type Checking and Linting

```bash
pnpm type-check  # Run TypeScript type checking
pnpm lint        # Run ESLint
pnpm lint-fix    # Run ESLint with auto-fix
```

### Clean Build Artifacts

```bash
pnpm clean  # Remove build directory
```

## Architecture Overview

This package provides Drizzle ORM storage implementations for the durable-execution library, supporting PostgreSQL, MySQL, and SQLite databases.

### Key Components

1. **Storage Implementations** (`src/`):
   - `pg.ts`: PostgreSQL storage implementation with `createPgDurableStorage` and table schema
   - `mysql.ts`: MySQL storage implementation with `createMySQLDurableStorage` and table schema
   - `sqlite.ts`: SQLite storage implementation with `createSQLiteDurableStorage` and table schema
   - `common.ts`: Shared conversion utilities between storage objects and database values
   - `index.ts`: Re-exports all database implementations

2. **Data Model**:
   - Core table: `durable_task_executions` (customizable name)
   - Tracks task execution state including parent-child relationships
   - Supports transactions for atomic state updates
   - JSON columns for complex data (children, errors, outputs)

3. **Testing Strategy**:
   - Comprehensive integration tests in `tests/index.test.ts`
   - Tests all three database implementations (SQLite, PostgreSQL via PGlite, MySQL)
   - Validates complex parent-child task hierarchies and concurrent execution
   - Uses temporary databases for isolation

### Build Configuration

- **TypeScript**: Dual tsconfig setup (`tsconfig.json` for development, `tsconfig.build.json` for builds)
- **Bundling**: Uses tsup with ESM output format
- **Dependencies**: Drizzle ORM and durable-execution are peer dependencies
- **Monorepo**: Part of a pnpm workspace with shared configurations from parent

### Database Schema Design

Each database implementation creates a table with:

- Unique index on `execution_id` for performance
- Indexes on status fields for efficient querying
- Support for hierarchical task relationships (root, parent, children)
- Optimized for durable execution patterns with proper transaction boundaries
