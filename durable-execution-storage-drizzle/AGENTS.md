# AGENTS.md - durable-execution-storage-drizzle

## Project Overview

This is a TypeScript library that provides Drizzle ORM storage implementation for the durable-execution framework. It supports PostgreSQL, MySQL, and SQLite databases with transaction support and type-safe schema definitions.

## Project Structure

- `src/` - Main source code
  - `index.ts` - Main exports
  - `pg.ts` - PostgreSQL implementation
  - `mysql.ts` - MySQL implementation
  - `sqlite.ts` - SQLite implementation
  - `common.ts` - Shared utilities
- `tests/` - Test files
- `scripts/` - Build and utility scripts
- `build/` - Compiled output (generated)

## Common Commands

### Build & Development

- `npm run build` - Build the project (uses tsup)
- `npm run clean` - Clean build and coverage directories
- `npm run type-check` - Run TypeScript type checking and typedoc validation

### Testing Conventions

- `npm test` - Run tests (uses Vitest)
- `npm run test-coverage` - Run tests with coverage report
- `npm run bench` - Run benchmark scripts

### Code Quality

- `npm run lint` - Run ESLint
- `npm run lint-fix` - Run ESLint with auto-fix

## Technology Stack

- **Language**: TypeScript (ES modules)
- **ORM**: Drizzle ORM
- **Testing**: Vitest
- **Bundler**: tsup
- **Linting**: ESLint
- **Databases**: PostgreSQL, MySQL, SQLite

## Development Guidelines

### Code Style

- Uses ES modules (`"type": "module"`)
- TypeScript strict mode enabled
- Extends `@gpahal/tsconfig/base.json`
- No side effects (`"sideEffects": false`)

### Testing

- Tests are in `tests/` directory
- Uses Vitest for testing
- Supports test containers for database testing
- Run `npm test` to execute tests

### Database Support

- PostgreSQL: `createPgTaskExecutionsTable()`, `createPgTaskExecutionsStorage()`
- MySQL: `createMySqlTaskExecutionsTable()`, `createMySqlTaskExecutionsStorage()`
- SQLite: `createSQLiteTaskExecutionsTable()`, `createSQLiteTaskExecutionsStorage()`

### Dependencies

- **Peer Dependencies**: `drizzle-orm`, `durable-execution`
- **Runtime**: `@gpahal/std`
- **Dev Dependencies**: Various database clients, testcontainers, drizzle-kit

## Build Process

1. Type checking with TypeScript
2. Bundling with tsup
3. Output to `build/` directory
4. Generates both `.js` and `.d.ts` files

## Testing Strategy

Uses testcontainers for integration testing with real databases. Tests cover all three supported database types (PostgreSQL, MySQL, SQLite).

## Entry Points

- Main export: `./build/index.js` with types at `./build/index.d.ts`
- Package.json is also exported for metadata access
