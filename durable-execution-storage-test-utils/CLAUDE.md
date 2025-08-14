# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a TypeScript library that provides test utilities for validating storage implementations in the durable-execution ecosystem. It's part of a monorepo with packages including `durable-execution` (core), `durable-execution-orpc-utils`, and `durable-execution-storage-drizzle`.

## Essential Commands

### Development

```bash
# Install dependencies (from monorepo root)
pnpm install

# Build the package
pnpm build

# Run tests
pnpm test

# Run tests with coverage
pnpm test:coverage

# Lint code
pnpm lint

# Format code
pnpm format
```

### Monorepo Context

This package uses Turbo for build orchestration and pnpm workspaces. When making changes, consider running commands from the monorepo root to ensure proper dependency coordination.

## Architecture

### Core Functionality

The package exports test utilities from `src/index.ts`:

- **`runStorageTest(storage, cleanup?)`** - Main comprehensive test suite that validates storage implementations through complex scenarios including:
  - DurableExecutor functionality with task hierarchies
  - Concurrent execution (250+ tasks)
  - Retry mechanisms and error handling
  - Parent-child task relationships

- **`createTaskExecutionStorageValue()`** - Factory for test storage values
- **Temporary resource helpers** - `withTemporaryDirectory()`, `withTemporaryFile()`, `cleanupTemporaryFiles()`

### Dependencies

- **Peer dependency**: `durable-execution` (workspace)
- **Dev dependency**: `@gpahal/std` for utilities
- **Runtime**: Node.js >=20.0.0, ESM modules

### Testing Strategy

- Uses **Vitest** with 120-second timeouts for comprehensive storage tests
- Tests demonstrate usage with `InMemoryTaskExecutionsStorage`
- Coverage reporting via `@vitest/coverage-v8`

## Key Configuration

- **TypeScript**: Strict configuration with declaration generation
- **Build**: tsup bundler with inherited config from parent
- **Linting**: ESLint with `@gpahal/eslint-config/base`
- **Package manager**: pnpm with workspace support

## Development Notes

- This is a library package (not an application)
- Uses semantic versioning and is published to NPM
- Part of coordinated releases across the durable-execution ecosystem
- When implementing new test utilities, ensure they work with the existing `runStorageTest` framework
