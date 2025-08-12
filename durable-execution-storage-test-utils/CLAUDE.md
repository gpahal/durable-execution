# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Build and Development

- `pnpm build` - Build the package using tsup
- `pnpm clean` - Clean build artifacts
- `pnpm type-check` - Run TypeScript type checking and typedoc validation

### Testing

- `pnpm test` - Run all tests with Vitest
- `pnpm test-coverage` - Run tests with coverage report

### Linting

- `pnpm lint` - Run ESLint
- `pnpm lint-fix` - Run ESLint with automatic fixes

## Architecture

This is a test utilities package for durable-execution storage implementations. The main architecture consists of:

### Core Components

- **runStorageTest()**: Main test function that takes a `Storage` implementation and runs comprehensive tests against it
- **DurableExecutor**: Creates and manages durable task execution with configurable timeout and retry options
- **Test utilities**: Helper functions for temporary file/directory management

### Test Pattern Structure

The test suite validates storage implementations through:

1. **Sequential Tasks** (`taskB1`, `taskB2`, `taskB3`) - Tests chained task execution
2. **Parent Tasks with Children** (`taskA`) - Tests hierarchical task structures with finalization
3. **Concurrent Tasks** (250 tasks) - Tests parallel execution handling
4. **Retry Logic** - Tests task retry mechanisms with configurable attempts
5. **Error Handling** - Tests various failure scenarios (task failures, child failures, finalization failures)
6. **Large Scale Concurrency** (500 child tasks) - Tests high-volume concurrent execution

### Key Testing Scenarios

- Root task with nested parent/child task hierarchies
- Sequential task chaining with data flow
- Concurrent task execution at scale
- Retry mechanisms and failure handling
- Parent task finalization with child task results aggregation

## Dependencies

This package has a peer dependency on `durable-execution` (workspace) and uses `@gpahal/std` for utilities. The package is built as an ES module with TypeScript support.
