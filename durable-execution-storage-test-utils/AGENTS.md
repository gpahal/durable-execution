# AGENTS.md

This is a TypeScript library for testing durable-execution storage implementations. It provides comprehensive test utilities and benchmarks for validating TaskExecutionsStorage implementations.

## Project Structure

- `src/` - Main source code (TypeScript)
- `tests/` - Test files (Vitest)
- `scripts/` - Utility scripts (benchmarking)
- `build/` - Compiled output directory

## Development Commands

### Build & Type Checking

- `pnpm build` - Build the project (cleans and runs tsup)
- `pnpm type-check` - Run TypeScript compiler and typedoc checks
- `pnpm clean` - Clean build and coverage directories

### Testing

- `pnpm test` - Run tests with Vitest
- `pnpm test-coverage` - Run tests with coverage reporting

### Linting

- `pnpm lint` - Run ESLint
- `pnpm lint-fix` - Run ESLint with auto-fix

### Benchmarking

- `pnpm bench` - Run benchmark scripts

## Code Conventions

- **Language**: TypeScript with ES modules (`"type": "module"`)
- **Build tool**: tsup for building
- **Testing**: Vitest with global APIs
- **Linting**: ESLint with `@gpahal/eslint-config`'s `defineConfig`
- **TypeScript config**: Extends @gpahal/tsconfig/base.json
- **File structure**: Single index.ts entry point in src/
- **Imports**: Use node: protocol for Node.js built-ins
- **Dependencies**: Uses catalog: references for workspace deps

## Key Features

This library provides:

- `runStorageTest()` - Comprehensive storage implementation validation
- `runStorageBench()` - Performance benchmarking for storage implementations
- Temporary file/directory helpers for testing
- Tests for concurrent execution, retry logic, parent-child relationships, error handling

## Dependencies

- **Peer dependencies**: durable-execution (workspace)
- **Runtime**: @gpahal/std, nanoid
- **Dev**: durable-execution, effect, TypeScript, Vitest, ESLint

## Notes

- This is a test utilities package - not meant for production use
- Designed specifically for validating durable-execution storage implementations
- Uses Vitest for testing framework
- Part of a monorepo workspace structure
