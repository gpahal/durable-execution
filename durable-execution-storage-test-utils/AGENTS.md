# AGENTS.md

This is a TypeScript library for testing durable-execution storage implementations. It provides comprehensive test utilities and benchmarks for validating TaskExecutionsStorage implementations.

## Project Structure

- `src/` - Main source code (TypeScript)
- `tests/` - Test files (Vitest)
- `scripts/` - Utility scripts (benchmarking)
- `build/` - Compiled output directory

## Development Commands

### Build & Type Checking

- `npm run build` - Build the project (cleans and runs tsup)
- `npm run type-check` - Run TypeScript compiler and typedoc checks
- `npm run clean` - Clean build and coverage directories

### Testing

- `npm run test` - Run tests with Vitest
- `npm run test-coverage` - Run tests with coverage reporting

### Linting

- `npm run lint` - Run ESLint
- `npm run lint-fix` - Run ESLint with auto-fix

### Benchmarking

- `npm run bench` - Run benchmark scripts

## Code Conventions

- **Language**: TypeScript with ES modules (`"type": "module"`)
- **Build tool**: tsup for building
- **Testing**: Vitest with global APIs
- **Linting**: ESLint with @gpahal/eslint-config/base
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
- **Dev**: @gpahal/std-node, TypeScript, Vitest, ESLint

## Notes

- This is a test utilities package - not meant for production use
- Designed specifically for validating durable-execution storage implementations
- Uses Vitest for testing framework
- Part of a monorepo workspace structure
