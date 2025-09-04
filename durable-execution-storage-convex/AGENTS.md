# AGENTS.md

## Project Overview

This project provides a Convex storage implementation for the `durable-execution` library. It includes both client-side components and server-side Convex components for storing and managing durable task executions.

## Frequently Used Commands

### Building & Type Checking

- `pnpm build` - Build the project using tsup
- `pnpm type-check` - Run TypeScript type checking and TypeDoc validation
- `tsc --noEmit` - Quick TypeScript type check only

### Testing

- `pnpm test` - Run all tests with vitest
- `vitest` - Run tests in watch mode
- `pnpm bench` - Run benchmarks

### Linting & Code Quality

- `pnpm lint` - Run ESLint
- `pnpm lint-fix` - Run ESLint with auto-fix

### Development

- `pnpm convex-dev` - Start Convex development server with live component sources and type checking
- `pnpm convex` - Run Convex CLI commands
- `pnpm clean` - Clean build and coverage directories

## Project Structure

- `src/client/` - Client-side TypeScript code for the storage implementation
- `src/component/` - Convex component configuration and server functions
- `src/common.ts` - Shared types and utilities
- `test/` - Test Convex project setup
- `tests/` - Vitest test files
- `scripts/` - Build and utility scripts

## Code Conventions

- TypeScript with strict type checking enabled
- ESM modules (`"type": "module"`)
- Uses `@gpahal/eslint-config/base` for linting rules
- Filename case rules disabled for Convex compatibility
- Test files use `.test.ts` suffix in the `tests/` directory
- Vitest with edge-runtime environment for testing

## Dependencies

### Peer Dependencies

- `convex` - Convex platform SDK
- `durable-execution` - Core durable execution library

### Key Dependencies

- `@gpahal/std` - Standard utilities
- `zod` - Schema validation

## Testing Conventions

- Tests run in edge-runtime environment using vitest
- Test files located in `tests/` directory with `.test.ts` extension
- Coverage reports generated with v8 provider
- Uses testcontainers for integration testing

## Development Notes

- The project exports both client code and Convex component configuration
- Component source files are exposed via `@convex-dev/component-source` for live reloading
- Generated Convex files are ignored by ESLint (`**/_generated/**`)
- Build output goes to `build/` directory with separate client and component builds
