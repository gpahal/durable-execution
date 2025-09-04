# AGENTS.md

This file contains information for AI coding agents working with the `durable-execution-orpc-utils` package.

## Project Overview

This is a TypeScript library that provides oRPC utilities for the `durable-execution` framework. It enables:

- Creating oRPC routers for durable task management
- Converting oRPC procedures to durable tasks
- Handling task enqueueing, execution status retrieval, and sleeping task wake-up operations

## Frequently Used Commands

### Build & Development

- `pnpm build` - Clean build directory and compile TypeScript with tsup
- `pnpm clean` - Remove build and coverage directories

### Testing

- `pnpm test` - Run tests with Vitest
- `pnpm test-coverage` - Run tests with coverage reporting

### Quality Assurance

- `pnpm type-check` - Run TypeScript compiler and TypeDoc validation (no emit)
- `pnpm lint` - Run ESLint
- `pnpm lint-fix` - Run ESLint with auto-fix

## Code Style & Conventions

### TypeScript

- Uses strict TypeScript configuration extending `@gpahal/tsconfig/base.json`
- Module type: ES modules (`"type": "module"`)
- Target: Modern ES standards
- No code comments unless explicitly needed for complex logic

### Code Structure

- Single entry point: `src/index.ts`
- All exports should be from the main index file
- Use proper TypeScript generics for type safety
- Follow existing patterns for oRPC procedure creation and error handling

### Dependencies

- Core dependencies: `@orpc/*` packages, `durable-execution`, `@gpahal/std`
- All peer dependencies should be declared explicitly
- Use workspace references for internal packages

### Error Handling

- Map HTTP status codes to appropriate oRPC errors:
  - 404 → `ORPCError('NOT_FOUND')`
  - 408, 429, 500-504 → Retryable errors
  - 5xx → Internal server errors
- Use `DurableExecutionError` types appropriately
- Include meaningful error messages

## Testing Conventions

- Tests are located in the `tests/` directory
- Uses Vitest as the test runner
- Global test types are configured in tsconfig.json
- Coverage reporting available with `pnpm test-coverage`

## Build Configuration

- Uses `tsup` for building
- Output directory: `build/`
- Generates both JS and TypeScript declaration files
- ESM format only

## Package Information

- Part of the larger `durable-execution` monorepo
- Published to npm as `durable-execution-orpc-utils`
- MIT licensed
- Sideeffect-free package
