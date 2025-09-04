# AGENTS.md

This is the **durable-execution** package - a TypeScript library for durable task execution with resilience to failures.

## Project Structure

- `src/` - Main source code
- `tests/` - Vitest test files
- `build/` - Built output (generated)
- `docs/` - Documentation
- `scripts/` - Build scripts

## Commands

### Build & Type Check

- `pnpm build` - Build the project with tsup
- `pnpm type-check` - Run TypeScript compiler and typedoc validation

### Testing

- `pnpm test` - Run tests with Vitest
- `pnpm test-coverage` - Run tests with coverage report

### Linting

- `pnpm lint` - Run ESLint
- `pnpm lint-fix` - Run ESLint with auto-fix

### Other

- `pnpm clean` - Clean build and coverage directories
- `pnpm build-docs` - Build documentation

## Code Style & Conventions

### TypeScript

- Uses strict TypeScript configuration extending `@gpahal/tsconfig/base.json`
- ESM modules only (`"type": "module"`)
- Vitest for testing with globals enabled
- Test timeout: 15 seconds

### Dependencies

- **Core**: Uses Zod for validation, superjson for serialization, nanoid for IDs
- **Testing**: Vitest with v8 coverage provider
- **Build**: tsup for bundling

### Architecture

- Durable execution engine for tasks and workflows
- Task-based system with parent/child relationships
- Storage abstraction with in-memory and persistent implementations
- Resilient to failures with retry mechanisms
- TypeScript-first with comprehensive type safety

### Naming Conventions

- Use camelCase for variables and functions
- Use PascalCase for classes and types
- Use kebab-case for file names
- Task IDs should be descriptive strings

### Error Handling

- Use `DurableExecutionError` for task-specific errors
- Tasks should be idempotent as they may be re-executed
- Prefer explicit error handling over throwing generic errors

### Testing Conventions

- Tests are in `tests/` directory with `.test.ts` extension
- Use Vitest globals (no need to import `describe`, `it`, etc.)
- Test files mirror the source structure
- Aim for comprehensive test coverage
