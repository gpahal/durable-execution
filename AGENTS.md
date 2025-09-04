# AGENTS.md - Durable Execution Monorepo

## Project Overview

This is a TypeScript monorepo containing the **durable-execution** ecosystem - a library for durable task execution with resilience to failures, along with various storage implementations and utilities.

## Monorepo Structure

- [`durable-execution/`](./durable-execution/) - Core durable execution library
- [`durable-execution-storage-convex/`](./durable-execution-storage-convex/) - Convex storage implementation
- [`durable-execution-storage-drizzle/`](./durable-execution-storage-drizzle/) - Drizzle ORM storage implementation
- [`durable-execution-storage-test-utils/`](./durable-execution-storage-test-utils/) - Test utilities for storage implementations
- [`durable-execution-orpc-utils/`](./durable-execution-orpc-utils/) - oRPC utilities for durable execution

## Frequently Used Commands

### Monorepo Commands (from root)

- `pnpm build` - Build all packages using Turbo
- `pnpm test` - Run all tests across packages
- `pnpm test-coverage` - Run all tests with coverage reports
- `pnpm type-check` - Run TypeScript checking across all packages
- `pnpm lint` - Run ESLint across all packages
- `pnpm lint-fix` - Run ESLint with auto-fix across all packages
- `pnpm clean` - Clean build artifacts from all packages
- `pnpm bench` - Run benchmarks across packages
- `pnpm build-docs` - Build documentation for the core package
- `pnpm fmt` - Format all files with Prettier
- `pnpm fmt-check` - Check formatting with Prettier
- `pnpm pre-commit` - Run full pre-commit checks (type-check, lint, format, build)

### Package Management

- `pnpm cs` - Create and version changesets for publishing
- `pnpm cs-publish` - Publish packages with changesets

### Individual Package Commands

Each package has its own [`AGENTS.md`] file with package-specific commands. Use `pnpm -F <package-name> <command>` to run commands in specific packages:

- `pnpm -F durable-execution build` - Build only the core package
- `pnpm -F durable-execution-storage-convex test` - Test only the Convex storage package

## Technology Stack

- **Language**: TypeScript (ES modules only)
- **Package Manager**: pnpm with workspaces
- **Build System**: Turbo for monorepo orchestration
- **Bundler**: tsup for package building
- **Testing**: Vitest with v8 coverage
- **Linting**: ESLint with `@gpahal/eslint-config`
- **Formatting**: Prettier
- **Type Checking**: TypeScript with strict mode
- **Documentation**: TypeDoc with Mermaid plugin

## Development Guidelines

### TypeScript Code Style & Conventions

#### Language Standards

- **Strict TypeScript**: All packages use strict mode with comprehensive type checking
- **ESM Only**: Pure ES modules (`"type": "module"`) - no CommonJS
- **Base Configuration**: Extends `@gpahal/tsconfig/base.json` for consistent compiler settings
- **Node.js**: Minimum version 20.0.0 required

#### Naming Conventions

- **Variables & Functions**: camelCase (`taskExecution`, `createStorage()`)
- **Classes & Types**: PascalCase (`DurableExecutor`, `TaskExecution`)
- **Constants**: SCREAMING_SNAKE_CASE (`ALL_TASK_EXECUTION_STATUSES`)
- **Files**: kebab-case (`promise-pool.ts`, `task-execution.ts`)
- **Directories**: kebab-case (`durable-execution`, `storage-convex`)

#### Type System Usage

- **Export Types**: Always use `export { type TypeName }` for type-only exports
- **Readonly Arrays**: Use `ReadonlyArray<T>` for immutable parameters
- **Generic Constraints**: Use descriptive generic names (`InferTaskInput<T>`, `AnyTask`)
- **Union Types**: Prefer union types over enums for string constants
- **Optional Properties**: Use `?` for optional properties, avoid `| undefined`

#### Documentation Standards

- **JSDoc Comments**: Comprehensive documentation for all public APIs
- **Examples**: Include `@example` blocks with practical usage
- **Categories**: Use `@category` tags to organize documentation sections
- **Parameter Descriptions**: Document all parameters with `@param`
- **Return Values**: Document return types with `@returns`

#### Code Organization

- **Index Files**: Central exports from `src/index.ts` with explicit type exports
- **Internal Types**: Prefix with `Internal` for private implementation types
- **Error Handling**: Custom error classes extending base error types
- **Factory Functions**: Use factory pattern for object creation (`createLogger`, `createStorage`)
- **Validation**: Use Effect Schema for runtime validation

#### Import/Export Patterns

- **Explicit Exports**: Always explicitly export types and values
- **Grouped Imports**: Group by type vs value imports
- **External Dependencies**: Import from `@gpahal/std` for common utilities
- **Re-exports**: Use re-exports to create clean public APIs

#### Error Handling Conventions

- **Custom Errors**: Extend `CustomError` from `@gpahal/std/errors`
- **Error Types**: Use specific error types for different failure modes
- **Retry Logic**: Include `isRetryable` flags for durable execution errors
- **Factory Methods**: Provide `.retryable()` and `.nonRetryable()` factory methods

#### Testing Standards

- **Vitest**: Use Vitest with globals enabled (no need to import `describe`, `it`)
- **Test Structure**: Mirror source structure in `tests/` directories
- **File Naming**: Use `.test.ts` suffix for all test files
- **Edge Runtime**: Test in edge-runtime environment where applicable
- **Coverage**: Aim for comprehensive test coverage with v8 provider

#### Package Standards

- **Side Effects**: Mark packages as `"sideEffects": false`
- **Peer Dependencies**: Use peer dependencies for shared libraries
- **Catalog Dependencies**: Use catalog system for version management
- **Build Output**: Generate both `.js` and `.d.ts` files in `build/` directory

### Testing Conventions

- Vitest for all testing with globals enabled
- Tests in `tests/` directories with `.test.ts` extension
- Edge-runtime environment for relevant packages
- Testcontainers for integration testing with databases
- Comprehensive coverage requirements

### Package Conventions

- All packages follow consistent structure with `src/`, `tests/`, `build/` directories
- Individual AGENTS.md files in each package
- Shared build scripts and configurations
- Catalog-based dependency management

### Git Workflow

- Pre-commit hooks run type-check, lint, format check, and build
- Pre-push hooks run the same checks
- Changesets for version management and publishing

## Architecture Overview

The durable execution system consists of:

1. **Core Library** (`durable-execution`) - Main execution engine with task orchestration
2. **Storage Implementations** - Multiple backend options (Convex, Drizzle ORM)
3. **Utilities** - Helper packages for testing and integration (oRPC, test utils)

### Key Concepts

- **Durable Tasks**: Resilient to failures with automatic retry mechanisms
- **Task Relationships**: Parent/child hierarchies with dependency management
- **Storage Abstraction**: Pluggable storage backends for different use cases
- **Type Safety**: Comprehensive TypeScript coverage throughout

## Package Dependencies

- **Peer Dependencies**: Each storage package depends on `durable-execution`
- **Shared Dependencies**: Common tooling and configuration packages
- **Catalog System**: Centralized dependency version management

## Getting Started

1. Install dependencies: `pnpm install`
2. Build all packages: `pnpm build`
3. Run tests: `pnpm test`
4. Check types: `pnpm type-check`

For package-specific development, see individual AGENTS.md files in each package directory.
