# Cursor Rules

This directory contains Cursor Rules that provide context and guidance to AI assistants working with this codebase.

## Rules Overview

### Always Applied Rules

- **`project-structure.mdc`** - Monorepo structure, package layout, and build system overview

### Context-Specific Rules

- **`typescript-standards.mdc`** - TypeScript coding standards and patterns (applies to `*.ts`, `*.tsx` files)
- **`testing-conventions.mdc`** - Testing frameworks, patterns, and best practices (applies to test files)

### Manual/Description-Based Rules

- **`durable-execution-concepts.mdc`** - Core domain concepts, architecture, and implementation patterns
- **`package-development.mdc`** - Guidelines for developing and maintaining packages in the monorepo
- **`build-deployment.mdc`** - Build system, CI/CD workflows, and deployment patterns
- **`error-handling-patterns.mdc`** - Comprehensive error handling strategies for durable execution
- **`performance-optimization.mdc`** - Performance tuning, monitoring, and optimization patterns
- **`debugging-troubleshooting.mdc`** - Debugging tools, troubleshooting guides, and production monitoring

## Usage

These rules are automatically loaded by Cursor to provide AI assistants with:

- Understanding of the project structure and conventions
- Domain-specific knowledge about durable execution concepts
- Development workflow and testing practices
- Package management and release processes
- Build system and deployment guidance
- Error handling and recovery patterns
- Performance optimization strategies
- Debugging and troubleshooting techniques

The rules use the `.mdc` extension and include frontmatter metadata to control when they are applied:

- `alwaysApply: true` - Applied to every AI request
- `globs: "*.ts,*.tsx"` - Applied only to specific file types
- `description: "..."` - Applied when manually selected or contextually relevant

## Maintenance

When updating the codebase structure or conventions:

1. Update the relevant rule files
2. Ensure file references use the `[filename](mdc:path/to/file)` format
3. Test that rules provide accurate guidance for common development tasks
