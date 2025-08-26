# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Essential Commands

```bash
# Build the library
pnpm build

# Run tests
pnpm test              # Run all tests
pnpm test-coverage     # Run tests with coverage report

# Type checking and linting
pnpm type-check        # TypeScript type checking + typedoc validation
pnpm lint              # Run ESLint
pnpm lint-fix          # Auto-fix linting issues

# Clean build artifacts
pnpm clean

# Run benchmarks
pnpm bench              # Performance benchmarks for storage implementations
```

### Testing Specific Files

```bash
# Run a specific test file
pnpm test tests/index.test.ts

# Run tests in watch mode
pnpm test -w

# Run tests matching a pattern
pnpm test -t 'should validate storage'
```

## Architecture Overview

This package provides **comprehensive test utilities** for validating durable-execution storage implementations, ensuring they correctly handle all aspects of task execution.

### Core Components

**Storage Test Suite** (`src/index.ts`):

- `runStorageTest()`: Main test function that validates complete storage behavior
- `runStorageBench()`: Benchmark function that measures storage performance
- Tests complex task hierarchies, concurrency, and error scenarios
- Validates 250+ concurrent tasks with proper coordination
- Ensures ACID properties and transaction isolation

**Temporary Resource Utilities**:

- `withTemporaryDirectory()`: Creates and cleans up temporary directories
- `withTemporaryFile()`: Creates and cleans up temporary files
- `cleanupTemporaryFiles()`: Removes leftover temporary resources
- Automatic cleanup on process exit

### Test Coverage Areas

**Task Lifecycle Management**:

- State transitions: ready → running → completed/failed/timed_out
- Parent-child task relationships and coordination
- Sequential task chains and finalization
- Cancellation and cleanup processes

**Concurrency Testing**:

- High concurrency scenarios (250+ tasks)
- Race condition detection
- Deadlock prevention
- Optimistic concurrency control

**Error Handling**:

- Retry logic with exponential backoff
- Error propagation in task hierarchies
- Timeout handling and expiration
- Graceful degradation patterns

**Storage Operations**:

- CRUD operations and batch updates
- Transaction rollback scenarios
- Index performance validation
- Query optimization verification

### Key Test Scenarios

**Complex Task Hierarchies**: Tests nested parent-child relationships with multiple levels, ensuring proper state propagation and active child counting.

**Background Processing**: Validates task expiration, closure processes, promise cancellation, and executor handoff scenarios.

**Performance Under Load**: Stress tests with high task volumes, concurrent executors, and rapid state changes to ensure storage scalability.

**Edge Cases**: Tests boundary conditions, null/undefined handling, maximum retry attempts, and timeout edge cases.

### Implementation Strategy

**Test Structure**:

```ts
await runStorageTest(storage, async () => {
  // Optional cleanup function
  await storage.close()
})
```

**Validation Approach**:

1. Create executor with storage implementation
2. Execute complex task scenarios
3. Verify state consistency
4. Check background process behavior
5. Validate cleanup and resource management

### Testing Best Practices

**Storage Implementation Requirements**:

- Must implement complete `TaskExecutionsStorage` interface
- Support concurrent transactions
- Handle large batch operations efficiently
- Maintain consistency under high load

**Common Pitfalls to Test**:

- Transaction isolation violations
- Race conditions in parent-child updates
- Incorrect active children counting
- Missing index causing slow queries
- Improper error serialization

### Extending Test Suite

**Custom Test Scenarios**:

```ts
import { runStorageTest } from 'durable-execution-storage-test-utils'
import type { TaskExecutionsStorage } from 'durable-execution'

// Add custom validations
const customTest = async (storage: TaskExecutionsStorage) => {
  await runStorageTest(storage)

  // Additional custom tests
  await testCustomScenario(storage)
}
```

**Performance Benchmarking**:

- Use `runStorageBench()` to measure storage performance
- Configurable workload parameters (executors, tasks, processes)
- Measure operation latencies across different task types
- Track memory usage patterns
- Monitor connection pool behavior
- Analyze query execution plans

```ts
import { runStorageBench } from 'durable-execution-storage-test-utils'

await runStorageBench("my-storage", () => new MyStorage(), {
  executorsCount: 3,
  parentTasksCount: 100,
  childTasksCount: 50,
  storageCleanup: async (storage) => {
    await storage.close()
  },
})
```

### Important Conventions

- All test utilities exported through `src/index.ts`
- Tests should be deterministic and repeatable
- Clean up all resources after test completion
- Use realistic data volumes and patterns
- Test both success and failure paths thoroughly
