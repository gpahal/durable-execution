import { createSuperjsonSerializer, SerializerInternal } from '../src/serializer'
import {
  convertTaskExecutionStorageValueToTaskExecution,
  getTaskExecutionStorageUpdate,
  TaskExecutionsStorageWithMutex,
  validateStorageMaxRetryAttempts,
  type TaskExecutionsStorage,
  type TaskExecutionStorageValue,
} from '../src/storage'

describe('validateStorageMaxRetryAttempts', () => {
  it('should handle valid max retry attempts', () => {
    expect(validateStorageMaxRetryAttempts(1)).toBe(1)
    expect(validateStorageMaxRetryAttempts(10)).toBe(10)
  })

  it('should handle undefined or null max retry attempts', () => {
    expect(validateStorageMaxRetryAttempts(undefined)).toBe(1)
    expect(validateStorageMaxRetryAttempts(null)).toBe(1)
  })

  it('should handle invalid max retry attempts', () => {
    expect(() => validateStorageMaxRetryAttempts(0)).toThrow('Invalid storage max retry attempts')
    expect(() => validateStorageMaxRetryAttempts(11)).toThrow('Invalid storage max retry attempts')
  })
})

describe('getTaskExecutionStorageUpdate', () => {
  it('should handle empty update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {})
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle ready update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'ready',
    })
    expect(update.unsetExecutorId).toBe(true)
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBe(true)
    expect(update.unsetError).toBeUndefined()
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle running update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'running',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle failed update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'failed',
    })
    expect(update.unsetExecutorId).toBe(true)
    expect(update.isFinished).toBe(true)
    expect(update.unsetRunOutput).toBe(true)
    expect(update.unsetError).toBeUndefined()
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.finishedAt).toBe(now)
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle timed_out update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'timed_out',
    })
    expect(update.unsetExecutorId).toBe(true)
    expect(update.isFinished).toBe(true)
    expect(update.unsetRunOutput).toBe(true)
    expect(update.unsetError).toBeUndefined()
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.finishedAt).toBe(now)
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle waiting_for_children update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'waiting_for_children',
    })
    expect(update.unsetExecutorId).toBe(true)
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBe(true)
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle waiting_for_finalize update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'waiting_for_finalize',
    })
    expect(update.unsetExecutorId).toBe(true)
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBe(true)
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle finalize_failed update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'finalize_failed',
    })
    expect(update.unsetExecutorId).toBe(true)
    expect(update.isFinished).toBe(true)
    expect(update.unsetRunOutput).toBe(true)
    expect(update.unsetError).toBeUndefined()
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.finishedAt).toBe(now)
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle completed update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'completed',
    })
    expect(update.unsetExecutorId).toBe(true)
    expect(update.isFinished).toBe(true)
    expect(update.unsetRunOutput).toBe(true)
    expect(update.unsetError).toBe(true)
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.finishedAt).toBe(now)
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle cancelled update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'cancelled',
    })
    expect(update.unsetExecutorId).toBe(true)
    expect(update.isFinished).toBe(true)
    expect(update.unsetRunOutput).toBe(true)
    expect(update.unsetError).toBeUndefined()
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.finishedAt).toBe(now)
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle on_children_finished_processing_status idle update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      onChildrenFinishedProcessingStatus: 'idle',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBe(true)
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle on_children_finished_processing_status processing update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      onChildrenFinishedProcessingStatus: 'processing',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle on_children_finished_processing_status processed update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      onChildrenFinishedProcessingStatus: 'processed',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBe(true)
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle close_status idle update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      closeStatus: 'idle',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBe(true)
    expect(update.updatedAt).toBe(now)
  })

  it('should handle close_status ready update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      closeStatus: 'ready',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBe(true)
    expect(update.updatedAt).toBe(now)
  })

  it('should handle close_status closing update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      closeStatus: 'closing',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle close_status closed update', () => {
    const now = new Date()
    const update = getTaskExecutionStorageUpdate(now, {
      closeStatus: 'closed',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBe(true)
    expect(update.updatedAt).toBe(now)
  })
})

describe('convertTaskExecutionStorageValueToTaskExecution', () => {
  let now: Date
  let serializer: SerializerInternal

  beforeEach(() => {
    now = new Date()
    serializer = new SerializerInternal(createSuperjsonSerializer())
  })

  it('should handle ready execution', () => {
    const taskExecutionStorageValue: TaskExecutionStorageValue = {
      root: {
        taskId: 'rootTaskId',
        executionId: 'rootExecutionId',
      },
      parent: {
        taskId: 'parentTaskId',
        executionId: 'parentExecutionId',
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      input: serializer.serialize({ name: 'test' }),
      executorId: 'de_executor_id',
      status: 'ready',
      isFinished: false,
      retryAttempts: 0,
      startAt: now,
      activeChildrenCount: 0,
      onChildrenFinishedProcessingStatus: 'idle',
      closeStatus: 'idle',
      needsPromiseCancellation: false,
      createdAt: now,
      updatedAt: now,
    }

    const taskExecution = convertTaskExecutionStorageValueToTaskExecution(
      taskExecutionStorageValue,
      serializer,
    )
    expect(taskExecution.root).toEqual({
      taskId: 'rootTaskId',
      executionId: 'rootExecutionId',
    })
    expect(taskExecution.parent).toEqual({
      taskId: 'parentTaskId',
      executionId: 'parentExecutionId',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    })
    expect(taskExecution.taskId).toBe('taskId')
    expect(taskExecution.executionId).toBe('executionId')
    expect(taskExecution.retryOptions).toEqual({
      maxAttempts: 1,
    })
    expect(taskExecution.sleepMsBeforeRun).toBe(0)
    expect(taskExecution.timeoutMs).toBe(0)
    expect(taskExecution.input).toEqual({ name: 'test' })
    expect(taskExecution.status).toBe('ready')
    assert(taskExecution.status === 'ready')
    expect(taskExecution.error).toBeUndefined()
    expect(taskExecution.retryAttempts).toBe(0)
    expect(taskExecution.createdAt).toBe(now)
    expect(taskExecution.updatedAt).toBe(now)
  })

  it('should handle running execution', () => {
    const taskExecutionStorageValue: TaskExecutionStorageValue = {
      root: {
        taskId: 'rootTaskId',
        executionId: 'rootExecutionId',
      },
      parent: {
        taskId: 'parentTaskId',
        executionId: 'parentExecutionId',
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      input: serializer.serialize({ name: 'test' }),
      executorId: 'de_executor_id',
      status: 'running',
      isFinished: false,
      retryAttempts: 0,
      startAt: now,
      activeChildrenCount: 0,
      onChildrenFinishedProcessingStatus: 'idle',
      closeStatus: 'idle',
      needsPromiseCancellation: false,
      createdAt: now,
      updatedAt: now,

      startedAt: now,
      expiresAt: now,
    }

    const taskExecution = convertTaskExecutionStorageValueToTaskExecution(
      taskExecutionStorageValue,
      serializer,
    )
    expect(taskExecution.root).toEqual({
      taskId: 'rootTaskId',
      executionId: 'rootExecutionId',
    })
    expect(taskExecution.parent).toEqual({
      taskId: 'parentTaskId',
      executionId: 'parentExecutionId',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    })
    expect(taskExecution.taskId).toBe('taskId')
    expect(taskExecution.executionId).toBe('executionId')
    expect(taskExecution.retryOptions).toEqual({
      maxAttempts: 1,
    })
    expect(taskExecution.sleepMsBeforeRun).toBe(0)
    expect(taskExecution.timeoutMs).toBe(0)
    expect(taskExecution.input).toEqual({ name: 'test' })
    expect(taskExecution.status).toBe('running')
    assert(taskExecution.status === 'running')
    expect(taskExecution.error).toBeUndefined()
    expect(taskExecution.retryAttempts).toBe(0)
    expect(taskExecution.createdAt).toBe(now)
    expect(taskExecution.updatedAt).toBe(now)
    expect(taskExecution.startedAt).toBe(now)
    expect(taskExecution.expiresAt).toBe(now)
  })

  it('should handle failed execution', () => {
    const taskExecutionStorageValue: TaskExecutionStorageValue = {
      root: {
        taskId: 'rootTaskId',
        executionId: 'rootExecutionId',
      },
      parent: {
        taskId: 'parentTaskId',
        executionId: 'parentExecutionId',
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      input: serializer.serialize({ name: 'test' }),
      executorId: 'de_executor_id',
      status: 'failed',
      isFinished: true,
      retryAttempts: 0,
      startAt: now,
      activeChildrenCount: 0,
      onChildrenFinishedProcessingStatus: 'idle',
      closeStatus: 'idle',
      needsPromiseCancellation: false,
      createdAt: now,
      updatedAt: now,

      startedAt: now,
      expiresAt: now,
      error: {
        message: 'test_error_message',
        errorType: 'generic',
        isRetryable: true,
        isInternal: false,
      },
      finishedAt: now,
    }

    const taskExecution = convertTaskExecutionStorageValueToTaskExecution(
      taskExecutionStorageValue,
      serializer,
    )
    expect(taskExecution.root).toEqual({
      taskId: 'rootTaskId',
      executionId: 'rootExecutionId',
    })
    expect(taskExecution.parent).toEqual({
      taskId: 'parentTaskId',
      executionId: 'parentExecutionId',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    })
    expect(taskExecution.taskId).toBe('taskId')
    expect(taskExecution.executionId).toBe('executionId')
    expect(taskExecution.retryOptions).toEqual({
      maxAttempts: 1,
    })
    expect(taskExecution.sleepMsBeforeRun).toBe(0)
    expect(taskExecution.timeoutMs).toBe(0)
    expect(taskExecution.input).toEqual({ name: 'test' })
    expect(taskExecution.status).toBe('failed')
    assert(taskExecution.status === 'failed')
    expect(taskExecution.error).toEqual({
      message: 'test_error_message',
      errorType: 'generic',
      isRetryable: true,
      isInternal: false,
    })
    expect(taskExecution.retryAttempts).toBe(0)
    expect(taskExecution.createdAt).toBe(now)
    expect(taskExecution.updatedAt).toBe(now)
    expect(taskExecution.startedAt).toBe(now)
    expect(taskExecution.expiresAt).toBe(now)
    expect(taskExecution.finishedAt).toBe(now)
  })

  it('should handle failed execution', () => {
    const taskExecutionStorageValue: TaskExecutionStorageValue = {
      root: {
        taskId: 'rootTaskId',
        executionId: 'rootExecutionId',
      },
      parent: {
        taskId: 'parentTaskId',
        executionId: 'parentExecutionId',
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      input: serializer.serialize({ name: 'test' }),
      executorId: 'de_executor_id',
      status: 'timed_out',
      isFinished: true,
      retryAttempts: 0,
      startAt: now,
      activeChildrenCount: 0,
      onChildrenFinishedProcessingStatus: 'idle',
      closeStatus: 'idle',
      needsPromiseCancellation: false,
      createdAt: now,
      updatedAt: now,

      startedAt: now,
      expiresAt: now,
      error: {
        message: 'test_error_message',
        errorType: 'timed_out',
        isRetryable: true,
        isInternal: false,
      },
      finishedAt: now,
    }

    const taskExecution = convertTaskExecutionStorageValueToTaskExecution(
      taskExecutionStorageValue,
      serializer,
    )
    expect(taskExecution.root).toEqual({
      taskId: 'rootTaskId',
      executionId: 'rootExecutionId',
    })
    expect(taskExecution.parent).toEqual({
      taskId: 'parentTaskId',
      executionId: 'parentExecutionId',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    })
    expect(taskExecution.taskId).toBe('taskId')
    expect(taskExecution.executionId).toBe('executionId')
    expect(taskExecution.retryOptions).toEqual({
      maxAttempts: 1,
    })
    expect(taskExecution.sleepMsBeforeRun).toBe(0)
    expect(taskExecution.timeoutMs).toBe(0)
    expect(taskExecution.input).toEqual({ name: 'test' })
    expect(taskExecution.status).toBe('timed_out')
    assert(taskExecution.status === 'timed_out')
    expect(taskExecution.error).toEqual({
      message: 'test_error_message',
      errorType: 'timed_out',
      isRetryable: true,
      isInternal: false,
    })
    expect(taskExecution.retryAttempts).toBe(0)
    expect(taskExecution.createdAt).toBe(now)
    expect(taskExecution.updatedAt).toBe(now)
    expect(taskExecution.startedAt).toBe(now)
    expect(taskExecution.expiresAt).toBe(now)
    expect(taskExecution.finishedAt).toBe(now)
  })

  it('should handle waiting_for_children execution', () => {
    const taskExecutionStorageValue: TaskExecutionStorageValue = {
      root: {
        taskId: 'rootTaskId',
        executionId: 'rootExecutionId',
      },
      parent: {
        taskId: 'parentTaskId',
        executionId: 'parentExecutionId',
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      input: serializer.serialize({ name: 'test' }),
      executorId: 'de_executor_id',
      status: 'waiting_for_children',
      isFinished: false,
      retryAttempts: 0,
      startAt: now,
      activeChildrenCount: 1,
      onChildrenFinishedProcessingStatus: 'idle',
      closeStatus: 'idle',
      needsPromiseCancellation: false,
      createdAt: now,
      updatedAt: now,

      startedAt: now,
      expiresAt: now,
      children: [
        {
          taskId: 'childTaskId',
          executionId: 'childExecutionId',
        },
      ],
    }

    const taskExecution = convertTaskExecutionStorageValueToTaskExecution(
      taskExecutionStorageValue,
      serializer,
    )
    expect(taskExecution.root).toEqual({
      taskId: 'rootTaskId',
      executionId: 'rootExecutionId',
    })
    expect(taskExecution.parent).toEqual({
      taskId: 'parentTaskId',
      executionId: 'parentExecutionId',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    })
    expect(taskExecution.taskId).toBe('taskId')
    expect(taskExecution.executionId).toBe('executionId')
    expect(taskExecution.retryOptions).toEqual({
      maxAttempts: 1,
    })
    expect(taskExecution.sleepMsBeforeRun).toBe(0)
    expect(taskExecution.timeoutMs).toBe(0)
    expect(taskExecution.input).toEqual({ name: 'test' })
    expect(taskExecution.status).toBe('waiting_for_children')
    assert(taskExecution.status === 'waiting_for_children')
    expect(taskExecution.retryAttempts).toBe(0)
    expect(taskExecution.createdAt).toBe(now)
    expect(taskExecution.updatedAt).toBe(now)
    expect(taskExecution.startedAt).toBe(now)
    expect(taskExecution.expiresAt).toBe(now)
    expect(taskExecution.children).toEqual([
      {
        taskId: 'childTaskId',
        executionId: 'childExecutionId',
      },
    ])
    expect(taskExecution.activeChildrenCount).toBe(1)
  })

  it('should handle waiting_for_finalize execution', () => {
    const taskExecutionStorageValue: TaskExecutionStorageValue = {
      root: {
        taskId: 'rootTaskId',
        executionId: 'rootExecutionId',
      },
      parent: {
        taskId: 'parentTaskId',
        executionId: 'parentExecutionId',
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      input: serializer.serialize({ name: 'test' }),
      executorId: 'de_executor_id',
      status: 'waiting_for_finalize',
      isFinished: false,
      retryAttempts: 0,
      startAt: now,
      activeChildrenCount: 0,
      onChildrenFinishedProcessingStatus: 'idle',
      closeStatus: 'idle',
      needsPromiseCancellation: false,
      createdAt: now,
      updatedAt: now,

      startedAt: now,
      expiresAt: now,
      children: [
        {
          taskId: 'childTaskId',
          executionId: 'childExecutionId',
        },
      ],
      finalize: {
        taskId: 'finalizeTaskId',
        executionId: 'finalizeExecutionId',
      },
    }

    const taskExecution = convertTaskExecutionStorageValueToTaskExecution(
      taskExecutionStorageValue,
      serializer,
    )
    expect(taskExecution.root).toEqual({
      taskId: 'rootTaskId',
      executionId: 'rootExecutionId',
    })
    expect(taskExecution.parent).toEqual({
      taskId: 'parentTaskId',
      executionId: 'parentExecutionId',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    })
    expect(taskExecution.taskId).toBe('taskId')
    expect(taskExecution.executionId).toBe('executionId')
    expect(taskExecution.retryOptions).toEqual({
      maxAttempts: 1,
    })
    expect(taskExecution.sleepMsBeforeRun).toBe(0)
    expect(taskExecution.timeoutMs).toBe(0)
    expect(taskExecution.input).toEqual({ name: 'test' })
    expect(taskExecution.status).toBe('waiting_for_finalize')
    assert(taskExecution.status === 'waiting_for_finalize')
    expect(taskExecution.retryAttempts).toBe(0)
    expect(taskExecution.createdAt).toBe(now)
    expect(taskExecution.updatedAt).toBe(now)
    expect(taskExecution.startedAt).toBe(now)
    expect(taskExecution.expiresAt).toBe(now)
    expect(taskExecution.children).toEqual([
      {
        taskId: 'childTaskId',
        executionId: 'childExecutionId',
      },
    ])
    expect(taskExecution.activeChildrenCount).toBe(0)
    expect(taskExecution.finalize).toEqual({
      taskId: 'finalizeTaskId',
      executionId: 'finalizeExecutionId',
    })
  })

  it('should handle finalize_failed execution', () => {
    const taskExecutionStorageValue: TaskExecutionStorageValue = {
      root: {
        taskId: 'rootTaskId',
        executionId: 'rootExecutionId',
      },
      parent: {
        taskId: 'parentTaskId',
        executionId: 'parentExecutionId',
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      input: serializer.serialize({ name: 'test' }),
      executorId: 'de_executor_id',
      status: 'finalize_failed',
      isFinished: true,
      retryAttempts: 0,
      startAt: now,
      activeChildrenCount: 0,
      onChildrenFinishedProcessingStatus: 'idle',
      closeStatus: 'idle',
      needsPromiseCancellation: false,
      createdAt: now,
      updatedAt: now,

      startedAt: now,
      expiresAt: now,
      children: [
        {
          taskId: 'childTaskId',
          executionId: 'childExecutionId',
        },
      ],
      finalize: {
        taskId: 'finalizeTaskId',
        executionId: 'finalizeExecutionId',
      },
      error: {
        message: 'test_error_message',
        errorType: 'generic',
        isRetryable: true,
        isInternal: false,
      },
      finishedAt: now,
    }

    const taskExecution = convertTaskExecutionStorageValueToTaskExecution(
      taskExecutionStorageValue,
      serializer,
    )
    expect(taskExecution.root).toEqual({
      taskId: 'rootTaskId',
      executionId: 'rootExecutionId',
    })
    expect(taskExecution.parent).toEqual({
      taskId: 'parentTaskId',
      executionId: 'parentExecutionId',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    })
    expect(taskExecution.taskId).toBe('taskId')
    expect(taskExecution.executionId).toBe('executionId')
    expect(taskExecution.retryOptions).toEqual({
      maxAttempts: 1,
    })
    expect(taskExecution.sleepMsBeforeRun).toBe(0)
    expect(taskExecution.timeoutMs).toBe(0)
    expect(taskExecution.input).toEqual({ name: 'test' })
    expect(taskExecution.status).toBe('finalize_failed')
    assert(taskExecution.status === 'finalize_failed')
    expect(taskExecution.error).toEqual({
      message: 'test_error_message',
      errorType: 'generic',
      isRetryable: true,
      isInternal: false,
    })
    expect(taskExecution.retryAttempts).toBe(0)
    expect(taskExecution.createdAt).toBe(now)
    expect(taskExecution.updatedAt).toBe(now)
    expect(taskExecution.startedAt).toBe(now)
    expect(taskExecution.expiresAt).toBe(now)
    expect(taskExecution.children).toEqual([
      {
        taskId: 'childTaskId',
        executionId: 'childExecutionId',
      },
    ])
    expect(taskExecution.activeChildrenCount).toBe(0)
    expect(taskExecution.finalize).toEqual({
      taskId: 'finalizeTaskId',
      executionId: 'finalizeExecutionId',
    })
    expect(taskExecution.finishedAt).toBe(now)
  })

  it('should handle completed execution', () => {
    const taskExecutionStorageValue: TaskExecutionStorageValue = {
      root: {
        taskId: 'rootTaskId',
        executionId: 'rootExecutionId',
      },
      parent: {
        taskId: 'parentTaskId',
        executionId: 'parentExecutionId',
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      input: serializer.serialize({ name: 'test' }),
      executorId: 'de_executor_id',
      status: 'completed',
      isFinished: true,
      retryAttempts: 0,
      startAt: now,
      activeChildrenCount: 0,
      onChildrenFinishedProcessingStatus: 'idle',
      closeStatus: 'idle',
      needsPromiseCancellation: false,
      createdAt: now,
      updatedAt: now,

      startedAt: now,
      expiresAt: now,
      children: [
        {
          taskId: 'childTaskId',
          executionId: 'childExecutionId',
        },
      ],
      finalize: {
        taskId: 'finalizeTaskId',
        executionId: 'finalizeExecutionId',
      },
      output: serializer.serialize({ output_name: 'test_output' }),
      finishedAt: now,
    }

    const taskExecution = convertTaskExecutionStorageValueToTaskExecution(
      taskExecutionStorageValue,
      serializer,
    )
    expect(taskExecution.root).toEqual({
      taskId: 'rootTaskId',
      executionId: 'rootExecutionId',
    })
    expect(taskExecution.parent).toEqual({
      taskId: 'parentTaskId',
      executionId: 'parentExecutionId',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    })
    expect(taskExecution.taskId).toBe('taskId')
    expect(taskExecution.executionId).toBe('executionId')
    expect(taskExecution.retryOptions).toEqual({
      maxAttempts: 1,
    })
    expect(taskExecution.sleepMsBeforeRun).toBe(0)
    expect(taskExecution.timeoutMs).toBe(0)
    expect(taskExecution.input).toEqual({ name: 'test' })
    expect(taskExecution.status).toBe('completed')
    assert(taskExecution.status === 'completed')
    expect(taskExecution.retryAttempts).toBe(0)
    expect(taskExecution.createdAt).toBe(now)
    expect(taskExecution.updatedAt).toBe(now)
    expect(taskExecution.startedAt).toBe(now)
    expect(taskExecution.expiresAt).toBe(now)
    expect(taskExecution.children).toEqual([
      {
        taskId: 'childTaskId',
        executionId: 'childExecutionId',
      },
    ])
    expect(taskExecution.activeChildrenCount).toBe(0)
    expect(taskExecution.finalize).toEqual({
      taskId: 'finalizeTaskId',
      executionId: 'finalizeExecutionId',
    })
    expect(taskExecution.output).toEqual({ output_name: 'test_output' })
    expect(taskExecution.finishedAt).toBe(now)
  })

  it('should handle cancelled execution', () => {
    const taskExecutionStorageValue: TaskExecutionStorageValue = {
      root: {
        taskId: 'rootTaskId',
        executionId: 'rootExecutionId',
      },
      parent: {
        taskId: 'parentTaskId',
        executionId: 'parentExecutionId',
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      input: serializer.serialize({ name: 'test' }),
      executorId: 'de_executor_id',
      status: 'cancelled',
      isFinished: true,
      retryAttempts: 0,
      startAt: now,
      activeChildrenCount: 0,
      onChildrenFinishedProcessingStatus: 'idle',
      closeStatus: 'idle',
      needsPromiseCancellation: false,
      createdAt: now,
      updatedAt: now,

      startedAt: now,
      expiresAt: now,
      children: [
        {
          taskId: 'childTaskId',
          executionId: 'childExecutionId',
        },
      ],
      finalize: {
        taskId: 'finalizeTaskId',
        executionId: 'finalizeExecutionId',
      },
      error: {
        message: 'test_error_message',
        errorType: 'cancelled',
        isRetryable: false,
        isInternal: false,
      },
      finishedAt: now,
    }

    const taskExecution = convertTaskExecutionStorageValueToTaskExecution(
      taskExecutionStorageValue,
      serializer,
    )
    expect(taskExecution.root).toEqual({
      taskId: 'rootTaskId',
      executionId: 'rootExecutionId',
    })
    expect(taskExecution.parent).toEqual({
      taskId: 'parentTaskId',
      executionId: 'parentExecutionId',
      indexInParentChildTaskExecutions: 0,
      isFinalizeTaskOfParentTask: false,
    })
    expect(taskExecution.taskId).toBe('taskId')
    expect(taskExecution.executionId).toBe('executionId')
    expect(taskExecution.retryOptions).toEqual({
      maxAttempts: 1,
    })
    expect(taskExecution.sleepMsBeforeRun).toBe(0)
    expect(taskExecution.timeoutMs).toBe(0)
    expect(taskExecution.input).toEqual({ name: 'test' })
    expect(taskExecution.status).toBe('cancelled')
    assert(taskExecution.status === 'cancelled')
    expect(taskExecution.error).toEqual({
      message: 'test_error_message',
      errorType: 'cancelled',
      isRetryable: false,
      isInternal: false,
    })
    expect(taskExecution.retryAttempts).toBe(0)
    expect(taskExecution.createdAt).toBe(now)
    expect(taskExecution.updatedAt).toBe(now)
    expect(taskExecution.startedAt).toBe(now)
    expect(taskExecution.expiresAt).toBe(now)
    expect(taskExecution.children).toEqual([
      {
        taskId: 'childTaskId',
        executionId: 'childExecutionId',
      },
    ])
    expect(taskExecution.activeChildrenCount).toBe(0)
    expect(taskExecution.finalize).toEqual({
      taskId: 'finalizeTaskId',
      executionId: 'finalizeExecutionId',
    })
    expect(taskExecution.finishedAt).toBe(now)
  })

  it('should handle invalid execution status', () => {
    const taskExecutionStorageValue: TaskExecutionStorageValue = {
      root: {
        taskId: 'rootTaskId',
        executionId: 'rootExecutionId',
      },
      parent: {
        taskId: 'parentTaskId',
        executionId: 'parentExecutionId',
        indexInParentChildTaskExecutions: 0,
        isFinalizeTaskOfParentTask: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      input: serializer.serialize({ name: 'test' }),
      // @ts-expect-error - Testing invalid input
      status: 'invalid_status',
      retryAttempts: 0,
      startAt: now,
      activeChildrenCount: 0,
      onChildrenFinishedProcessingStatus: 'idle',
      closeStatus: 'idle',
      needsPromiseCancellation: false,
      createdAt: now,
      updatedAt: now,
    }

    expect(() =>
      convertTaskExecutionStorageValueToTaskExecution(taskExecutionStorageValue, serializer),
    ).toThrow('Invalid task execution status: invalid_status')
  })
})

describe('TaskExecutionsStorageWithMutex', () => {
  it('should handle all methods', async () => {
    let executionCount = 0
    const testStorage = {
      insertMany: () => {
        executionCount++
      },
      getById: () => {
        executionCount++
        return undefined
      },
      getBySleepingTaskUniqueId: () => {
        executionCount++
        return undefined
      },
      updateById: () => {
        executionCount++
      },
      updateByIdAndInsertManyIfUpdated: () => {
        executionCount++
      },
      updateByStatusAndStartAtLessThanAndReturn: () => {
        executionCount++
        return []
      },
      updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn:
        () => {
          executionCount++
          return []
        },
      updateByCloseStatusAndReturn: () => {
        executionCount++
        return []
      },
      updateByIsSleepingTaskAndExpiresAtLessThanAndReturn: () => {
        executionCount++
        return []
      },
      updateByOnChildrenFinishedProcessingExpiresAtLessThanAndReturn: () => {
        executionCount++
        return []
      },
      updateByCloseExpiresAtLessThanAndReturn: () => {
        executionCount++
        return []
      },
      updateByExecutorIdAndNeedsPromiseCancellationAndReturn: () => {
        executionCount++
        return []
      },
      getByParentExecutionId: () => {
        executionCount++
        return []
      },
      updateByParentExecutionIdAndIsFinished: () => {
        executionCount++
      },
      updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus: () => {
        executionCount++
        return 0
      },
      deleteById: () => {
        executionCount++
      },
      deleteAll: () => {
        executionCount++
      },
    } satisfies TaskExecutionsStorage

    const now = new Date()
    const storage = new TaskExecutionsStorageWithMutex(testStorage)

    await storage.insertMany([])
    expect(executionCount).toBe(1)

    await storage.getById('executionId', {})
    expect(executionCount).toBe(2)

    await storage.getBySleepingTaskUniqueId('sleepingTaskUniqueId')
    expect(executionCount).toBe(3)

    await storage.updateById(
      'executionId',
      {},
      {
        updatedAt: new Date(),
      },
    )
    expect(executionCount).toBe(4)

    await storage.updateByIdAndInsertManyIfUpdated(
      'executionId',
      {},
      {
        updatedAt: now,
      },
      [],
    )
    expect(executionCount).toBe(5)

    await storage.updateByStatusAndStartAtLessThanAndReturn(
      'completed',
      new Date(),
      {
        updatedAt: now,
      },
      new Date(),
      0,
    )
    expect(executionCount).toBe(6)

    await storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn(
      'completed',
      'idle',
      1,
      {
        updatedAt: now,
      },
      1,
    )
    expect(executionCount).toBe(7)

    await storage.updateByCloseStatusAndReturn(
      'idle',
      {
        updatedAt: now,
      },
      1,
    )
    expect(executionCount).toBe(8)

    await storage.updateByIsSleepingTaskAndExpiresAtLessThanAndReturn(
      false,
      new Date(),
      {
        updatedAt: now,
      },
      1,
    )
    expect(executionCount).toBe(9)

    await storage.updateByOnChildrenFinishedProcessingExpiresAtLessThanAndReturn(
      new Date(),
      {
        updatedAt: now,
      },
      1,
    )
    expect(executionCount).toBe(10)

    await storage.updateByCloseExpiresAtLessThanAndReturn(
      new Date(),
      {
        updatedAt: now,
      },
      1,
    )
    expect(executionCount).toBe(11)

    await storage.updateByExecutorIdAndNeedsPromiseCancellationAndReturn(
      'de_executor_id',
      true,
      {
        updatedAt: now,
      },
      1,
    )
    expect(executionCount).toBe(12)

    await storage.getByParentExecutionId('executionId')
    expect(executionCount).toBe(13)

    await storage.updateByParentExecutionIdAndIsFinished('executionId', true, {
      updatedAt: now,
    })
    expect(executionCount).toBe(14)

    await storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
      true,
      'idle',
      {
        updatedAt: now,
      },
      1,
    )
    expect(executionCount).toBe(15)

    await storage.deleteById('executionId')
    expect(executionCount).toBe(16)

    await storage.deleteAll()
    expect(executionCount).toBe(17)
  })
})
