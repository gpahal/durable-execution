import { createSuperjsonSerializer, SerializerInternal } from '../src/serializer'
import {
  convertTaskExecutionStorageValueToTaskExecution,
  TaskExecutionsStorageWithMutex,
  type TaskExecutionsStorage,
  type TaskExecutionStorageValue,
} from '../src/storage'
import {
  getTaskExecutionStorageUpdate,
  validateStorageMaxRetryAttempts,
} from '../src/storage-internal'

describe('validateStorageMaxRetryAttempts', () => {
  it('should handle valid max retry attempts', () => {
    expect(validateStorageMaxRetryAttempts(0)).toBe(0)
    expect(validateStorageMaxRetryAttempts(1)).toBe(1)
    expect(validateStorageMaxRetryAttempts(10)).toBe(10)
  })

  it('should handle undefined or null max retry attempts', () => {
    expect(validateStorageMaxRetryAttempts(undefined)).toBe(1)
    expect(validateStorageMaxRetryAttempts(null)).toBe(1)
  })

  it('should handle invalid max retry attempts', () => {
    expect(() => validateStorageMaxRetryAttempts(-1)).toThrow('Invalid storage max retry attempts')
    expect(() => validateStorageMaxRetryAttempts(11)).toThrow('Invalid storage max retry attempts')
  })
})

describe('getTaskExecutionStorageUpdate', () => {
  it('should handle empty update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {})
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle ready update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'ready',
    })
    expect(update.unsetExecutorId).toBe(true)
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBe(true)
    expect(update.unsetError).toBeUndefined()
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBe(true)
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle running update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'running',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.startedAt).toBe(now)
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle failed update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'failed',
    })
    expect(update.unsetExecutorId).toBe(true)
    expect(update.isFinished).toBe(true)
    expect(update.unsetRunOutput).toBe(true)
    expect(update.unsetError).toBeUndefined()
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBe(now)
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle timed_out update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'timed_out',
    })
    expect(update.unsetExecutorId).toBe(true)
    expect(update.isFinished).toBe(true)
    expect(update.unsetRunOutput).toBe(true)
    expect(update.unsetError).toBeUndefined()
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBe(now)
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle waiting_for_children update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'waiting_for_children',
    })
    expect(update.unsetExecutorId).toBe(true)
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBe(true)
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.waitingForChildrenStartedAt).toBe(now)
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle waiting_for_finalize update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'waiting_for_finalize',
    })
    expect(update.unsetExecutorId).toBe(true)
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBe(true)
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBe(now)
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle finalize_failed update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'finalize_failed',
    })
    expect(update.unsetExecutorId).toBe(true)
    expect(update.isFinished).toBe(true)
    expect(update.unsetRunOutput).toBe(true)
    expect(update.unsetError).toBeUndefined()
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBe(now)
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle completed update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'completed',
    })
    expect(update.unsetExecutorId).toBe(true)
    expect(update.isFinished).toBe(true)
    expect(update.unsetRunOutput).toBe(true)
    expect(update.unsetError).toBe(true)
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBe(now)
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle cancelled update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      status: 'cancelled',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBe(true)
    expect(update.unsetRunOutput).toBe(true)
    expect(update.unsetError).toBeUndefined()
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBe(true)
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBe(now)
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle on_children_finished_processing_status idle update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      onChildrenFinishedProcessingStatus: 'idle',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBe(true)
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle on_children_finished_processing_status processing update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      onChildrenFinishedProcessingStatus: 'processing',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle on_children_finished_processing_status processed update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      onChildrenFinishedProcessingStatus: 'processed',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBe(true)
    expect(update.onChildrenFinishedProcessingFinishedAt).toBe(now)
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle close_status idle update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      closeStatus: 'idle',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBe(true)
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle close_status ready update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      closeStatus: 'ready',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBe(true)
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle close_status closing update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      closeStatus: 'closing',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBeUndefined()
    expect(update.closedAt).toBeUndefined()
    expect(update.updatedAt).toBe(now)
  })

  it('should handle close_status closed update', () => {
    const now = Date.now()
    const update = getTaskExecutionStorageUpdate(now, {
      closeStatus: 'closed',
    })
    expect(update.unsetExecutorId).toBeUndefined()
    expect(update.isFinished).toBeUndefined()
    expect(update.unsetRunOutput).toBeUndefined()
    expect(update.unsetError).toBeUndefined()
    expect(update.startedAt).toBeUndefined()
    expect(update.unsetStartedAt).toBeUndefined()
    expect(update.unsetExpiresAt).toBeUndefined()
    expect(update.waitingForChildrenStartedAt).toBeUndefined()
    expect(update.waitingForFinalizeStartedAt).toBeUndefined()
    expect(update.finishedAt).toBeUndefined()
    expect(update.unsetOnChildrenFinishedProcessingExpiresAt).toBeUndefined()
    expect(update.onChildrenFinishedProcessingFinishedAt).toBeUndefined()
    expect(update.unsetCloseExpiresAt).toBe(true)
    expect(update.closedAt).toBe(now)
    expect(update.updatedAt).toBe(now)
  })
})

describe('convertTaskExecutionStorageValueToTaskExecution', () => {
  let now: number
  let nowDate: Date
  let serializer: SerializerInternal

  beforeEach(() => {
    now = Date.now()
    nowDate = new Date(now)
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
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      areChildrenSequential: false,
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
      indexInParentChildren: 0,
      isOnlyChildOfParent: false,
      isFinalizeOfParent: false,
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
    expect(taskExecution.createdAt).toStrictEqual(nowDate)
    expect(taskExecution.updatedAt).toStrictEqual(nowDate)
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
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      areChildrenSequential: false,
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
      indexInParentChildren: 0,
      isOnlyChildOfParent: false,
      isFinalizeOfParent: false,
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
    expect(taskExecution.createdAt).toStrictEqual(nowDate)
    expect(taskExecution.updatedAt).toStrictEqual(nowDate)
    expect(taskExecution.startedAt).toStrictEqual(nowDate)
    expect(taskExecution.expiresAt).toStrictEqual(nowDate)
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
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      areChildrenSequential: false,
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
      indexInParentChildren: 0,
      isOnlyChildOfParent: false,
      isFinalizeOfParent: false,
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
    expect(taskExecution.createdAt).toStrictEqual(nowDate)
    expect(taskExecution.updatedAt).toStrictEqual(nowDate)
    expect(taskExecution.startedAt).toStrictEqual(nowDate)
    expect(taskExecution.expiresAt).toStrictEqual(nowDate)
    expect(taskExecution.finishedAt).toStrictEqual(nowDate)
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
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      areChildrenSequential: false,
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
      indexInParentChildren: 0,
      isOnlyChildOfParent: false,
      isFinalizeOfParent: false,
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
    expect(taskExecution.createdAt).toStrictEqual(nowDate)
    expect(taskExecution.updatedAt).toStrictEqual(nowDate)
    expect(taskExecution.startedAt).toStrictEqual(nowDate)
    expect(taskExecution.expiresAt).toStrictEqual(nowDate)
    expect(taskExecution.finishedAt).toStrictEqual(nowDate)
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
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      areChildrenSequential: false,
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
      indexInParentChildren: 0,
      isOnlyChildOfParent: false,
      isFinalizeOfParent: false,
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
    expect(taskExecution.createdAt).toStrictEqual(nowDate)
    expect(taskExecution.updatedAt).toStrictEqual(nowDate)
    expect(taskExecution.startedAt).toStrictEqual(nowDate)
    expect(taskExecution.expiresAt).toStrictEqual(nowDate)
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
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      areChildrenSequential: false,
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
      indexInParentChildren: 0,
      isOnlyChildOfParent: false,
      isFinalizeOfParent: false,
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
    expect(taskExecution.createdAt).toStrictEqual(nowDate)
    expect(taskExecution.updatedAt).toStrictEqual(nowDate)
    expect(taskExecution.startedAt).toStrictEqual(nowDate)
    expect(taskExecution.expiresAt).toStrictEqual(nowDate)
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
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      areChildrenSequential: false,
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
      indexInParentChildren: 0,
      isOnlyChildOfParent: false,
      isFinalizeOfParent: false,
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
    expect(taskExecution.createdAt).toStrictEqual(nowDate)
    expect(taskExecution.updatedAt).toStrictEqual(nowDate)
    expect(taskExecution.startedAt).toStrictEqual(nowDate)
    expect(taskExecution.expiresAt).toStrictEqual(nowDate)
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
    expect(taskExecution.finishedAt).toStrictEqual(nowDate)
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
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      areChildrenSequential: false,
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
      indexInParentChildren: 0,
      isOnlyChildOfParent: false,
      isFinalizeOfParent: false,
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
    expect(taskExecution.createdAt).toStrictEqual(nowDate)
    expect(taskExecution.updatedAt).toStrictEqual(nowDate)
    expect(taskExecution.startedAt).toStrictEqual(nowDate)
    expect(taskExecution.expiresAt).toStrictEqual(nowDate)
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
    expect(taskExecution.finishedAt).toStrictEqual(nowDate)
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
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      areChildrenSequential: false,
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
      indexInParentChildren: 0,
      isOnlyChildOfParent: false,
      isFinalizeOfParent: false,
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
    expect(taskExecution.createdAt).toStrictEqual(nowDate)
    expect(taskExecution.updatedAt).toStrictEqual(nowDate)
    expect(taskExecution.startedAt).toStrictEqual(nowDate)
    expect(taskExecution.expiresAt).toStrictEqual(nowDate)
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
    expect(taskExecution.finishedAt).toStrictEqual(nowDate)
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
        indexInParentChildren: 0,
        isOnlyChildOfParent: false,
        isFinalizeOfParent: false,
      },
      taskId: 'taskId',
      executionId: 'executionId',
      isSleepingTask: false,
      retryOptions: {
        maxAttempts: 1,
      },
      sleepMsBeforeRun: 0,
      timeoutMs: 0,
      areChildrenSequential: false,
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
      getManyById: () => {
        executionCount++
        return []
      },
      getManyBySleepingTaskUniqueId: () => {
        executionCount++
        return []
      },
      updateManyById: () => {
        executionCount++
      },
      updateManyByIdAndInsertChildrenIfUpdated: () => {
        executionCount++
      },
      updateByStatusAndStartAtLessThanAndReturn: () => {
        executionCount++
        return []
      },
      updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn:
        () => {
          executionCount++
          return []
        },
      updateByCloseStatusAndReturn: () => {
        executionCount++
        return []
      },
      updateByStatusAndIsSleepingTaskAndExpiresAtLessThan: () => {
        executionCount++
        return 0
      },
      updateByOnChildrenFinishedProcessingExpiresAtLessThan: () => {
        executionCount++
        return 0
      },
      updateByCloseExpiresAtLessThan: () => {
        executionCount++
        return 0
      },
      updateByExecutorIdAndNeedsPromiseCancellationAndReturn: () => {
        executionCount++
        return []
      },
      getManyByParentExecutionId: () => {
        executionCount++
        return []
      },
      updateManyByParentExecutionIdAndIsFinished: () => {
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

    const storage = new TaskExecutionsStorageWithMutex(testStorage)

    await storage.insertMany([])
    expect(executionCount).toBe(1)

    await storage.getManyById([{ executionId: 'executionId', filters: {} }])
    expect(executionCount).toBe(2)

    await storage.getManyBySleepingTaskUniqueId([{ sleepingTaskUniqueId: 'sleepingTaskUniqueId' }])
    expect(executionCount).toBe(3)

    const now = Date.now()
    await storage.updateManyById([
      {
        executionId: 'executionId',
        filters: {},
        update: {
          updatedAt: now,
        },
      },
    ])
    expect(executionCount).toBe(4)

    await storage.updateManyByIdAndInsertChildrenIfUpdated([
      {
        executionId: 'executionId',
        filters: {},
        update: {
          updatedAt: now,
        },
        childrenTaskExecutionsToInsertIfAnyUpdated: [],
      },
    ])
    expect(executionCount).toBe(5)

    await storage.updateByStatusAndStartAtLessThanAndReturn({
      status: 'completed',
      startAtLessThan: now,
      update: {
        updatedAt: now,
      },
      updateExpiresAtWithStartedAt: now,
      limit: 0,
    })
    expect(executionCount).toBe(6)

    await storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
      {
        status: 'completed',
        onChildrenFinishedProcessingStatus: 'idle',
        update: {
          updatedAt: now,
        },
        limit: 1,
      },
    )
    expect(executionCount).toBe(7)

    await storage.updateByCloseStatusAndReturn({
      closeStatus: 'idle',
      update: {
        updatedAt: now,
      },
      limit: 1,
    })
    expect(executionCount).toBe(8)

    await storage.updateByStatusAndIsSleepingTaskAndExpiresAtLessThan({
      status: 'running',
      isSleepingTask: false,
      expiresAtLessThan: now,
      update: {
        updatedAt: now,
      },
      limit: 1,
    })
    expect(executionCount).toBe(9)

    await storage.updateByOnChildrenFinishedProcessingExpiresAtLessThan({
      onChildrenFinishedProcessingExpiresAtLessThan: now,
      update: {
        updatedAt: now,
      },
      limit: 1,
    })
    expect(executionCount).toBe(10)

    await storage.updateByCloseExpiresAtLessThan({
      closeExpiresAtLessThan: now,
      update: {
        updatedAt: now,
      },
      limit: 1,
    })
    expect(executionCount).toBe(11)

    await storage.updateByExecutorIdAndNeedsPromiseCancellationAndReturn({
      executorId: 'de_executor_id',
      needsPromiseCancellation: true,
      update: {
        updatedAt: now,
      },
      limit: 1,
    })
    expect(executionCount).toBe(12)

    await storage.getManyByParentExecutionId([{ parentExecutionId: 'executionId' }])
    expect(executionCount).toBe(13)

    await storage.updateManyByParentExecutionIdAndIsFinished([
      {
        parentExecutionId: 'executionId',
        isFinished: true,
        update: {
          updatedAt: now,
        },
      },
    ])
    expect(executionCount).toBe(14)

    await storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus({
      isFinished: true,
      closeStatus: 'idle',
      update: {
        updatedAt: now,
      },
      limit: 1,
    })
    expect(executionCount).toBe(15)

    await storage.deleteById({ executionId: 'executionId' })
    expect(executionCount).toBe(16)

    await storage.deleteAll()
    expect(executionCount).toBe(17)
  })
})
