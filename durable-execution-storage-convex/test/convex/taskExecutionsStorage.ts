import { convertDurableExecutionStorageComponentToPublicApiImpl } from 'durable-execution-storage-convex'

import { components } from './_generated/api'

export const {
  insertMany,
  getManyById,
  getManyBySleepingTaskUniqueId,
  updateManyById,
  updateManyByIdAndInsertChildrenIfUpdated,
  updateByStatusAndStartAtLessThanAndReturn,
  updateByStatusAndOCFPStatusAndACCZeroAndReturn,
  updateByCloseStatusAndReturn,
  updateByStatusAndIsSleepingTaskAndExpiresAtLessThan,
  updateByOCFPExpiresAt,
  updateByCloseExpiresAt,
  updateByExecutorIdAndNPCAndReturn,
  getManyByParentExecutionId,
  updateManyByParentExecutionIdAndIsFinished,
  updateAndDecrementParentACCByIsFinishedAndCloseStatus,
  deleteById,
  deleteAll,
} = convertDurableExecutionStorageComponentToPublicApiImpl(
  components.taskExecutionsStorage,
  'SUPER_SECRET',
)
