import { convertDurableExecutionStorageComponentToPublicApiImpl } from 'durable-execution-storage-convex'

import { components } from './_generated/api'

export const {
  insertMany,
  getById,
  getManyById,
  getBySleepingTaskUniqueId,
  updateManyById,
  updateByIdAndInsertChildrenIfUpdated,
  updateManyByIdAndInsertChildrenIfUpdated,
  updateByStatusAndStartAtLessThanAndReturn,
  updateByStatusAndOCFPStatusAndACCZeroAndReturn,
  updateByCloseStatusAndReturn,
  updateByIsSleepingTaskAndExpiresAtLessThan,
  updateByOCFPExpiresAt,
  updateByCloseExpiresAt,
  updateByExecutorIdAndNPCAndReturn,
  getByParentExecutionId,
  updateByParentExecutionIdAndIsFinished,
  updateAndDecrementParentACCByIsFinishedAndCloseStatus,
  deleteById,
  deleteAll,
} = convertDurableExecutionStorageComponentToPublicApiImpl(
  components.taskExecutionsStorage,
  'SUPER_SECRET',
)
