/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type * as taskExecutionsStorage from "../taskExecutionsStorage.js";

import type {
  ApiFromModules,
  FilterApi,
  FunctionReference,
} from "convex/server";

/**
 * A utility for referencing Convex functions in your app's API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = api.myModule.myFunction;
 * ```
 */
declare const fullApi: ApiFromModules<{
  taskExecutionsStorage: typeof taskExecutionsStorage;
}>;
declare const fullApiWithMounts: typeof fullApi;

export declare const api: FilterApi<
  typeof fullApiWithMounts,
  FunctionReference<any, "public">
>;
export declare const internal: FilterApi<
  typeof fullApiWithMounts,
  FunctionReference<any, "internal">
>;

export declare const components: {
  taskExecutionsStorage: {
    lib: {
      acquireLock: FunctionReference<
        "mutation",
        "internal",
        { key: string },
        any
      >;
      deleteAll: FunctionReference<"action", "internal", {}, any>;
      deleteById: FunctionReference<
        "mutation",
        "internal",
        { executionId: string },
        any
      >;
      getManyById: FunctionReference<
        "query",
        "internal",
        {
          requests: Array<{
            executionId: string;
            filters?: {
              isFinished?: boolean;
              isSleepingTask?: boolean;
              status?:
                | "ready"
                | "running"
                | "failed"
                | "timed_out"
                | "waiting_for_children"
                | "waiting_for_finalize"
                | "finalize_failed"
                | "completed"
                | "cancelled";
            };
          }>;
        },
        any
      >;
      getManyByParentExecutionId: FunctionReference<
        "query",
        "internal",
        { requests: Array<{ parentExecutionId: string }> },
        any
      >;
      getManyBySleepingTaskUniqueId: FunctionReference<
        "query",
        "internal",
        { requests: Array<{ sleepingTaskUniqueId: string }> },
        any
      >;
      insertMany: FunctionReference<
        "mutation",
        "internal",
        {
          executions: Array<{
            acc: number;
            areChildrenSequential: boolean;
            children?: Array<{ executionId: string; taskId: string }>;
            closeExpiresAt?: number;
            closeStatus: "idle" | "ready" | "closing" | "closed";
            closedAt?: number;
            createdAt: number;
            error?: {
              errorType: "generic" | "not_found" | "timed_out" | "cancelled";
              isInternal: boolean;
              isRetryable: boolean;
              message: string;
            };
            executionId: string;
            executorId?: string;
            expiresAt?: number;
            finalize?: { executionId: string; taskId: string };
            finishedAt?: number;
            indexInParentChildren?: number;
            input: string;
            isFinalizeOfParent?: boolean;
            isFinished: boolean;
            isOnlyChildOfParent?: boolean;
            isSleepingTask: boolean;
            npc: boolean;
            ocfpExpiresAt?: number;
            ocfpFinishedAt?: number;
            ocfpStatus: "idle" | "processing" | "processed";
            output?: string;
            parentExecutionDocId?: string;
            parentExecutionId?: string;
            parentTaskId?: string;
            retryAttempts: number;
            retryOptions: {
              baseDelayMs?: number;
              delayMultiplier?: number;
              maxAttempts: number;
              maxDelayMs?: number;
            };
            rootExecutionId?: string;
            rootTaskId?: string;
            runOutput?: string;
            shard: number;
            sleepMsBeforeRun: number;
            sleepingTaskUniqueId?: string;
            startAt: number;
            startedAt?: number;
            status:
              | "ready"
              | "running"
              | "failed"
              | "timed_out"
              | "waiting_for_children"
              | "waiting_for_finalize"
              | "finalize_failed"
              | "completed"
              | "cancelled";
            taskId: string;
            timeoutMs: number;
            updatedAt: number;
          }>;
        },
        any
      >;
      releaseLock: FunctionReference<
        "mutation",
        "internal",
        { id: string },
        any
      >;
      updateAndDecrementParentACCByIsFinishedAndCloseStatus: FunctionReference<
        "mutation",
        "internal",
        any,
        any
      >;
      updateByCloseExpiresAt: FunctionReference<
        "mutation",
        "internal",
        any,
        any
      >;
      updateByCloseStatusAndReturn: FunctionReference<
        "mutation",
        "internal",
        any,
        any
      >;
      updateByExecutorIdAndNPCAndReturn: FunctionReference<
        "mutation",
        "internal",
        any,
        any
      >;
      updateByIsSleepingTaskAndExpiresAtLessThan: FunctionReference<
        "mutation",
        "internal",
        any,
        any
      >;
      updateByOCFPExpiresAt: FunctionReference<
        "mutation",
        "internal",
        any,
        any
      >;
      updateByStatusAndOCFPStatusAndACCZeroAndReturn: FunctionReference<
        "mutation",
        "internal",
        any,
        any
      >;
      updateByStatusAndStartAtLessThanAndReturn: FunctionReference<
        "mutation",
        "internal",
        any,
        any
      >;
      updateManyById: FunctionReference<
        "mutation",
        "internal",
        {
          requests: Array<{
            executionId: string;
            filters?: {
              isFinished?: boolean;
              isSleepingTask?: boolean;
              status?:
                | "ready"
                | "running"
                | "failed"
                | "timed_out"
                | "waiting_for_children"
                | "waiting_for_finalize"
                | "finalize_failed"
                | "completed"
                | "cancelled";
            };
            update: {
              acc?: number;
              children?: Array<{ executionId: string; taskId: string }>;
              closeExpiresAt?: number;
              closeStatus?: "idle" | "ready" | "closing" | "closed";
              closedAt?: number;
              error?: {
                errorType: "generic" | "not_found" | "timed_out" | "cancelled";
                isInternal: boolean;
                isRetryable: boolean;
                message: string;
              };
              executorId?: string;
              expiresAt?: number;
              finalize?: { executionId: string; taskId: string };
              finishedAt?: number;
              isFinished?: boolean;
              npc?: boolean;
              ocfpExpiresAt?: number;
              ocfpFinishedAt?: number;
              ocfpStatus?: "idle" | "processing" | "processed";
              output?: string;
              retryAttempts?: number;
              runOutput?: string;
              startAt?: number;
              startedAt?: number;
              status?:
                | "ready"
                | "running"
                | "failed"
                | "timed_out"
                | "waiting_for_children"
                | "waiting_for_finalize"
                | "finalize_failed"
                | "completed"
                | "cancelled";
              unsetCloseExpiresAt?: boolean;
              unsetError?: boolean;
              unsetExecutorId?: boolean;
              unsetExpiresAt?: boolean;
              unsetOCFPExpiresAt?: boolean;
              unsetRunOutput?: boolean;
              updatedAt: number;
            };
          }>;
        },
        any
      >;
      updateManyByIdAndInsertChildrenIfUpdated: FunctionReference<
        "mutation",
        "internal",
        {
          requests: Array<{
            childrenTaskExecutionsToInsertIfAnyUpdated: Array<{
              acc: number;
              areChildrenSequential: boolean;
              children?: Array<{ executionId: string; taskId: string }>;
              closeExpiresAt?: number;
              closeStatus: "idle" | "ready" | "closing" | "closed";
              closedAt?: number;
              createdAt: number;
              error?: {
                errorType: "generic" | "not_found" | "timed_out" | "cancelled";
                isInternal: boolean;
                isRetryable: boolean;
                message: string;
              };
              executionId: string;
              executorId?: string;
              expiresAt?: number;
              finalize?: { executionId: string; taskId: string };
              finishedAt?: number;
              indexInParentChildren?: number;
              input: string;
              isFinalizeOfParent?: boolean;
              isFinished: boolean;
              isOnlyChildOfParent?: boolean;
              isSleepingTask: boolean;
              npc: boolean;
              ocfpExpiresAt?: number;
              ocfpFinishedAt?: number;
              ocfpStatus: "idle" | "processing" | "processed";
              output?: string;
              parentExecutionDocId?: string;
              parentExecutionId?: string;
              parentTaskId?: string;
              retryAttempts: number;
              retryOptions: {
                baseDelayMs?: number;
                delayMultiplier?: number;
                maxAttempts: number;
                maxDelayMs?: number;
              };
              rootExecutionId?: string;
              rootTaskId?: string;
              runOutput?: string;
              shard: number;
              sleepMsBeforeRun: number;
              sleepingTaskUniqueId?: string;
              startAt: number;
              startedAt?: number;
              status:
                | "ready"
                | "running"
                | "failed"
                | "timed_out"
                | "waiting_for_children"
                | "waiting_for_finalize"
                | "finalize_failed"
                | "completed"
                | "cancelled";
              taskId: string;
              timeoutMs: number;
              updatedAt: number;
            }>;
            executionId: string;
            filters?: {
              isFinished?: boolean;
              isSleepingTask?: boolean;
              status?:
                | "ready"
                | "running"
                | "failed"
                | "timed_out"
                | "waiting_for_children"
                | "waiting_for_finalize"
                | "finalize_failed"
                | "completed"
                | "cancelled";
            };
            update: {
              acc?: number;
              children?: Array<{ executionId: string; taskId: string }>;
              closeExpiresAt?: number;
              closeStatus?: "idle" | "ready" | "closing" | "closed";
              closedAt?: number;
              error?: {
                errorType: "generic" | "not_found" | "timed_out" | "cancelled";
                isInternal: boolean;
                isRetryable: boolean;
                message: string;
              };
              executorId?: string;
              expiresAt?: number;
              finalize?: { executionId: string; taskId: string };
              finishedAt?: number;
              isFinished?: boolean;
              npc?: boolean;
              ocfpExpiresAt?: number;
              ocfpFinishedAt?: number;
              ocfpStatus?: "idle" | "processing" | "processed";
              output?: string;
              retryAttempts?: number;
              runOutput?: string;
              startAt?: number;
              startedAt?: number;
              status?:
                | "ready"
                | "running"
                | "failed"
                | "timed_out"
                | "waiting_for_children"
                | "waiting_for_finalize"
                | "finalize_failed"
                | "completed"
                | "cancelled";
              unsetCloseExpiresAt?: boolean;
              unsetError?: boolean;
              unsetExecutorId?: boolean;
              unsetExpiresAt?: boolean;
              unsetOCFPExpiresAt?: boolean;
              unsetRunOutput?: boolean;
              updatedAt: number;
            };
          }>;
        },
        any
      >;
      updateManyByParentExecutionIdAndIsFinished: FunctionReference<
        "mutation",
        "internal",
        {
          requests: Array<{
            isFinished: boolean;
            parentExecutionId: string;
            update: {
              acc?: number;
              children?: Array<{ executionId: string; taskId: string }>;
              closeExpiresAt?: number;
              closeStatus?: "idle" | "ready" | "closing" | "closed";
              closedAt?: number;
              error?: {
                errorType: "generic" | "not_found" | "timed_out" | "cancelled";
                isInternal: boolean;
                isRetryable: boolean;
                message: string;
              };
              executorId?: string;
              expiresAt?: number;
              finalize?: { executionId: string; taskId: string };
              finishedAt?: number;
              isFinished?: boolean;
              npc?: boolean;
              ocfpExpiresAt?: number;
              ocfpFinishedAt?: number;
              ocfpStatus?: "idle" | "processing" | "processed";
              output?: string;
              retryAttempts?: number;
              runOutput?: string;
              startAt?: number;
              startedAt?: number;
              status?:
                | "ready"
                | "running"
                | "failed"
                | "timed_out"
                | "waiting_for_children"
                | "waiting_for_finalize"
                | "finalize_failed"
                | "completed"
                | "cancelled";
              unsetCloseExpiresAt?: boolean;
              unsetError?: boolean;
              unsetExecutorId?: boolean;
              unsetExpiresAt?: boolean;
              unsetOCFPExpiresAt?: boolean;
              unsetRunOutput?: boolean;
              updatedAt: number;
            };
          }>;
        },
        any
      >;
    };
  };
};
