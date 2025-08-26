/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type * as lib from "../lib.js";

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
  lib: typeof lib;
}>;
export type Mounts = {
  lib: {
    deleteAll: FunctionReference<"action", "public", {}, any>;
    deleteById: FunctionReference<
      "mutation",
      "public",
      { executionId: string },
      any
    >;
    getById: FunctionReference<
      "query",
      "public",
      {
        executionId: string;
        filters: {
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
      },
      any
    >;
    getByParentExecutionId: FunctionReference<
      "query",
      "public",
      { parentExecutionId: string },
      any
    >;
    getBySleepingTaskUniqueId: FunctionReference<
      "query",
      "public",
      { sleepingTaskUniqueId: string },
      any
    >;
    getManyById: FunctionReference<
      "query",
      "public",
      {
        requests: Array<{
          executionId: string;
          filters: {
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
    insertMany: FunctionReference<
      "mutation",
      "public",
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
    updateAndDecrementParentACCByIsFinishedAndCloseStatus: FunctionReference<
      "action",
      "public",
      {
        closeStatus: "idle" | "ready" | "closing" | "closed";
        isFinished: boolean;
        limit: number;
        shard: number;
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
      },
      any
    >;
    updateByCloseExpiresAt: FunctionReference<
      "action",
      "public",
      {
        closeExpiresAtLessThan: number;
        limit: number;
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
      },
      any
    >;
    updateByCloseStatusAndReturn: FunctionReference<
      "action",
      "public",
      {
        closeStatus: "idle" | "ready" | "closing" | "closed";
        limit: number;
        shard: number;
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
      },
      any
    >;
    updateByExecutorIdAndNPCAndReturn: FunctionReference<
      "action",
      "public",
      {
        executorId: string;
        limit: number;
        npc: boolean;
        shard: number;
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
      },
      any
    >;
    updateByIdAndInsertChildrenIfUpdated: FunctionReference<
      "mutation",
      "public",
      {
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
        filters: {
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
      },
      any
    >;
    updateByIsSleepingTaskAndExpiresAtLessThan: FunctionReference<
      "action",
      "public",
      {
        expiresAtLessThan: number;
        isSleepingTask: boolean;
        limit: number;
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
      },
      any
    >;
    updateByOCFPExpiresAt: FunctionReference<
      "action",
      "public",
      {
        limit: number;
        ocfpExpiresAtLessThan: number;
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
      },
      any
    >;
    updateByParentExecutionIdAndIsFinished: FunctionReference<
      "mutation",
      "public",
      {
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
      },
      any
    >;
    updateByStatusAndOCFPStatusAndACCZeroAndReturn: FunctionReference<
      "action",
      "public",
      {
        limit: number;
        ocfpStatus: "idle" | "processing" | "processed";
        shard: number;
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
      },
      any
    >;
    updateByStatusAndStartAtLessThanAndReturn: FunctionReference<
      "action",
      "public",
      {
        limit: number;
        shard: number;
        startAtLessThan: number;
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
        updateExpiresAtWithStartedAt: number;
      },
      any
    >;
    updateManyById: FunctionReference<
      "mutation",
      "public",
      {
        requests: Array<{
          executionId: string;
          filters: {
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
      "public",
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
          filters: {
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
  };
};
// For now fullApiWithMounts is only fullApi which provides
// jump-to-definition in component client code.
// Use Mounts for the same type without the inference.
declare const fullApiWithMounts: typeof fullApi;

export declare const api: FilterApi<
  typeof fullApiWithMounts,
  FunctionReference<any, "public">
>;
export declare const internal: FilterApi<
  typeof fullApiWithMounts,
  FunctionReference<any, "internal">
>;

export declare const components: {};
