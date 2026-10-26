/* eslint-disable */
/**
 * Generated `ComponentApi` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type { FunctionReference } from "convex/server";

/**
 * A utility for referencing a Convex component's exposed API.
 *
 * Useful when expecting a parameter like `components.myComponent`.
 * Usage:
 * ```ts
 * async function myFunction(ctx: QueryCtx, component: ComponentApi) {
 *   return ctx.runQuery(component.someFile.someQuery, { ...args });
 * }
 * ```
 */
export type ComponentApi<Name extends string | undefined = string | undefined> =
  {
    lib: {
      acquireLock: FunctionReference<
        "mutation",
        "internal",
        { key: string },
        any,
        Name
      >;
      deleteAll: FunctionReference<"action", "internal", {}, any, Name>;
      deleteById: FunctionReference<
        "mutation",
        "internal",
        { executionId: string },
        any,
        Name
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
        any,
        Name
      >;
      getManyByParentExecutionId: FunctionReference<
        "query",
        "internal",
        { requests: Array<{ parentExecutionId: string }> },
        any,
        Name
      >;
      getManyBySleepingTaskUniqueId: FunctionReference<
        "query",
        "internal",
        { requests: Array<{ sleepingTaskUniqueId: string }> },
        any,
        Name
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
            waitingForChildrenStartedAt?: number;
            waitingForFinalizeStartedAt?: number;
          }>;
        },
        any,
        Name
      >;
      releaseLock: FunctionReference<
        "mutation",
        "internal",
        { id: string },
        any,
        Name
      >;
      updateAndDecrementParentACCByIsFinishedAndCloseStatus: FunctionReference<
        "mutation",
        "internal",
        any,
        any,
        Name
      >;
      updateByCloseExpiresAt: FunctionReference<
        "mutation",
        "internal",
        any,
        any,
        Name
      >;
      updateByCloseStatusAndReturn: FunctionReference<
        "mutation",
        "internal",
        any,
        any,
        Name
      >;
      updateByExecutorIdAndNPCAndReturn: FunctionReference<
        "mutation",
        "internal",
        any,
        any,
        Name
      >;
      updateByOCFPExpiresAt: FunctionReference<
        "mutation",
        "internal",
        any,
        any,
        Name
      >;
      updateByStatusAndIsSleepingTaskAndExpiresAtLessThan: FunctionReference<
        "mutation",
        "internal",
        any,
        any,
        Name
      >;
      updateByStatusAndOCFPStatusAndACCZeroAndReturn: FunctionReference<
        "mutation",
        "internal",
        any,
        any,
        Name
      >;
      updateByStatusAndStartAtLessThanAndReturn: FunctionReference<
        "mutation",
        "internal",
        any,
        any,
        Name
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
              unset?: {
                closeExpiresAt?: boolean;
                error?: boolean;
                executorId?: boolean;
                expiresAt?: boolean;
                ocfpExpiresAt?: boolean;
                runOutput?: boolean;
                startedAt?: boolean;
              };
              updatedAt: number;
              waitingForChildrenStartedAt?: number;
              waitingForFinalizeStartedAt?: number;
            };
          }>;
        },
        any,
        Name
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
              waitingForChildrenStartedAt?: number;
              waitingForFinalizeStartedAt?: number;
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
              unset?: {
                closeExpiresAt?: boolean;
                error?: boolean;
                executorId?: boolean;
                expiresAt?: boolean;
                ocfpExpiresAt?: boolean;
                runOutput?: boolean;
                startedAt?: boolean;
              };
              updatedAt: number;
              waitingForChildrenStartedAt?: number;
              waitingForFinalizeStartedAt?: number;
            };
          }>;
        },
        any,
        Name
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
              unset?: {
                closeExpiresAt?: boolean;
                error?: boolean;
                executorId?: boolean;
                expiresAt?: boolean;
                ocfpExpiresAt?: boolean;
                runOutput?: boolean;
                startedAt?: boolean;
              };
              updatedAt: number;
              waitingForChildrenStartedAt?: number;
              waitingForFinalizeStartedAt?: number;
            };
          }>;
        },
        any,
        Name
      >;
    };
  };
