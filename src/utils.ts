import type { StandardSchemaV1 } from '@standard-schema/spec'
import { getDotPath } from '@standard-schema/utils'
import { customAlphabet } from 'nanoid'

import { sleep } from '@gpahal/std/promises'

import { DurableTaskError } from './errors'

/**
 * Sleep with jitter.
 *
 * @param ms - The time to sleep.
 * @returns A promise that resolves after the time has passed.
 */
export function sleepWithJitter(ms: number): Promise<void> {
  return sleep(ms * (0.5 + Math.random()))
}

const _ALPHABET = '0123456789ABCDEFGHJKMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz'

/**
 * Generate a random id.
 *
 * @param size - The length of the id. If not provided, it will be 24.
 * @returns A random id.
 */
export const generateId = customAlphabet(_ALPHABET, 24)

/**
 * Validate input against a standard schema.
 *
 * @param schema - The input schema.
 * @param input - The input to validate.
 * @returns The validated output.
 * @throws A {@link DurableTaskError} if the input is invalid.
 */
export async function validateStandardSchema<TInputSchema extends StandardSchemaV1>(
  schema: TInputSchema,
  input: StandardSchemaV1.InferInput<TInputSchema>,
): Promise<StandardSchemaV1.InferOutput<TInputSchema>> {
  const validateResult = await schema['~standard'].validate(input)
  if (validateResult.issues != null) {
    throw new DurableTaskError(summarizeStandardSchemaIssues(validateResult.issues), false)
  }
  return validateResult.value
}

/**
 * Summarize standard schema issues.
 *
 * @param issues - The issues to summarize.
 * @returns A summary of the issues.
 */
function summarizeStandardSchemaIssues(issues: ReadonlyArray<StandardSchemaV1.Issue>): string {
  let summary = ''
  for (const issue of issues) {
    if (summary) {
      summary += '\n'
    }

    summary += `× ${issue.message}`
    const dotPath = getDotPath(issue)
    if (dotPath) {
      summary += `\n  → at ${dotPath}`
    }
  }

  return summary
}
