import { sleep } from '@gpahal/std/promises'

const _ID_REGEX = /^\w+$/

/**
 * Validate an id. Make sure it is not empty, not longer than 255 characters, and only contains
 * alphanumeric characters and underscores.
 *
 * @param id - The id to validate.
 * @throws An error if the id is invalid.
 */
export function validateId(id: string): void {
  if (id.length === 0) {
    throw new Error('Id cannot be empty')
  }
  if (id.length > 255) {
    throw new Error('Id cannot be longer than 255 characters')
  }
  if (!_ID_REGEX.test(id)) {
    throw new Error('Id can only contain alphanumeric characters and underscores')
  }
}

export function sleepWithJitter(ms: number): Promise<void> {
  return sleep(ms * (0.5 + Math.random()))
}
