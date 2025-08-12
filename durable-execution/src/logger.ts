/**
 * A logger interface.
 *
 * @category Logger
 */
export type Logger = {
  debug: (message: string) => void
  info: (message: string) => void
  error: (message: string, error?: unknown) => void
}

/**
 * Create a console logger.
 *
 * @example
 * ```ts
 * const logger = createConsoleLogger('my-app')
 * logger.info('Hello, world!')
 * ```
 *
 * @param name - The name of the logger.
 * @returns The logger.
 *
 * @category Logger
 */
export function createConsoleLogger(name: string): Logger {
  return {
    debug: (message) => console.debug(`DEBUG [${name}] ${message}`),
    info: (message) => console.info(`INFO  [${name}] ${message}`),
    error: (message, error) =>
      error
        ? console.error(`ERROR [${name}] ${message}`, error)
        : console.error(`ERROR [${name}] ${message}`),
  }
}

function noop(): void {
  // Do nothing
}

/**
 * Create a logger that disables debug logs.
 *
 * @param logger - The logger to disable debug logs for.
 * @returns The logger.
 *
 * @category Logger
 */
export function createLoggerWithDebugDisabled(logger: Logger): Logger {
  return {
    debug: noop,
    info: logger.info,
    error: logger.error,
  }
}
