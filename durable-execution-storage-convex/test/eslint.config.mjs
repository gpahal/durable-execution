import path from 'node:path'

import { defineConfig, files } from '@gpahal/eslint-config'

export default defineConfig({
  root: import.meta.dirname,
  ignores: (defaults) => [...defaults, '**/_generated/**'],
  turbo: true,
  vitest: true,
  // Convex function files are named after the functions they define, such as `taskExecutions.ts`
  unicorn: { rules: { 'unicorn/filename-case': 'off' } },
  configs: [
    {
      // The package sits in the folder of durable-execution-storage-convex, whose dependencies,
      // such as durable-execution, its code imports
      files: [...files.code],
      rules: {
        'import-x/no-extraneous-dependencies': [
          'error',
          {
            devDependencies: false,
            peerDependencies: true,
            optionalDependencies: true,
            packageDir: [import.meta.dirname, path.join(import.meta.dirname, '..')],
          },
        ],
      },
    },
    {
      // Tools such as vitest and tsup are devDependencies of the workspace root
      files: [...files.test, ...files.config, ...files.script],
      rules: {
        'import-x/no-extraneous-dependencies': [
          'error',
          {
            devDependencies: true,
            peerDependencies: true,
            optionalDependencies: true,
            packageDir: [
              import.meta.dirname,
              path.join(import.meta.dirname, '..'),
              path.join(import.meta.dirname, '..', '..'),
            ],
          },
        ],
      },
    },
  ],
})
