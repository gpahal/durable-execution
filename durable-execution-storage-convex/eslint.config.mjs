import path from 'node:path'

import { defineConfig, files } from '@gpahal/eslint-config'

export default defineConfig({
  root: import.meta.dirname,
  // `test` is a workspace package of its own, linted with its own config
  ignores: (defaults) => [...defaults, '**/_generated/**', 'test/**'],
  turbo: true,
  vitest: true,
  // Convex function files are named after the functions they define, such as `taskExecutions.ts`
  unicorn: { rules: { 'unicorn/filename-case': 'off' } },
  configs: [
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
            packageDir: [import.meta.dirname, path.join(import.meta.dirname, '..')],
          },
        ],
      },
    },
  ],
})
