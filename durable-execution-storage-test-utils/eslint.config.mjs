import path from 'node:path'

import { defineConfig, files } from '@gpahal/eslint-config'

export default defineConfig({
  root: import.meta.dirname,
  // The package is a test harness: its source runs Vitest assertions and prints bench results
  testFiles: (defaults) => [...defaults, 'src/**/*.ts'],
  turbo: true,
  vitest: true,
  configs: [
    {
      // The test and bench harness prints its progress and the bench results
      files: ['src/**/*.ts'],
      rules: { 'no-console': ['error', { allow: ['log', 'warn', 'error'] }] },
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
            packageDir: [import.meta.dirname, path.join(import.meta.dirname, '..')],
          },
        ],
      },
    },
  ],
})
