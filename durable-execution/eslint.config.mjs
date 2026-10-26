import path from 'node:path'

import { defineConfig, files } from '@gpahal/eslint-config'

export default defineConfig({
  root: import.meta.dirname,
  ignores: (defaults) => [...defaults, 'docs/**'],
  turbo: true,
  vitest: true,
  configs: [
    {
      // Tools such as vitest, tsup and typedoc are devDependencies of the workspace root
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
