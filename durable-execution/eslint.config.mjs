import eslintBaseConfig from '@gpahal/eslint-config/base'
import eslintTurboConfig from '@gpahal/eslint-config/turbo'
import eslintVitestConfig from '@gpahal/eslint-config/vitest'

/** @type {import("@gpahal/eslint-config/base").Config} */
export default eslintBaseConfig({
  tsconfigRootDir: import.meta.dirname,
  configs: [
    {
      ignores: ['docs'],
    },
    eslintVitestConfig,
    eslintTurboConfig,
  ],
})
