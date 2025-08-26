import eslintBaseConfig from '@gpahal/eslint-config/base'

/** @type {import("@gpahal/eslint-config/base").Config} */
export default eslintBaseConfig({
  tsconfigRootDir: import.meta.dirname,
  configs: [
    {
      ignores: ['**/_generated/**'],
      rules: {
        'unicorn/filename-case': 'off',
      },
    },
  ],
})
