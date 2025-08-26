import { defineConfig, type Options } from 'tsup'

export default defineConfig((options: Options): Options => {
  const env =
    process.env.ENV === 'production' || process.env.NODE_ENV === 'production' || !!options.watch
      ? 'production'
      : 'development'
  const isDevEnv = env === 'development'
  return {
    entry: ['src/**/*'],
    tsconfig: 'tsconfig.build.json',
    outDir: 'build',
    format: ['esm'],
    env: {
      ENV: env,
      NODE_ENV: env,
    },
    splitting: false,
    clean: true,
    sourcemap: true,
    dts: false,
    minify: !isDevEnv,
    onSuccess: 'tsc -p tsconfig.build.json',
  }
})
