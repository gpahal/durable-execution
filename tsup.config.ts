import { defineConfig, type Options } from 'tsup'

export default defineConfig((options: Options): Options => {
  const isDevEnv = process.env.NODE_ENV === 'development' || !!options.watch
  return {
    entry: ['src/*'],
    tsconfig: 'tsconfig.build.json',
    outDir: 'build',
    format: ['esm'],
    env: {
      NODE_ENV: isDevEnv ? 'development' : 'production',
    },
    splitting: false,
    clean: true,
    sourcemap: true,
    dts: false,
    minify: !isDevEnv,
    onSuccess: 'tsc -p tsconfig.build.json',
  }
})
