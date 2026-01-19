import { defineConfig } from 'tsup'

export default defineConfig([
  {
    entry: {
      index: 'src/index.ts',
    },
    format: ['cjs', 'esm'],
    dts: true,
    splitting: false,
    sourcemap: true,
    clean: true,
  },
  {
    entry: {
      generator: 'src/generator.ts',
    },
    format: ['cjs'],
    dts: false,
    splitting: false,
    sourcemap: false,
    clean: false,
    banner: {
      js: '#!/usr/bin/env node',
    },
  },
])
