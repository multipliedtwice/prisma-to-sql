import { configDefaults, defineConfig } from 'vitest/config'
import path from 'path'

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    testTimeout: 30000,
    hookTimeout: 30000,
    include: [
      'tests/unit/**/*.test.ts',
      'tests/integration/**/*.test.ts',
      'tests/sql-injection/**/*.test.ts',
    ],
    exclude: [
      ...configDefaults.exclude,
      'tests/e2e/**',
      '**/node_modules/**',
      '**/dist/**',
      '/tests/generated/**',
      '/tests/prisma/*.full.prisma',
      '/tests/prisma/db.sqlite*',
    ],
    // Unit tests CAN run in parallel
    fileParallelism: true,
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      exclude: [
        'node_modules/',
        'tests/',
        '**/*.d.ts',
        '**/*.config.*',
        '**/dist/',
        '**/generated/',
      ],
    },
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
})
