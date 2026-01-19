export default {
  packageManager: 'npm',
  reporters: ['html', 'clear-text', 'progress'],
  testRunner: 'vitest',
  vitest: {
    configFile: 'vitest.config.ts'
  },
  coverageAnalysis: 'perTest',
  mutate: [
    'src/**/*.ts',
    '!src/**/*.spec.ts',
    '!src/index.ts',
  ],
  thresholds: {
    high: 80,
    low: 60,
    break: 50
  },
  incremental: true,
  incrementalFile: '.stryker-tmp/incremental.json',
  ignoreStatic: true,
  timeoutMS: 60000,
  concurrency: 12,
  maxConcurrentTestRunners: 2
}