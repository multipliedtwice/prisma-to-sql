import type { ReducerConfig } from './reducer'
import { createCoreReducer } from './core-reducer'

interface ProgressiveReducer {
  processRow(row: any): void
  getCurrentParentKey(row: any): string | null
  getCompletedParent(parentKey: string): any | null
  getRemainingParents(): any[]
}

export function createProgressiveReducer(
  config: ReducerConfig,
): ProgressiveReducer {
  const coreReducer = createCoreReducer({
    parentModel: config.parentModel,
    includedRelations: config.includedRelations,
  })

  const completedKeys = new Set<string>()

  return {
    processRow(row: any): void {
      coreReducer.processRow(row)
    },

    getCurrentParentKey(row: any): string | null {
      return coreReducer.processRow(row)
    },

    getCompletedParent(parentKey: string): any | null {
      if (completedKeys.has(parentKey)) return null
      const parent = coreReducer.getParent(parentKey)
      if (!parent) return null
      completedKeys.add(parentKey)
      return parent
    },

    getRemainingParents(): any[] {
      const remaining: any[] = []
      for (const [key, parent] of coreReducer.getParentMap()) {
        if (!completedKeys.has(key)) {
          remaining.push(parent)
          completedKeys.add(key)
        }
      }
      return remaining
    },
  }
}
