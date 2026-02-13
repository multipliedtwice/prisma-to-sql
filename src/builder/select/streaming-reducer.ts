import { ReducerConfig } from './reducer'
import { createCoreReducer } from './core-reducer'

export function createStreamingReducer(config: ReducerConfig) {
  const coreReducer = createCoreReducer({
    parentModel: config.parentModel,
    includedRelations: config.includedRelations,
  })

  return {
    processRow(row: any): void {
      coreReducer.processRow(row)
    },

    getResults(): any[] {
      return coreReducer.getAllParents()
    },

    getParentMap(): Map<string, any> {
      return coreReducer.getParentMap()
    },
  }
}
