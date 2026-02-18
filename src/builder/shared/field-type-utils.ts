import type { Field } from '../../types'

export function isListRelation(field: Field): boolean {
  return typeof field.type === 'string' && field.type.endsWith('[]')
}
