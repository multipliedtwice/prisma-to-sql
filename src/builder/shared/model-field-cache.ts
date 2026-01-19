import { Model } from '../../types'

const SCALAR_SET_CACHE = new WeakMap<Model, Set<string>>()
const RELATION_SET_CACHE = new WeakMap<Model, Set<string>>()

export function getScalarFieldSet(model: Model): Set<string> {
  const cached = SCALAR_SET_CACHE.get(model)
  if (cached) return cached

  const s = new Set<string>()
  for (const f of model.fields) if (!f.isRelation) s.add(f.name)
  SCALAR_SET_CACHE.set(model, s)
  return s
}

export function getRelationFieldSet(model: Model): Set<string> {
  const cached = RELATION_SET_CACHE.get(model)
  if (cached) return cached

  const s = new Set<string>()
  for (const f of model.fields) if (f.isRelation) s.add(f.name)
  RELATION_SET_CACHE.set(model, s)
  return s
}
