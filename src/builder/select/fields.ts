import { Model } from '../../types'
import { SCHEMA_PREFIXES, SQL_SEPARATORS } from '../shared/constants'
import {
  getScalarFieldSet,
  getRelationFieldSet,
} from '../shared/model-field-cache'
import { col, colWithAlias, sqlStringLiteral } from '../shared/sql-utils'
import {
  hasProperty,
  isNotNullish,
  isNonEmptyArray,
  isPlainObject,
} from '../shared/validators/type-guards'

type SelectEntry = readonly [string, unknown]

function toSelectEntries(select: Record<string, unknown>): SelectEntry[] {
  const out: SelectEntry[] = []
  for (const [k, v] of Object.entries(select)) {
    if (v !== false && v !== undefined) out.push([k, v])
  }
  return out
}

function validateSelectKeys(
  entries: readonly SelectEntry[],
  scalarSet: ReadonlySet<string>,
  relationSet: ReadonlySet<string>,
): void {
  const unknown: string[] = []
  for (const [k] of entries) {
    if (k === '_count') continue
    if (!scalarSet.has(k) && !relationSet.has(k)) unknown.push(k)
  }
  if (unknown.length > 0) {
    throw new Error(`Select contains unknown fields: ${unknown.join(', ')}`)
  }
}

function analyzeSelectEntries(
  entries: readonly SelectEntry[],
  scalarSet: ReadonlySet<string>,
  relationSet: ReadonlySet<string>,
): {
  scalarSelected: string[]
  hasRelationSelection: boolean
  hasCount: boolean
} {
  const scalarSelected: string[] = []
  let hasRelationSelection = false
  let hasCount = false

  for (const [k, v] of entries) {
    if (k === '_count') {
      hasCount = true
      continue
    }
    if (relationSet.has(k)) hasRelationSelection = true
    if (scalarSet.has(k) && v === true) scalarSelected.push(k)
  }

  return { scalarSelected, hasRelationSelection, hasCount }
}

function buildDefaultScalarFields(model: Model, alias: string): string[] {
  const excludedPrefixes = [
    SCHEMA_PREFIXES.INTERNAL,
    SCHEMA_PREFIXES.COMMENT,
  ] as const

  const out: string[] = []
  for (const f of model.fields) {
    if (f.isRelation) continue
    const excluded = excludedPrefixes.some((p) => f.name.startsWith(p))
    if (!excluded) out.push(colWithAlias(alias, f.name, model))
  }

  if (!isNonEmptyArray(out)) {
    throw new Error(`Model ${model.name} has no selectable fields`)
  }

  return out
}

export function buildSelectFields(
  args: { select?: Record<string, boolean | unknown> },
  model: Model,
  alias: string,
): string {
  const scalarSet = getScalarFieldSet(model)
  const relationSet = getRelationFieldSet(model)

  if (!isNotNullish(args.select)) {
    return buildDefaultScalarFields(model, alias).join(
      SQL_SEPARATORS.FIELD_LIST,
    )
  }

  const entries = toSelectEntries(args.select)
  validateSelectKeys(entries, scalarSet, relationSet)

  const { scalarSelected, hasRelationSelection, hasCount } =
    analyzeSelectEntries(entries, scalarSet, relationSet)

  const fields = scalarSelected.map((field) =>
    colWithAlias(alias, field, model),
  )

  if (!isNonEmptyArray(fields)) {
    if (hasRelationSelection) return ''
    if (!hasCount) {
      throw new Error('Select must have at least one scalar field set to true')
    }
  }

  return fields.join(SQL_SEPARATORS.FIELD_LIST)
}

function buildAllScalarParts(model: Model, alias: string): string[] {
  const scalarFields = model.fields.filter((f) => !f.isRelation)
  if (!isNonEmptyArray(scalarFields)) {
    throw new Error(`Model ${model.name} has no scalar fields`)
  }

  const parts: string[] = []
  for (const field of scalarFields) {
    parts.push(
      `${sqlStringLiteral(field.name)}, ${col(alias, field.name, model)}`,
    )
  }
  return parts
}

function validateRelationSelectKeys(
  entries: readonly SelectEntry[],
  scalarNames: ReadonlySet<string>,
  relationNames: ReadonlySet<string>,
): void {
  const unknown: string[] = []
  for (const [k] of entries) {
    if (!scalarNames.has(k) && !relationNames.has(k)) unknown.push(k)
  }
  if (unknown.length > 0) {
    throw new Error(`Select contains unknown fields: ${unknown.join(', ')}`)
  }
}

function buildSelectedScalarParts(
  entries: readonly SelectEntry[],
  scalarNames: ReadonlySet<string>,
  alias: string,
  model: Model,
): string[] {
  const parts: string[] = []
  for (const [key, value] of entries) {
    if (!scalarNames.has(key)) continue
    if (value === true) {
      parts.push(`${sqlStringLiteral(key)}, ${col(alias, key, model)}`)
    }
  }
  return parts
}

export function buildRelationSelect(
  relArgs: unknown,
  relModel: Model,
  relAlias: string,
): string {
  if (relArgs === true) {
    return buildAllScalarParts(relModel, relAlias).join(
      SQL_SEPARATORS.FIELD_LIST,
    )
  }

  if (isPlainObject(relArgs) && hasProperty(relArgs, 'select')) {
    const sel = (relArgs as { select: Record<string, unknown> }).select
    if (!isPlainObject(sel)) {
      throw new Error(
        `Relation select must be an object for model ${relModel.name}`,
      )
    }

    const scalarNames = new Set(
      relModel.fields.filter((f) => !f.isRelation).map((f) => f.name),
    )
    const relationNames = new Set(
      relModel.fields.filter((f) => f.isRelation).map((f) => f.name),
    )

    const entries = toSelectEntries(sel)
    validateRelationSelectKeys(entries, scalarNames, relationNames)

    return buildSelectedScalarParts(
      entries,
      scalarNames,
      relAlias,
      relModel,
    ).join(SQL_SEPARATORS.FIELD_LIST)
  }

  return buildAllScalarParts(relModel, relAlias).join(SQL_SEPARATORS.FIELD_LIST)
}
