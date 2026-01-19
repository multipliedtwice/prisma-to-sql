import { extractDynamicName, isDynamicParameter } from '@dee-wan/schema-parser'
import { ParamStore } from './param-store'

function scopeName(scope: string, dynamicName: string): string {
  const s = String(scope).trim()
  const dn = String(dynamicName).trim()
  if (s.length === 0) return dn
  return `${s}:${dn}`
}

export function addAutoScoped(
  params: ParamStore,
  value: unknown,
  scope: string,
): string {
  if (isDynamicParameter(value)) {
    const dn = extractDynamicName(value as string)
    return params.add(undefined, scopeName(scope, dn))
  }
  return params.add(value)
}
