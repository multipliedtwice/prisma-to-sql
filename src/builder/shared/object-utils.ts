export function hasOwnProperty<K extends string>(
  obj: unknown,
  key: K,
): obj is Record<K, unknown> {
  return (
    obj !== null &&
    obj !== undefined &&
    typeof obj === 'object' &&
    Object.prototype.hasOwnProperty.call(obj, key)
  )
}

export function hasOwnKey(obj: Record<string, unknown>, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(obj, key)
}
