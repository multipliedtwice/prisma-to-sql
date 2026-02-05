export class StringBuilder {
  private parts: string[] = []

  append(str: string): this {
    if (str) this.parts.push(str)
    return this
  }

  appendIf(condition: boolean, str: string): this {
    if (condition && str) this.parts.push(str)
    return this
  }

  join(separator: string): string {
    return this.parts.join(separator)
  }

  toString(): string {
    return this.parts.join('')
  }

  clear(): void {
    this.parts.length = 0
  }
}

export function joinNonEmpty(parts: string[], sep: string): string {
  let result = ''
  for (const p of parts) {
    if (p) {
      if (result) result += sep
      result += p
    }
  }
  return result
}
