import { AsyncLocalStorage } from 'node:async_hooks'

export type CapturedQuery = {
  sql: string
  params: unknown[]
  durationMs?: number
}

const prismaStore = new AsyncLocalStorage<CapturedQuery[]>()
const drizzleStore = new AsyncLocalStorage<CapturedQuery[]>()

const registeredPrismaClients = new WeakSet<object>()

function parseMaybeJson(value: unknown): unknown {
  if (typeof value !== 'string') return value
  try {
    return JSON.parse(value)
  } catch {
    return value
  }
}

export function registerPrismaQueryCapture(prisma: any): void {
  if (!prisma || typeof prisma !== 'object') return
  if (registeredPrismaClients.has(prisma)) return
  registeredPrismaClients.add(prisma)

  prisma.$on('query', (e: any) => {
    const store = prismaStore.getStore()
    if (!store) return

    const parsed = parseMaybeJson(e?.params)
    const params = Array.isArray(parsed)
      ? parsed
      : parsed !== undefined
        ? [parsed]
        : []

    store.push({
      sql: String(e?.query ?? ''),
      params,
      durationMs: typeof e?.duration === 'number' ? e.duration : undefined,
    })
  })
}

export async function withPrismaCapture<T>(
  fn: () => Promise<T>,
): Promise<{ result: T; queries: CapturedQuery[] }> {
  const queries: CapturedQuery[] = []
  const result = await prismaStore.run(queries, fn)
  return { result, queries }
}

export class CaptureDrizzleLogger {
  logQuery(query: string, params: unknown[]): void {
    const store = drizzleStore.getStore()
    if (!store) return
    store.push({
      sql: query,
      params: Array.isArray(params) ? params : [params],
    })
  }
}

export async function withDrizzleCapture<T>(
  fn: () => Promise<T>,
): Promise<{ result: T; queries: CapturedQuery[] }> {
  const queries: CapturedQuery[] = []
  const result = await drizzleStore.run(queries, fn)
  return { result, queries }
}

export function formatCapturedQueries(
  queries: CapturedQuery[],
  limit = 10,
): string {
  const shown = queries.slice(0, limit)
  const lines: string[] = []
  lines.push(`Captured queries: ${queries.length}`)
  if (queries.length === 0) return lines.join('\n')

  for (let i = 0; i < shown.length; i++) {
    const q = shown[i]
    lines.push(`  ${i + 1})`)
    lines.push(`    sql: ${q.sql}`)
    lines.push(`    params: ${JSON.stringify(q.params)}`)
    if (typeof q.durationMs === 'number')
      lines.push(`    durationMs: ${q.durationMs}`)
  }

  if (queries.length > limit) lines.push(`  ... ${queries.length - limit} more`)
  return lines.join('\n')
}
