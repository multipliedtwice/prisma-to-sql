interface CompactIssue {
  severity: string
  rule: string
  message: string
  line?: number
  status: string
}

interface CompactReport {
  total: number
  files: {
    [filePath: string]: {
      issues: CompactIssue[]
    }
  }
}

function compactSonarReport(report: any): CompactReport {
  const compact: CompactReport = {
    total: report.total,
    files: {},
  }

  const componentMap: Record<string, { path: string }> = {}
  for (const comp of report.components ?? []) {
    if (comp?.path) componentMap[comp.key] = { path: comp.path }
  }

  for (const issue of report.issues ?? []) {
    if (issue?.status === 'CLOSED') continue

    const compKey = issue.component
    const filePath = componentMap[compKey]?.path || compKey

    ;(compact.files[filePath] ??= { issues: [] }).issues.push({
      severity: issue.severity,
      rule: issue.rule,
      message: issue.message,
      line: issue.line,
      status: issue.status,
    })
  }

  return compact
}

const pathCollator = new Intl.Collator(undefined, {
  numeric: true,
  sensitivity: 'base',
})

function comparePath(a: string, b: string): number {
  return pathCollator.compare(a, b)
}

function compareIssue(a: CompactIssue, b: CompactIssue): number {
  const aLine = a.line ?? Number.POSITIVE_INFINITY
  const bLine = b.line ?? Number.POSITIVE_INFINITY
  if (aLine !== bLine) return aLine - bLine

  const ruleCmp = pathCollator.compare(a.rule, b.rule)
  if (ruleCmp !== 0) return ruleCmp

  const sevCmp = pathCollator.compare(a.severity, b.severity)
  if (sevCmp !== 0) return sevCmp

  return pathCollator.compare(a.message, b.message)
}

async function fetchAndLogSonarIssues(token: string, projectKey: string = 'b') {
  const url = `http://localhost:9000/api/issues/search?componentKeys=${encodeURIComponent(projectKey)}&ps=500`
  const auth = btoa(`${token}:`)

  const response = await fetch(url, {
    headers: { Authorization: `Basic ${auth}` },
  })

  if (!response.ok) {
    console.error(`Failed to fetch: ${response.status} ${response.statusText}`)
    return
  }

  const data = await response.json()
  const compact = compactSonarReport(data)

  const sortedFiles = Object.entries(compact.files).sort(([a], [b]) =>
    comparePath(a, b),
  )

  for (const [file, { issues }] of sortedFiles) {
    const sortedIssues = [...issues].sort(compareIssue)

    console.group(file + ':' + issues[0].line)
    for (const issue of sortedIssues) {
      const line = issue.line ?? '-'
      console.log(`${issue.rule} at line ${line}: ${issue.message}`)
    }
    console.groupEnd()
  }
}

const token = 'sqp_0d5fbc16a275fceb6458d193a8aa8a975956edc1'
if (!token) {
  console.error('SONAR_TOKEN environment variable is required')
  process.exit(1)
}

fetchAndLogSonarIssues(token).catch((err) => console.error(err))
