import { execFile } from 'node:child_process'
import { readFile, writeFile } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { promisify } from 'node:util'

const execFileAsync = promisify(execFile)
const workspace = resolve(process.cwd())
const prismaDir = join(workspace, 'tests', 'prisma')
const prismaCli = join(workspace, 'node_modules', 'prisma', 'build', 'index.js')

async function generateTestClient(
  dialect: 'postgres' | 'sqlite',
): Promise<void> {
  const [header, base] = await Promise.all([
    readFile(join(prismaDir, `${dialect}.prisma`), 'utf8'),
    readFile(join(prismaDir, 'base.prisma'), 'utf8'),
  ])
  const schemaPath = join(prismaDir, `schema-${dialect}.prisma`)
  await writeFile(schemaPath, `${header}\n\n${base}`)

  await execFileAsync(
    process.execPath,
    [prismaCli, 'generate', `--schema=${schemaPath}`],
    {
      cwd: workspace,
      env: {
        ...process.env,
        DATABASE_URL:
          dialect === 'postgres'
            ? 'postgres://postgres:postgres@localhost:5433/prisma_test'
            : 'file:./tests/prisma/db.sqlite',
      },
    },
  )
}

async function main(): Promise<void> {
  for (const dialect of ['postgres', 'sqlite'] as const) {
    await generateTestClient(dialect)
  }
}

void main()
