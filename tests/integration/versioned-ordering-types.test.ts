import { execFile } from 'node:child_process'
import {
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { promisify } from 'node:util'
import { getDMMF } from '@prisma/internals'
import ts from 'typescript'
import { generateClient } from '../../src/code-emitter'
import { getClientImportPath } from '../../src/generator-paths'

const execFileAsync = promisify(execFile)
const prismaPackage = JSON.parse(
  readFileSync(resolve('node_modules/prisma/package.json'), 'utf8'),
)
if (typeof prismaPackage.version !== 'string') {
  throw new Error('Installed Prisma package has no version')
}
const installedPrismaVersion = prismaPackage.version
const prismaMajor = Number(installedPrismaVersion.split('.')[0])

const plannerArtifacts = {
  relationStats: {},
  modelStats: {},
  roundtripRowEquivalent: 73,
  jsonRowFactor: 1.5,
  collectedAt: 0,
  edgeTimings: {},
}

function expectConsumerCompiles(
  consumerPath: string,
  moduleResolution: ts.ModuleResolutionKind,
): void {
  const program = ts.createProgram([consumerPath], {
    esModuleInterop: true,
    module:
      moduleResolution === ts.ModuleResolutionKind.Bundler
        ? ts.ModuleKind.ESNext
        : ts.ModuleKind.CommonJS,
    moduleResolution,
    noEmit: true,
    skipLibCheck: true,
    strict: true,
    target: ts.ScriptTarget.ES2020,
  })
  const diagnostics = ts
    .getPreEmitDiagnostics(program)
    .filter((diagnostic) => diagnostic.file?.fileName === consumerPath)

  expect(
    ts.formatDiagnosticsWithColorAndContext(diagnostics, {
      getCanonicalFileName: (fileName) => fileName,
      getCurrentDirectory: process.cwd,
      getNewLine: () => '\n',
    }),
  ).toBe('')
}

describe('versioned generated-client ordering types', () => {
  const fixtureDir = mkdtempSync(join(tmpdir(), 'prisma-sql-version-types-'))
  const clientDir = join(fixtureDir, 'client')
  const sqlDir = join(fixtureDir, 'sql')
  const schemaPath = join(fixtureDir, 'schema.prisma')
  symlinkSync(resolve('node_modules'), join(fixtureDir, 'node_modules'), 'dir')

  afterAll(() => {
    rmSync(fixtureDir, { recursive: true, force: true })
  })

  it(`generates and compiles with Prisma ${installedPrismaVersion}`, async () => {
    expect([6, 7]).toContain(prismaMajor)
    if (process.env.PRISMA_VERSION) {
      expect(installedPrismaVersion).toBe(process.env.PRISMA_VERSION)
    }

    const provider = prismaMajor === 6 ? 'prisma-client-js' : 'prisma-client'
    const datasourceUrl = prismaMajor === 6 ? '\n  url = env("DATABASE_URL")' : ''
    const schema = `generator client {
  provider = "${provider}"
  output   = "./client"
}

datasource db {
  provider = "postgresql"${datasourceUrl}
}

model Parent {
  id              Int            @id @default(autoincrement())
  requiredDate    DateTime
  nullableDate    DateTime?
  optionalChild   OptionalChild?
  requiredChildId Int            @unique
  requiredChild   RequiredChild  @relation(fields: [requiredChildId], references: [id])
}

model OptionalChild {
  id           Int      @id @default(autoincrement())
  requiredDate DateTime
  parentId     Int      @unique
  parent       Parent   @relation(fields: [parentId], references: [id])
}

model RequiredChild {
  id           Int      @id @default(autoincrement())
  requiredDate DateTime
  parent       Parent?
}
`
    writeFileSync(schemaPath, schema)

    await execFileAsync(
      process.execPath,
      [
        resolve('node_modules/prisma/build/index.js'),
        'generate',
        `--schema=${schemaPath}`,
      ],
      {
        env: {
          ...process.env,
          DATABASE_URL: 'postgresql://test:test@localhost:5432/test',
        },
      },
    )

    const clientImportPath = getClientImportPath(
      {
        schemaPath,
        otherGenerators: [
          {
            provider: { value: provider },
            output: { value: './client' },
          },
        ],
      },
      sqlDir,
    )
    expect(clientImportPath).toBe(
      prismaMajor === 6 ? '../client' : '../client/client',
    )

    const dmmf = await getDMMF({ datamodel: schema })
    await generateClient({
      datamodel: dmmf.datamodel,
      outputDir: sqlDir,
      config: { dialect: 'postgres', skipInvalid: false },
      runtimeImportPath: resolve('src/index'),
      clientImportPath,
      plannerArtifacts,
    })

    const generatedSource = readFileSync(join(sqlDir, 'index.ts'), 'utf8')
    expect(generatedSource).toContain(
      `from ${JSON.stringify(clientImportPath)}`,
    )

    const clientEntry =
      prismaMajor === 6 ? clientDir : join(clientDir, 'client')
    const consumerPath = join(sqlDir, 'consumer.ts')
    writeFileSync(
      consumerPath,
      `import type { PrismaClient } from ${JSON.stringify(clientEntry)}
import type { SpeedClient } from './index'

declare const baseClient: PrismaClient
declare const client: SpeedClient<typeof baseClient>

async function acceptedQueries() {
  await client.parent.findMany({
    orderBy: {
      optionalChild: {
        requiredDate: { sort: 'desc', nulls: 'last' },
      },
    },
  })

  await client.parent.findFirst({
    orderBy: [
      { requiredDate: 'asc' },
      {
        optionalChild: {
          requiredDate: { sort: 'asc', nulls: 'first' },
        },
      },
    ],
  })

  await client.parent.findMany({
    orderBy: { nullableDate: { sort: 'asc', nulls: 'first' } },
  })

  const selected = await client.parent.findMany({
    select: { id: true },
    orderBy: {
      optionalChild: {
        requiredDate: { sort: 'desc', nulls: 'last' },
      },
    },
  })
  selected[0].id
  // @ts-expect-error selected payload stays narrow
  selected[0].requiredDate

  const included = await client.parent.findFirst({
    include: { optionalChild: true },
    orderBy: {
      optionalChild: {
        requiredDate: { sort: 'desc', nulls: 'last' },
      },
    },
  })
  included?.optionalChild?.requiredDate

  await baseClient.parent.findMany({
    // @ts-expect-error native Prisma input stays narrow
    orderBy: {
      optionalChild: {
        requiredDate: { sort: 'desc', nulls: 'last' },
      },
    },
  })

  await client.parent.findMany({
    // @ts-expect-error required root scalar stays Prisma-typed
    orderBy: { requiredDate: { sort: 'desc', nulls: 'last' } },
  })

  await client.parent.findMany({
    // @ts-expect-error fully required relation path stays Prisma-typed
    orderBy: {
      requiredChild: {
        requiredDate: { sort: 'desc', nulls: 'last' },
      },
    },
  })
}

void acceptedQueries
`,
    )

    expectConsumerCompiles(
      consumerPath,
      prismaMajor === 6
        ? ts.ModuleResolutionKind.Node10
        : ts.ModuleResolutionKind.Bundler,
    )
  }, 30000)
})
