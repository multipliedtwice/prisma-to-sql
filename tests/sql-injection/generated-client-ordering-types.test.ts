import { execFile } from 'node:child_process'
import {
  mkdirSync,
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
import { Prisma } from '../generated/postgres/client'
import { generateClient } from '../../src/code-emitter'
import { getClientImportPath } from '../../src/generator-paths'

const execFileAsync = promisify(execFile)

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
  moduleResolution = ts.ModuleResolutionKind.Node10,
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

describe('generated client optional relation ordering types', () => {
  const fixtureDir = mkdtempSync(join(tmpdir(), 'prisma-sql-order-types-'))
  symlinkSync(resolve('node_modules'), join(fixtureDir, 'node_modules'), 'dir')

  afterAll(() => {
    rmSync(fixtureDir, { recursive: true, force: true })
  })

  it('supports the Prisma 6 client type contract', async () => {
    const clientPath = resolve('tests/generated/postgres/client')
    const runtimePath = resolve('src/index')
    const outputDir = join(fixtureDir, 'prisma-6-sql')
    const clientImportPath = getClientImportPath(
      {
        schemaPath: resolve('tests/prisma/postgres.prisma'),
        otherGenerators: [
          {
            provider: { value: 'prisma-client-js' },
            output: { value: '../generated/postgres' },
          },
        ],
      },
      outputDir,
    )

    expect(resolve(outputDir, clientImportPath)).toBe(
      resolve('tests/generated/postgres'),
    )

    await generateClient({
      datamodel: Prisma.dmmf.datamodel,
      outputDir,
      config: { dialect: 'postgres', skipInvalid: false },
      runtimeImportPath: runtimePath,
      clientImportPath,
      plannerArtifacts,
    })
    const generatedSource = readFileSync(join(outputDir, 'index.ts'), 'utf8')
    expect(generatedSource).not.toContain('PrismaTypes.NullsOrder')
    expect(generatedSource).toContain(
      `from ${JSON.stringify(clientImportPath)}`,
    )

    const consumerPath = join(outputDir, 'consumer.ts')
    writeFileSync(
      consumerPath,
      `import type { PrismaClient } from ${JSON.stringify(clientPath)}
import type { SpeedClient } from './index'

declare const client: SpeedClient<PrismaClient>
declare const baseClient: PrismaClient

const extendedBase = baseClient.$extends({
  result: {
    task: {
      titleLength: {
        needs: { title: true },
        compute(task) {
          return task.title.length
        },
      },
    },
  },
})
declare const extendedClient: SpeedClient<typeof extendedBase>

async function acceptedQueries() {
  const allTasks = await client.task.findMany()
  allTasks[0].title
  const firstTask = await client.task.findFirst()
  firstTask?.title

  await client.task.findMany({
    orderBy: {
      assignee: {
        createdAt: { sort: 'desc', nulls: 'last' },
      },
    },
  })

  await client.task.findFirst({
    orderBy: [
      { title: 'asc' },
      {
        assignee: {
          createdAt: { sort: 'asc', nulls: 'first' },
        },
      },
    ],
  })

  await client.task.findMany({
    orderBy: {
      parent: {
        creator: {
          createdAt: { sort: 'desc', nulls: 'last' },
        },
      },
    },
  })

  await client.task.findMany({
    orderBy: { dueDate: { sort: 'asc', nulls: 'first' } },
  })
  await client.task.findMany({ orderBy: { createdAt: 'asc' } })

  const selected = await client.task.findMany({
    select: { title: true },
    orderBy: {
      assignee: {
        createdAt: { sort: 'desc', nulls: 'last' },
      },
    },
  })
  selected[0].title
  // @ts-expect-error selected payload stays narrow
  selected[0].createdAt

  const included = await client.task.findFirst({
    include: { assignee: true },
    orderBy: {
      assignee: {
        createdAt: { sort: 'desc', nulls: 'last' },
      },
    },
  })
  included?.assignee?.createdAt

  const computed = await extendedClient.task.findMany({
    select: { titleLength: true },
    orderBy: {
      assignee: {
        createdAt: { sort: 'desc', nulls: 'last' },
      },
    },
  })
  computed[0].titleLength
  // @ts-expect-error computed-only selection stays narrow
  computed[0].title

  await client.task.findMany({
    // @ts-expect-error required root scalar stays Prisma-typed
    orderBy: { createdAt: { sort: 'desc', nulls: 'last' } },
  })

  await client.task.findMany({
    // @ts-expect-error fully required relation path stays Prisma-typed
    orderBy: {
      creator: {
        createdAt: { sort: 'desc', nulls: 'last' },
      },
    },
  })
}

void acceptedQueries
`,
    )

    expectConsumerCompiles(consumerPath)

    const { speedExtension } = await import(join(outputDir, 'index.ts'))
    let executeOperation:
      | ((input: {
          model: string
          operation: string
          args: object
          query: (args: object) => Promise<object[]>
        }) => Promise<object>)
      | undefined
    const prisma = {
      $transaction: async () => [],
      $extends(extension: {
        query: {
          $allModels: {
            $allOperations: typeof executeOperation
          }
        }
      }) {
        executeOperation = extension.query.$allModels.$allOperations
        return extension
      },
    }
    const fallback = vi.fn(async () => [])

    speedExtension({
      fallbackOnError: true,
      postgres: {
        unsafe() {
          throw new Error('forced execution failure')
        },
      },
    })(prisma)

    if (!executeOperation) throw new Error('query extension was not registered')

    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    try {
      await expect(
        executeOperation({
          model: 'Task',
          operation: 'findMany',
          args: {
            orderBy: {
              assignee: {
                createdAt: { sort: 'desc', nulls: 'last' },
              },
            },
          },
          query: fallback,
        }),
      ).rejects.toThrow('prisma-sql cannot fall back')
      expect(fallback).not.toHaveBeenCalled()

      await expect(
        executeOperation({
          model: 'Task',
          operation: 'findMany',
          args: {
            orderBy: { assignee: { createdAt: 'desc' } },
          },
          query: fallback,
        }),
      ).resolves.toEqual([])

      await expect(
        executeOperation({
          model: 'Task',
          operation: 'findMany',
          args: {
            orderBy: {
              assignee: {
                name: { sort: 'asc', nulls: 'first' },
              },
            },
          },
          query: fallback,
        }),
      ).resolves.toEqual([])
    } finally {
      warn.mockRestore()
    }
    expect(fallback).toHaveBeenCalledTimes(2)
  })

  it('generates and compiles against a Prisma 7 client', async () => {
    const prismaDir = join(fixtureDir, 'prisma-7')
    const clientDir = join(prismaDir, 'client')
    const outputDir = join(prismaDir, 'sql')
    const schemaPath = join(prismaDir, 'schema.prisma')
    mkdirSync(prismaDir, { recursive: true })

    const schema = `generator client {
  provider = "prisma-client"
  output   = "./client"
}

datasource db {
  provider = "postgresql"
}

model Parent {
  id              Int            @id @default(autoincrement())
  requiredDate    DateTime
  nullableDate    DateTime?
  tags            String[]
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

    await execFileAsync(process.execPath, [
      resolve('node_modules/prisma/build/index.js'),
      'generate',
      `--schema=${schemaPath}`,
    ])

    const clientImportPath = getClientImportPath(
      {
        schemaPath,
        otherGenerators: [
          {
            provider: { value: 'prisma-client' },
            output: { value: './client' },
          },
        ],
      },
      outputDir,
    )
    expect(clientImportPath).toBe('../client/client')

    const dmmf = await getDMMF({ datamodel: schema })
    await generateClient({
      datamodel: dmmf.datamodel,
      outputDir,
      config: { dialect: 'postgres', skipInvalid: false },
      runtimeImportPath: resolve('src/index'),
      clientImportPath,
      plannerArtifacts,
    })

    const generatedSource = readFileSync(join(outputDir, 'index.ts'), 'utf8')
    expect(generatedSource).toContain(
      `from ${JSON.stringify(clientImportPath)}`,
    )

    const consumerPath = join(outputDir, 'consumer.ts')
    writeFileSync(
      consumerPath,
      `import { PrismaClient } from ${JSON.stringify(join(clientDir, 'client'))}
import { PrismaPg } from '@prisma/adapter-pg'
import type { SpeedClient } from './index'

const baseClient = new PrismaClient({
  adapter: new PrismaPg({ connectionString: 'postgres://test:test@localhost/test' }),
})
declare const client: SpeedClient<typeof baseClient>

async function acceptedQueries() {
  const baseSelected = await baseClient.parent.findMany({
    select: { id: true },
  })
  baseSelected[0].id
  // @ts-expect-error native Prisma 7 payload stays narrow
  baseSelected[0].requiredDate

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

    expectConsumerCompiles(consumerPath, ts.ModuleResolutionKind.Bundler)
  }, 30000)
})
