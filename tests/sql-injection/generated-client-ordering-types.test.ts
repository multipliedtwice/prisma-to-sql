import {
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import ts from 'typescript'
import { Prisma } from '../generated/postgres/client'
import { generateClient } from '../../src/code-emitter'
import { getClientImportPath } from '../../src/generator-paths'

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

})
