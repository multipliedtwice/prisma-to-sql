import { dirname, join, relative, resolve, sep } from 'path'

interface GeneratorPathEntry {
  provider: { value: string | null }
  output?: { value: string | null } | null
}

interface ClientImportPathOptions {
  schemaPath: string
  otherGenerators: GeneratorPathEntry[]
}

export function getClientImportPath(
  options: ClientImportPathOptions,
  outputDir: string,
): string {
  const clientGenerator = options.otherGenerators.find(
    (generator) =>
      generator.provider.value === 'prisma-client-js' ||
      generator.provider.value === 'prisma-client',
  )

  if (!clientGenerator?.output?.value) return '@prisma/client'

  const schemaDir = dirname(options.schemaPath)
  const clientOutput = resolve(schemaDir, clientGenerator.output.value)
  const clientEntry =
    clientGenerator.provider.value === 'prisma-client'
      ? join(clientOutput, 'client')
      : clientOutput
  const importPath = relative(outputDir, clientEntry).split(sep).join('/')

  return importPath.startsWith('.') ? importPath : `./${importPath}`
}
