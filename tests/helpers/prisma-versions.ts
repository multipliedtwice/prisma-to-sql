export const PRISMA_VERSIONS = [6, 7, 8] as const

export type PrismaVersion = (typeof PRISMA_VERSIONS)[number]

export const PRISMA_PACKAGES: Record<
  PrismaVersion,
  { packageVersion: string; label: string }
> = {
  6: { packageVersion: '6.19.3', label: 'Prisma v6 (6.19.3)' },
  7: { packageVersion: '7.10.0', label: 'Prisma v7 (7.10.0)' },
  8: { packageVersion: '8.1.0-dev.1', label: 'Prisma v8 (8.1.0-dev.1)' },
}

interface PackageJson {
  dependencies?: Record<string, string>
  devDependencies?: Record<string, string>
}

export function isPrismaVersion(version: number): version is PrismaVersion {
  return PRISMA_VERSIONS.some((candidate) => candidate === version)
}

export function parsePrismaVersion(
  value: string | undefined,
  fallback: PrismaVersion = 6,
): PrismaVersion {
  const version = Number(value ?? fallback)
  if (isPrismaVersion(version)) return version
  throw new Error(`Unsupported Prisma version: ${value}`)
}

export function updatePrismaPackages<T extends PackageJson>(
  pkg: T,
  version: PrismaVersion,
) {
  const packageVersion = PRISMA_PACKAGES[version].packageVersion
  const dependencies = { ...pkg.dependencies }
  const devDependencies = { ...pkg.devDependencies }

  for (const packageName of [
    '@prisma/client',
    'prisma',
    '@prisma/client-v7',
    'prisma-v7',
  ]) {
    delete dependencies[packageName]
    delete devDependencies[packageName]
  }

  dependencies['@prisma/generator-helper'] = packageVersion
  dependencies['@prisma/internals'] = packageVersion
  devDependencies['@prisma/client'] = packageVersion
  devDependencies.prisma = packageVersion
  devDependencies['@prisma/adapter-better-sqlite3'] = packageVersion
  devDependencies['@prisma/adapter-pg'] = packageVersion

  return { ...pkg, dependencies, devDependencies }
}
