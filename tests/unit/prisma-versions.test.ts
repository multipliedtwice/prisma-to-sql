import { describe, expect, it } from 'vitest'
import {
  PRISMA_PACKAGES,
  PRISMA_VERSIONS,
  parsePrismaVersion,
  updatePrismaPackages,
} from '../helpers/prisma-versions'

describe('Prisma benchmark versions', () => {
  it('uses the supported reproducible package sets', () => {
    expect(PRISMA_VERSIONS).toEqual([6, 7, 8])
    expect(PRISMA_PACKAGES).toEqual({
      6: { packageVersion: '6.19.3', label: 'Prisma v6 (6.19.3)' },
      7: { packageVersion: '7.10.0', label: 'Prisma v7 (7.10.0)' },
      8: {
        packageVersion: '8.1.0-dev.1',
        label: 'Prisma v8 (8.1.0-dev.1)',
      },
    })
  })

  it('switches every Prisma package to the selected exact version', () => {
    const pkg = updatePrismaPackages(
      {
        name: 'prisma-sql',
        dependencies: {
          '@prisma/client-v7': 'npm:@prisma/client@7.4.1',
          'prisma-v7': 'npm:prisma@7.4.1',
        },
        devDependencies: {},
      },
      8,
    )

    expect(pkg.name).toBe('prisma-sql')
    expect(pkg.dependencies).toEqual({
      '@prisma/generator-helper': '8.1.0-dev.1',
      '@prisma/internals': '8.1.0-dev.1',
    })
    expect(pkg.devDependencies).toEqual({
      '@prisma/client': '8.1.0-dev.1',
      prisma: '8.1.0-dev.1',
      '@prisma/adapter-better-sqlite3': '8.1.0-dev.1',
      '@prisma/adapter-pg': '8.1.0-dev.1',
    })
  })

  it('parses v8 without collapsing it to v7', () => {
    expect(parsePrismaVersion('8')).toBe(8)
    expect(() => parsePrismaVersion('9')).toThrow('Unsupported Prisma version')
  })
})
