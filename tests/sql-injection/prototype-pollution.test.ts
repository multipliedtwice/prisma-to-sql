import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/client'
import { createToSQL } from '../../src'
import { convertDMMFToModels } from '@dee-wan/schema-parser'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'

describe('SQL Injection - Prototype Pollution Protection', () => {
  let prisma: PrismaClient
  let toSQL: ReturnType<typeof createToSQL>

  beforeAll(() => {
    prisma = new PrismaClient()
    const models = convertDMMFToModels(Prisma.dmmf.datamodel as DMMF.Datamodel)
    setGlobalDialect('postgres')
    toSQL = createToSQL(models, 'postgres')
  })

  afterAll(async () => {
    await prisma.$disconnect()
  })

  describe('Where Clause Prototype Pollution', () => {
    it('should reject __proto__ in where clause', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: {
            ['__proto__' as any]: 'malicious',
          },
        })
      }).toThrow(/does not exist/i)
    })

    it('should reject constructor in where clause', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: {
            ['constructor' as any]: 'malicious',
          },
        })
      }).toThrow(/does not exist/i)
    })

    it('should reject prototype in where clause', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: {
            ['prototype' as any]: 'malicious',
          },
        })
      }).toThrow(/does not exist/i)
    })

    it('should reject __defineGetter__ in where clause', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: {
            ['__defineGetter__' as any]: 'malicious',
          },
        })
      }).toThrow(/does not exist/i)
    })

    it('should reject __defineSetter__ in where clause', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: {
            ['__defineSetter__' as any]: 'malicious',
          },
        })
      }).toThrow(/does not exist/i)
    })

    it('should reject __lookupGetter__ in where clause', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: {
            ['__lookupGetter__' as any]: 'malicious',
          },
        })
      }).toThrow(/does not exist/i)
    })

    it('should reject __lookupSetter__ in where clause', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: {
            ['__lookupSetter__' as any]: 'malicious',
          },
        })
      }).toThrow(/does not exist/i)
    })
  })

  describe('Select Clause Prototype Pollution', () => {
    it('should reject __proto__ in select', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          select: {
            ['__proto__' as any]: true,
          },
        })
      }).toThrow(/unknown field/i)
    })

    it('should reject constructor in select', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          select: {
            ['constructor' as any]: true,
          },
        })
      }).toThrow(/unknown field/i)
    })

    it('should reject prototype in select', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          select: {
            ['prototype' as any]: true,
          },
        })
      }).toThrow(/unknown field/i)
    })
  })

  describe('Include Clause Prototype Pollution', () => {
    it('should safely ignore __proto__ in include', () => {
      // Include silently ignores non-relation fields (safe behavior)
      const { sql } = toSQL('User', 'findMany', {
        include: {
          ['__proto__' as any]: true,
        },
      })

      expect(sql).toBeDefined()
      expect(sql).not.toContain('__proto__')
    })

    it('should safely ignore constructor in include', () => {
      const { sql } = toSQL('User', 'findMany', {
        include: {
          ['constructor' as any]: true,
        },
      })

      expect(sql).toBeDefined()
      expect(sql).not.toContain('constructor')
    })
  })

  describe('OrderBy Clause Prototype Pollution', () => {
    it('should reject __proto__ in orderBy', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          orderBy: {
            ['__proto__' as any]: 'asc',
          },
        })
      }).toThrow(/unknown field/i)
    })

    it('should reject constructor in orderBy', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          orderBy: {
            ['constructor' as any]: 'desc',
          },
        })
      }).toThrow(/unknown field/i)
    })
  })

  describe('Distinct Clause Prototype Pollution', () => {
    it('should reject __proto__ in distinct', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          distinct: ['__proto__' as any],
        })
      }).toThrow(/unknown field/i)
    })

    it('should reject constructor in distinct', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          distinct: ['constructor' as any],
        })
      }).toThrow(/unknown field/i)
    })
  })

  describe('Cursor Clause Prototype Pollution', () => {
    it('should reject __proto__ in cursor', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          cursor: {
            ['__proto__' as any]: 1,
          },
          take: 10,
        })
      }).toThrow(/unknown field|does not exist/i)
    })

    it('should reject constructor in cursor', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          cursor: {
            ['constructor' as any]: 1,
          },
          take: 10,
        })
      }).toThrow(/unknown field|does not exist/i)
    })
  })

  describe('Aggregate Clause Prototype Pollution', () => {
    it('should reject __proto__ in aggregate _count', () => {
      expect(() => {
        toSQL('User', 'aggregate', {
          _count: {
            ['__proto__' as any]: true,
          },
        })
      }).toThrow(/unknown field|does not exist/i)
    })

    it('should reject constructor in aggregate _sum', () => {
      expect(() => {
        toSQL('Task', 'aggregate', {
          _sum: {
            ['constructor' as any]: true,
          },
        })
      }).toThrow(/unknown field|does not exist/i)
    })
  })

  describe('GroupBy Clause Prototype Pollution', () => {
    it('should reject __proto__ in groupBy by', () => {
      expect(() => {
        toSQL('Task', 'groupBy', {
          by: ['__proto__' as any],
        })
      }).toThrow(/unknown field/i)
    })

    it('should reject constructor in groupBy by', () => {
      expect(() => {
        toSQL('Task', 'groupBy', {
          by: ['constructor' as any],
        })
      }).toThrow(/unknown field/i)
    })

    it('should reject __proto__ in groupBy having', () => {
      expect(() => {
        toSQL('Task', 'groupBy', {
          by: ['projectId'],
          having: {
            ['__proto__' as any]: {
              _avg: { gt: 10 },
            },
          },
        })
      }).toThrow()
    })
  })

  describe('Nested Prototype Pollution', () => {
    it('should reject __proto__ in nested where conditions', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: {
            assignedTasks: {
              some: {
                ['__proto__' as any]: 'malicious',
              },
            },
          },
        })
      }).toThrow(/does not exist/i)
    })

    it('should reject constructor in nested select', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          select: {
            id: true,
            assignedTasks: {
              select: {
                ['constructor' as any]: true,
              },
            },
          },
        })
      }).toThrow(/unknown field/i)
    })

    it('should reject __proto__ in deeply nested structures', () => {
      expect(() => {
        toSQL('Organization', 'findMany', {
          where: {
            projects: {
              some: {
                tasks: {
                  some: {
                    ['__proto__' as any]: 'malicious',
                  },
                },
              },
            },
          },
        })
      }).toThrow(/does not exist/i)
    })
  })
})
