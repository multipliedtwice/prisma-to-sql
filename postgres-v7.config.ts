import { defineConfig } from 'prisma/config'

export default defineConfig({
  schema: 'tests/prisma/schema-postgres-v7.prisma',
  datasource: {
    url: 'postgres://postgres:postgres@localhost:5433/prisma_test',
  },
})
