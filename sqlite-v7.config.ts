import { defineConfig } from 'prisma/config'

export default defineConfig({
  schema: 'tests/prisma/schema-sqlite-v7.prisma',
  datasource: {
    url: 'file:./tests/prisma/db.sqlite',
  },
})
