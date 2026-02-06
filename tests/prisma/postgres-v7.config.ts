import { defineConfig } from '@prisma/client'

export default defineConfig({
  schema: './schema-postgres-v7.prisma',
  output: '../generated/postgres-v7'
})