import 'dotenv/config';
import path from 'path';
import { defineConfig } from 'prisma/config';

export default defineConfig({
  datasource: { url: process.env.DATABASE_URL },
  views: { path: path.join('prisma', 'views') },
  typedSql: { path: path.join('prisma', 'queries') },
  schema: path.join('prisma', 'schema'),
  migrations: {
    path: path.join('prisma', 'migrations'),
    seed: 'npx tsx prisma/seed/seed.ts',
  },
});
