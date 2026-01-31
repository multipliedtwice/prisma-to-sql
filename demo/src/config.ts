import { PrismaPg } from '@prisma/adapter-pg';
import 'dotenv/config';
import os from 'os';
import postgres from 'postgres';
import { PrismaClientOptions } from 'prisma/.generated/internal/prismaNamespace';
import { speedExtension } from 'prisma/.generated/sql';

export const PRISMA_OPTIONS: PrismaClientOptions = {
  adapter: new PrismaPg({
    keepAlive: true,
    connectionString: process.env.DATABASE_URL,

    min: Math.min(os.availableParallelism() * 1, 10),
    max: Math.min(os.availableParallelism() * 2 + 1, 20),

    idle_in_transaction_session_timeout: 30_000,
    options: '-c random_page_cost=1.1 -c seq_page_cost=1.0 -c work_mem=64MB -c temp_buffers=16MB',
  }),

  log: ['info', 'warn', 'error'],
  omit: { user: { password: true } },
  transactionOptions: { maxWait: 5_000, timeout: 10_000 },
};

export const EXTENSION_OPTIONS: Parameters<typeof speedExtension>[0] = {
  debug: true,
  postgres: postgres(process.env.DATABASE_URL, {
    keep_alive: 10_000,
    max: Math.min(os.availableParallelism() * 2 + 1, 20),
  }),
};
