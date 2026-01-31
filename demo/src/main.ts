import { EXTENSION_OPTIONS, PRISMA_OPTIONS } from 'src/config';
import { PrismaClient, UserPermission } from '../prisma/.generated/client';
import { speedExtension } from '../prisma/.generated/sql';

async function reproduce() {
  const prisma = new PrismaClient<typeof PRISMA_OPTIONS>(PRISMA_OPTIONS);
  
  const client = prisma
  ?.$extends(
    speedExtension({
      ...EXTENSION_OPTIONS,
      onQuery: (info) => {
        console.log(`⏱️  ${info.model}.${info.method}: ${info.duration}ms ${info.prebaked ? '⚡ PREBAKED' : '🔨 RUNTIME'}`);
      }
    })
  );

  console.log('Starting query...\n');
  
  const startTime = performance.now();
  
  const user = await client.user.findFirst({
    where: {
      kickId: null,
      country: { countryCode: 'US' },
      permissions: { has: UserPermission.USERS },
      email: { contains: 'system', mode: 'insensitive' },
    },
    select: {
      id: true,
      isDeleted: true,
      permissions: true,
      country: { select: { countryNameEn: true } },
    },
  });

  const endTime = performance.now();
  const totalTime = endTime - startTime;

  console.log('\n✅ Result:', user);
  console.log(`\n📊 Total execution time: ${totalTime.toFixed(2)}ms`);
}

reproduce();