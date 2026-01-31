import { EXTENSION_OPTIONS, PRISMA_OPTIONS } from 'src/config';
import { PrismaClient, UserPermission } from '../prisma/.generated/client';
import { speedExtension } from '../prisma/.generated/sql';

async function reproduce() {
  const prisma = new PrismaClient<typeof PRISMA_OPTIONS>(PRISMA_OPTIONS);
  const client = prisma.$extends(speedExtension(EXTENSION_OPTIONS));

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

  console.log(user);
}

reproduce();
