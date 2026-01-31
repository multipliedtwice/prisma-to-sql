import bcrypt from 'bcrypt';
import { Prisma, UserFlag, UserPermission } from '../../.generated/client';

export const users: Prisma.UserCreateManyInput[] = [
  {
    email: 'system@prisma.com',
    emailVerified: true,
    policyAgreed: true,
    username: 'System',
    countryCode: 'US',
    flag: UserFlag.GREEN,
    permissions: Object.values(UserPermission),
    password: bcrypt.hashSync('qwerty12345678', 10),
  },
];
