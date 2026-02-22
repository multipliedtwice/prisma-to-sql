import { execSync } from 'child_process'
import { readFileSync, writeFileSync } from 'fs'
import path from 'path'

const version = process.argv[2]

if (!version || !['6', '7'].includes(version)) {
  console.error('Usage: tsx scripts/switch-prisma-version.ts <6|7>')
  process.exit(1)
}

const packageJsonPath = path.join(process.cwd(), 'package.json')
const pkg = JSON.parse(readFileSync(packageJsonPath, 'utf-8'))

if (version === '6') {
  pkg.dependencies['@prisma/client'] = '6.19.2'
  pkg.dependencies['prisma'] = '6.19.2'
  pkg.devDependencies['@prisma/adapter-better-sqlite3'] = '^6.19.2'
  pkg.devDependencies['@prisma/adapter-pg'] = '^6.19.2'
} else {
  pkg.dependencies['@prisma/client'] = '7.4.1'
  pkg.dependencies['prisma'] = '7.4.1'
  pkg.devDependencies['@prisma/adapter-better-sqlite3'] = '^7.4.1'
  pkg.devDependencies['@prisma/adapter-pg'] = '^7.4.1'
}

delete pkg.dependencies['@prisma/client-v7']
delete pkg.dependencies['prisma-v7']

writeFileSync(packageJsonPath, JSON.stringify(pkg, null, 2))

console.log(`Switched to Prisma ${version}`)
console.log('Installing dependencies...')

execSync('npm install', { stdio: 'inherit' })

console.log(`\nPrisma ${version} installed. Run tests with: npm test`)
