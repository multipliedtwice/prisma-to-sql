import { execSync } from 'child_process'
import { readFileSync, writeFileSync } from 'fs'
import path from 'path'
import {
  isPrismaVersion,
  updatePrismaPackages,
} from './prisma-versions'

const version = Number(process.argv[2])

if (!isPrismaVersion(version)) {
  console.error('Usage: tsx tests/helpers/switch-prisma-version.ts <6|7|8>')
  process.exit(1)
}

const packageJsonPath = path.join(process.cwd(), 'package.json')
const pkg = JSON.parse(readFileSync(packageJsonPath, 'utf-8'))
const updatedPkg = updatePrismaPackages(pkg, version)

writeFileSync(packageJsonPath, JSON.stringify(updatedPkg, null, 2) + '\n')

console.log(`Switched to Prisma ${version}`)
console.log('Installing dependencies...')

execSync('npm install', { stdio: 'inherit' })

console.log(`\nPrisma ${version} installed. Run tests with: npm test`)
