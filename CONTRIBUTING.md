# Contributing to eav-to-prisma

Thank you for your interest in contributing! 

## Development Setup
```bash
git clone https://github.com/multipliedtwice/eav-to-prisma.git
cd eav-to-prisma
yarn install
```

## Development Workflow

1. Create a feature branch: `git checkout -b feat/my-feature`
2. Make your changes
3. Run tests: `yarn test`
4. Run type check: `yarn type-check`
5. Commit using conventional commits: `feat: add new feature` or [open-commit cli](https://github.com/di-sukharev/opencommit)
6. Push and create a PR


## Testing
```bash
yarn test          # Watch mode
yarn test:run      # Single run
yarn test:coverage # With coverage
```

## Pull Request Process

1. Update documentation if needed
2. Add tests for new features
3. Ensure all tests pass
4. Update CHANGELOG.md if applicable
5. Request review from maintainers

## Code Style

- Use TypeScript strict mode
- Follow existing code patterns
- Keep functions small and focused

## Questions?

Open a [Discussion](https://github.com/multipliedtwice/eav-to-prisma/discussions) or reach out to maintainers.
```

### 3. **.npmignore** (Redundant but safer)
```
# Source files
src/
tests/
example/

# Config files
*.config.ts
*.config.js
tsconfig.json
vitest.config.ts
.releaserc.json

# Development
.github/
.vscode/
coverage/
*.log
*.tsbuildinfo

# Testing
test-output/
test-temp/
*.test.ts
*.spec.ts

# Documentation development
docs/