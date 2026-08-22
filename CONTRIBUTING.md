# Contributing to prisma-to-sql

Thank you for your interest in contributing! 

## Development Setup
```bash
git clone https://github.com/multipliedtwice/prisma-to-sql.git
cd prisma-to-sql
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

Open a [Discussion](https://github.com/multipliedtwice/prisma-to-sql/discussions) or reach out to maintainers.

## Maintainers

- Releases run only from CI (`semantic-release` on `main`). No manual `npm publish`.
- npm publishing rights are held by the CI workflow (provenance enabled), not by personal accounts.
- A regular contributor becomes a co-maintainer after:
  1. several substantial merged PRs touching the query builder or planner,
  2. demonstrated ability to run the full test suite and reproduce release builds from a clean clone,
  3. explicit invitation from an existing maintainer.
- Behavioral or cost-model decisions (strategy selection, defaults, limits) should be recorded as short ADRs under `docs/adr/` so the reasoning does not depend on one person's memory.
