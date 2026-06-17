## [1.89.1](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.89.0...v1.89.1) (2026-06-17)


### Bug Fixes

* Trigger CI pipeline ([815e566](https://github.com/multipliedtwice/prisma-to-sql/commit/815e566efd2822dff04fb136f5377e90ebd1b8a6))

# [1.89.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.88.6...v1.89.0) (2026-06-15)


### Features

* add orRewriteApplied property to SqlResult and related functions for union-of-ids support ([ff39368](https://github.com/multipliedtwice/prisma-to-sql/commit/ff39368236f429200970dff1d99add453dad0c67))

## [1.88.6](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.88.5...v1.88.6) (2026-06-14)


### Bug Fixes

* add ensurePkInSelect utility function and update resolveSingleSegment to utilize it ([00b9293](https://github.com/multipliedtwice/prisma-to-sql/commit/00b9293718ff2f2722bc8083a8a92491dabd9bd9))

## [1.88.5](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.88.4...v1.88.5) (2026-06-14)


### Bug Fixes

* remove debug logging condition in dlog function for consistent logging ([62863f9](https://github.com/multipliedtwice/prisma-to-sql/commit/62863f968ab0d5f19d87353cb0449a7e30d100d0))

## [1.88.4](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.88.3...v1.88.4) (2026-06-14)


### Bug Fixes

* enhance debug logging in where-in-resolver and code-emitter for better traceability ([ac4f96d](https://github.com/multipliedtwice/prisma-to-sql/commit/ac4f96d85fa5ca58ea1bb04536f68afab746f02e))

## [1.88.3](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.88.2...v1.88.3) (2026-06-14)


### Bug Fixes

* improve debug logging in resolveSingleSegment and generateExtension for better traceability ([8c7a5e6](https://github.com/multipliedtwice/prisma-to-sql/commit/8c7a5e68db1db4a7b4ad95990e40590cfc5b4ba1))

## [1.88.2](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.88.1...v1.88.2) (2026-06-13)


### Bug Fixes

* enhance debug logging in resolveSingleSegment function for better traceability ([2ac00b4](https://github.com/multipliedtwice/prisma-to-sql/commit/2ac00b47395cbfa7adecd2b0972c6ef0c4037e10))

## [1.88.1](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.88.0...v1.88.1) (2026-06-13)


### Bug Fixes

* enhance debug logging in generateExtension function for WHERE IN segments and query results ([49b91ee](https://github.com/multipliedtwice/prisma-to-sql/commit/49b91eeaae5cbe6f7fb6efa0117d0f038165d641))

# [1.88.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.87.0...v1.88.0) (2026-06-13)


### Features

* move debug logging for whereInSegments to after plan generation in generateExtension function ([1e7955a](https://github.com/multipliedtwice/prisma-to-sql/commit/1e7955a5d2ff2a2b780aca0461f153e3c7eec145))

# [1.87.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.86.0...v1.87.0) (2026-06-13)


### Features

* add debug logging for whereInSegments in generateExtension function ([9788a12](https://github.com/multipliedtwice/prisma-to-sql/commit/9788a12725636a64a711c33a14e1f666e85f55ca))

# [1.86.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.85.0...v1.86.0) (2026-06-13)


### Features

* add support for union-of-ids SQL rewrite and extend SqlBuildOptions ([fb870fc](https://github.com/multipliedtwice/prisma-to-sql/commit/fb870fc92f31fd798ac0fef5ed2c4440ce23707a))

# [1.85.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.84.0...v1.85.0) (2026-06-09)


### Features

* implement PostgreSQL model statistics collection and analysis ([a4d4ee9](https://github.com/multipliedtwice/prisma-to-sql/commit/a4d4ee9362647e5f3c57174b31ada04cd64027a6))

# [1.84.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.83.0...v1.84.0) (2026-06-07)


### Features

* remove json_agg from strategies as it was caused performance issues ([613be07](https://github.com/multipliedtwice/prisma-to-sql/commit/613be0751efebee01cb7b07f3babe4dfb7cb983e))

# [1.83.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.82.0...v1.83.0) (2026-05-05)


### Features

* enhance planner stats collection with runtime loading and incremental updates ([242f2ee](https://github.com/multipliedtwice/prisma-to-sql/commit/242f2ee7b6dea91e3f1ab9de25ff6a7190b96c4a))

# [1.82.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.81.0...v1.82.0) (2026-04-24)


### Features

* introduce configurable limits and strategy parameters for query builder ([fadedfb](https://github.com/multipliedtwice/prisma-to-sql/commit/fadedfbffb5bfeae3e302bfbed46abafa3e2b2a6))

# [1.81.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.80.4...v1.81.0) (2026-04-18)


### Features

* **includes.ts:** add hasRelationLikeValuesInArgs function to check for relation-like values in arguments ([e13f15e](https://github.com/multipliedtwice/prisma-to-sql/commit/e13f15e103dcb589507c2e0c98e251aba1fe979b))

## [1.80.4](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.80.3...v1.80.4) (2026-04-08)


### Bug Fixes

* release ([26b8a53](https://github.com/multipliedtwice/prisma-to-sql/commit/26b8a5348db3d95ccf5f9a43259441eb5d3d5620))

## [1.80.3](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.80.2...v1.80.3) (2026-03-29)


### Bug Fixes

* **includes.ts:** add check for nested relations in arguments to prevent unnecessary processing of nested models ([c8f7cdf](https://github.com/multipliedtwice/prisma-to-sql/commit/c8f7cdf89a9580700ebcddee21027911caaa7a73))

## [1.80.2](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.80.1...v1.80.2) (2026-03-25)


### Bug Fixes

* **pure-utils.ts:** add PRISMA_ONLY_PARAMS set to filter specific query parameters from the URL for better URL management and security ([2a91229](https://github.com/multipliedtwice/prisma-to-sql/commit/2a912293dc2914bd0da37f0d235808503ebe386b))

## [1.80.1](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.80.0...v1.80.1) (2026-03-24)


### Bug Fixes

* **collect-planner-stats.ts:** remove unnecessary export type and as const statements to clean up emitted CJS output ([0ad0df6](https://github.com/multipliedtwice/prisma-to-sql/commit/0ad0df6754f1b67ace5fa2ff3f00532a94d8cf41))

# [1.80.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.79.1...v1.80.0) (2026-03-24)


### Features

* **cardinality-planner.ts:** replace cleanDatabaseUrl with stripPrismaParams to remove query parameters from database URL ([6cba3ba](https://github.com/multipliedtwice/prisma-to-sql/commit/6cba3ba57ab7f29d5b1edba76616a9be066f48e2))

## [1.79.1](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.79.0...v1.79.1) (2026-03-23)


### Bug Fixes

* **cardinality-planner.ts:** use cleanDatabaseUrl function to sanitize database URL for PostgreSQL connection to ensure proper formatting and prevent potential connection issues ([3908054](https://github.com/multipliedtwice/prisma-to-sql/commit/3908054e64fe3e33740e052a82c380c7046991ea))

# [1.79.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.78.0...v1.79.0) (2026-03-17)

# [1.78.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.77.0...v1.78.0) (2026-03-16)


### Features

* **select.ts, code-emitter.ts:** add normalization for compound cursors to improve cursor handling in queries ([31ba2e2](https://github.com/multipliedtwice/prisma-to-sql/commit/31ba2e230df2162ba4a1c2375d8da4b48640fd72))

# [1.77.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.76.2...v1.77.0) (2026-03-08)


### Features

* **reducer.ts:** add safety checks for property names to prevent prototype pollution ([436e2d6](https://github.com/multipliedtwice/prisma-to-sql/commit/436e2d65287b3d922870112a18f401cbc5a5c5f2))

## [1.76.2](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.76.1...v1.76.2) (2026-03-08)


### Bug Fixes

* **index.ts:** remove unused import of buildReducerConfig and reduceFlatRows to clean up code ([a21cb98](https://github.com/multipliedtwice/prisma-to-sql/commit/a21cb989c1a89a3d90c6b7b22b2ba8f200cf6cf7))
* **tests:** update orderBy to support multiple fields in cursor test case for SQL injection handling ([d123f31](https://github.com/multipliedtwice/prisma-to-sql/commit/d123f31fdd438e9692eda94957beb9748874bd9a))

## [1.76.1](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.76.0...v1.76.1) (2026-02-22)

# [1.76.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.75.12...v1.76.0) (2026-02-21)


### Features

* **strategy-estimator.ts:** add dynamic parameter handling for pagination take values to enhance flexibility ([0760bef](https://github.com/multipliedtwice/prisma-to-sql/commit/0760bef10208ae4e95599d7257475083bf20257e))

## [1.75.12](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.75.11...v1.75.12) (2026-02-21)

## [1.75.11](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.75.10...v1.75.11) (2026-02-21)

## [1.75.10](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.75.9...v1.75.10) (2026-02-20)

## [1.75.9](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.75.8...v1.75.9) (2026-02-20)

## [1.75.8](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.75.7...v1.75.8) (2026-02-20)

## [1.75.7](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.75.6...v1.75.7) (2026-02-20)

## [1.75.6](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.75.5...v1.75.6) (2026-02-20)

## [1.75.5](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.75.4...v1.75.5) (2026-02-20)

## [1.75.4](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.75.3...v1.75.4) (2026-02-19)

## [1.75.3](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.75.2...v1.75.3) (2026-02-19)

## [1.75.2](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.75.1...v1.75.2) (2026-02-19)

## [1.75.1](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.75.0...v1.75.1) (2026-02-19)

# [1.75.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.74.0...v1.75.0) (2026-02-19)


### Features

* **select.ts:** enhance orderBy validation to support relations and improve error handling ([4501957](https://github.com/multipliedtwice/prisma-to-sql/commit/4501957ab180e5db41deaabb8474e1acb2de8021))

# [1.74.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.73.0...v1.74.0) (2026-02-19)


### Features

* **assembly.ts:** enhance count selection logic to include count from include spec for better query flexibility ([295f686](https://github.com/multipliedtwice/prisma-to-sql/commit/295f686661362750de24cb67e7e2aa749a4ae250))

# [1.73.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.72.0...v1.73.0) (2026-02-19)


### Features

* **select/includes.ts:** enhance nested to-one selects to handle nullable relations and add primary key support ([4af9909](https://github.com/multipliedtwice/prisma-to-sql/commit/4af990913d68a6922874bfcf301258888fc5e3a5))

# [1.72.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.71.0...v1.72.0) (2026-02-19)


### Features

* **select/includes.ts:** add support for _count in nested includes to enhance query capabilities ([40d4799](https://github.com/multipliedtwice/prisma-to-sql/commit/40d479920f6113d8f4a34783b98de1736ed654f7))

# [1.71.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.70.0...v1.71.0) (2026-02-18)

# [1.70.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.69.0...v1.70.0) (2026-02-18)

# [1.69.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.68.0...v1.69.0) (2026-02-18)


### Features

* flat join & reduce ([e4d1ef6](https://github.com/multipliedtwice/prisma-to-sql/commit/e4d1ef6d56f09dea97a384b50ffd0e8dd0445c7b))
* reduce code duplication ([2d98249](https://github.com/multipliedtwice/prisma-to-sql/commit/2d98249eae9a2de5393e13c2e7bc87026d1c8c66))

# [1.68.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.67.0...v1.68.0) (2026-02-16)


### Bug Fixes

* **streaming-progressive-reducer.ts:** add type assertion for return value of getCurrentParentKey to ensure correct type handling ([07c5f54](https://github.com/multipliedtwice/prisma-to-sql/commit/07c5f540a67f7034f745414f50ea7d3c9a45e47b))


### Features

* **pagination.ts:** enhance cursor condition logic to handle single cursor and order entries for better query performance ([6af0af4](https://github.com/multipliedtwice/prisma-to-sql/commit/6af0af43182222d265c66c8dd4e2eb13dc0aace3))
* **streaming-where-in-executor:** implement recursive fetching of child segments with depth control to prevent stack overflow ([aec4a6a](https://github.com/multipliedtwice/prisma-to-sql/commit/aec4a6ae9e467bc8a8a19ebfbecaccafee326e66))
* **tests:** enable debug mode in speedExtension for better query logging and performance analysis ([c6de95d](https://github.com/multipliedtwice/prisma-to-sql/commit/c6de95d802d95989f19a2e4df6d3b48e6130ebbf))

# [1.67.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.66.1...v1.67.0) (2026-02-15)


### Features

* **relations.ts:** optimize handling of NONE filter in buildListRelationFilters function to improve performance ([f8faeb0](https://github.com/multipliedtwice/prisma-to-sql/commit/f8faeb0c410a17d225af7adab6483e46df4ceafe))

## [1.66.1](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.66.0...v1.66.1) (2026-02-13)


### Bug Fixes

* typescript in generated file ([561964d](https://github.com/multipliedtwice/prisma-to-sql/commit/561964d87828b7b2d57f1722f7942f590d32b37b))

# [1.66.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.65.0...v1.66.0) (2026-02-13)


### Bug Fixes

* **flat-join.ts:** cast field to any type to resolve TypeScript type issues ([a1975cc](https://github.com/multipliedtwice/prisma-to-sql/commit/a1975cc3345c92ac61304d3e6629b342dee602c1))


### Features

* row by row reduce ([b01515d](https://github.com/multipliedtwice/prisma-to-sql/commit/b01515d562431c4b35b5728130ddff05cda5166c))

# [1.65.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.64.0...v1.65.0) (2026-02-10)


### Bug Fixes

* improve benchmark logic ([5a7cc82](https://github.com/multipliedtwice/prisma-to-sql/commit/5a7cc8264909a5a68838228dad81e34c3fd2737c))


### Features

* major refactor of benchmark logic and nested includes ([f2a0516](https://github.com/multipliedtwice/prisma-to-sql/commit/f2a051660e7dbcae6b6065fb41f609f497689411))

# [1.64.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.63.0...v1.64.0) (2026-02-09)

# [1.63.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.62.0...v1.63.0) (2026-02-09)

# [1.62.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.61.0...v1.62.0) (2026-02-08)


### Features

* **code-emitter.ts:** simplify import statements by consolidating imports from 'prisma-sql' to improve readability ([a92cb9b](https://github.com/multipliedtwice/prisma-to-sql/commit/a92cb9b86f45348cb9fa46483c17fc536dc01fd7))

# [1.61.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.60.0...v1.61.0) (2026-02-08)


### Features

* **index.ts:** enhance SQL module by adding batch and transaction utilities for improved query handling and execution ([1188ffd](https://github.com/multipliedtwice/prisma-to-sql/commit/1188ffd4a9b4f59c187864b4e2cdb01c38aa4b28))
* **transaction.ts:** change interface declarations from private to public to allow external access and improve usability ([af23086](https://github.com/multipliedtwice/prisma-to-sql/commit/af230868d76eb3840c3bb7dc3b9a7c0ab58a2fc2))

# [1.60.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.59.0...v1.60.0) (2026-02-08)

# [1.59.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.58.0...v1.59.0) (2026-02-07)

# [1.58.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.57.0...v1.58.0) (2026-02-07)


### Features

* batch method ([740b22a](https://github.com/multipliedtwice/prisma-to-sql/commit/740b22a299c380584e34c23ceb126bf4bc989ed9))

# [1.57.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.56.0...v1.57.0) (2026-02-07)


### Features

* **batch.ts:** enhance parseCountValue function to handle bigint type for improved number parsing and safety checks ([e593110](https://github.com/multipliedtwice/prisma-to-sql/commit/e5931103bd96edd33583b39c65fc84e85fa6a609))

# [1.56.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.55.0...v1.56.0) (2026-02-06)


### Features

* update batching ([e875bc9](https://github.com/multipliedtwice/prisma-to-sql/commit/e875bc9dccb2a3e5f18bc912080b878e008cbd36))

# [1.55.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.54.0...v1.55.0) (2026-02-06)


### Features

* batch and transactions ([f0db233](https://github.com/multipliedtwice/prisma-to-sql/commit/f0db233c713b6712a05524bbd1285020b54217a5))

# [1.54.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.53.0...v1.54.0) (2026-02-06)


### Features

* **code-emitter.ts:** implement batch processing and transaction support for Prisma queries to enhance performance and usability ([10b5479](https://github.com/multipliedtwice/prisma-to-sql/commit/10b54797f4b2bbeddd5bc5a024a514ae5754efdb))

# [1.53.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.52.0...v1.53.0) (2026-02-06)

# [1.52.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.51.0...v1.52.0) (2026-02-05)

# [1.51.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.50.3...v1.51.0) (2026-02-05)

## [1.50.3](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.50.2...v1.50.3) (2026-02-05)


### Bug Fixes

* **release.yml:** update artifact upload path from ./docs-site/dist to ./docs to ensure correct deployment of documentation site ([b9d848f](https://github.com/multipliedtwice/prisma-to-sql/commit/b9d848ff547bac2de8d4396a46d7ced10ff9b14b))

## [1.50.2](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.50.1...v1.50.2) (2026-02-05)


### Bug Fixes

* **release.yml:** update artifact path from ./docs/dist to ./docs-site/dist to reflect the correct directory structure ([ee7ca61](https://github.com/multipliedtwice/prisma-to-sql/commit/ee7ca61c5a1f767e433ebfc96a3939c0a13303b7))

## [1.50.1](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.50.0...v1.50.1) (2026-02-05)


### Bug Fixes

* **release.yml:** update artifact upload path from ./docs to ./docs/dist to ensure correct deployment of documentation ([58bf4ac](https://github.com/multipliedtwice/prisma-to-sql/commit/58bf4ac507426f7a54025c5af3998f58dd5fb89a))

# [1.50.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.49.0...v1.50.0) (2026-02-05)

# [1.49.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.48.1...v1.49.0) (2026-02-05)


### Features

* **assembly.ts:** refactor SQL building functions for improved readability and maintainability by simplifying string concatenation and removing unnecessary code ([88d2efe](https://github.com/multipliedtwice/prisma-to-sql/commit/88d2efeee739b708e58975f34073e7c18e98b6c0))
* **string-builder:** add StringBuilder class for efficient string manipulation ([54b2737](https://github.com/multipliedtwice/prisma-to-sql/commit/54b2737202684b3abda1388d1e3221304183d5c6))

## [1.48.1](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.48.0...v1.48.1) (2026-02-05)


### Bug Fixes

* **code-emitter.ts:** handle Date instances in transformEnumValues function to prevent incorrect transformations ([50cf1ea](https://github.com/multipliedtwice/prisma-to-sql/commit/50cf1ea06982d6efcf2d0cdc3941cc7b9ad37615))

# [1.48.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.47.0...v1.48.0) (2026-02-05)

# [1.47.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.46.0...v1.47.0) (2026-02-05)


### Features

* **code-emitter.ts:** add getByPath function to retrieve nested object values using dot notation ([bb79e9f](https://github.com/multipliedtwice/prisma-to-sql/commit/bb79e9f82d0959bf49f98d734c9d22ed9b3ed287))
* **code-emitter.ts:** add support for Date objects in transformEnumValues function to ensure proper handling of date instances ([7a34831](https://github.com/multipliedtwice/prisma-to-sql/commit/7a34831ae5c62d5d9b7fc11918010a45981e085d))

# [1.47.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.46.0...v1.47.0) (2026-02-05)


### Features

* **code-emitter.ts:** add getByPath function to retrieve nested object values using dot notation ([bb79e9f](https://github.com/multipliedtwice/prisma-to-sql/commit/bb79e9f82d0959bf49f98d734c9d22ed9b3ed287))
* **code-emitter.ts:** add support for Date objects in transformEnumValues function to ensure proper handling of date instances ([7a34831](https://github.com/multipliedtwice/prisma-to-sql/commit/7a34831ae5c62d5d9b7fc11918010a45981e085d))

# [1.47.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.46.0...v1.47.0) (2026-02-05)


### Features

* **code-emitter.ts:** add getByPath function to retrieve nested object values using dot notation ([bb79e9f](https://github.com/multipliedtwice/prisma-to-sql/commit/bb79e9f82d0959bf49f98d734c9d22ed9b3ed287))
* **code-emitter.ts:** add support for Date objects in transformEnumValues function to ensure proper handling of date instances ([7a34831](https://github.com/multipliedtwice/prisma-to-sql/commit/7a34831ae5c62d5d9b7fc11918010a45981e085d))

# [1.47.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.46.0...v1.47.0) (2026-02-05)


### Features

* **code-emitter.ts:** add getByPath function to retrieve nested object values using dot notation ([bb79e9f](https://github.com/multipliedtwice/prisma-to-sql/commit/bb79e9f82d0959bf49f98d734c9d22ed9b3ed287))

# [1.47.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.46.0...v1.47.0) (2026-02-05)


### Features

* **code-emitter.ts:** add getByPath function to retrieve nested object values using dot notation ([bb79e9f](https://github.com/multipliedtwice/prisma-to-sql/commit/bb79e9f82d0959bf49f98d734c9d22ed9b3ed287))

# [1.47.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.46.0...v1.47.0) (2026-02-05)


### Features

* **code-emitter.ts:** add getByPath function to retrieve nested object values using dot notation ([bb79e9f](https://github.com/multipliedtwice/prisma-to-sql/commit/bb79e9f82d0959bf49f98d734c9d22ed9b3ed287))

# [1.46.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.45.0...v1.46.0) (2026-02-05)

# [1.45.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.44.0...v1.45.0) (2026-02-05)


### Features

* rerun benchmarks ([cc4a49e](https://github.com/multipliedtwice/prisma-to-sql/commit/cc4a49ef8573cc946e9f10727add25d8b5b64388))
* update benchmark results ([fd347c9](https://github.com/multipliedtwice/prisma-to-sql/commit/fd347c9d018689c1ed3eb599c3f62125254e0c84))

# [1.44.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.43.0...v1.44.0) (2026-02-04)

# [1.43.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.42.0...v1.43.0) (2026-02-04)

# [1.42.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.41.0...v1.42.0) (2026-02-04)


### Features

* **select.ts:** add model parameter to buildOrderByClause for enhanced functionality ([f04ec40](https://github.com/multipliedtwice/prisma-to-sql/commit/f04ec4098eda637b1e121b635c7a1d9776fd12f7))

# [1.41.0](https://github.com/multipliedtwice/prisma-to-sql/compare/v1.40.0...v1.41.0) (2026-02-04)

# 1.0.0 (2025-11-28)


### Features

* **index.ts:** add main library exports and type definitions for better modularity and usability ([7c2a402](https://github.com/multipliedtwice/eav-to-prisma/commit/7c2a402149a63c768356cd34d47aaaf126f3c69f))
* **package.json:** add prompts package to enable interactive user input in the application ([e518107](https://github.com/multipliedtwice/eav-to-prisma/commit/e51810701e94c73b16dffd622907d623865be00f))
