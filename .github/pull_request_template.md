## What and why
Work item: #

## Checklist
- [ ] Table changes are a NEW Liquibase changeset (nothing created in code, no merged changeset edited)
- [ ] Changeset has `runInTransaction: false` and a `rollback`
- [ ] Destructive DDL? Added the `confirm-destructive` label and explained why
- [ ] `metadata.yml` updated if tables/owners changed
- [ ] Tests added or updated; `pytest` passes locally
