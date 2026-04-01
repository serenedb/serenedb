## ALTER TABLE — PostgreSQL Compatibility

### RENAME (via `RenameStmt`)
- [x] `ALTER TABLE ... RENAME TO` — rename a table
- [x] `ALTER VIEW ... RENAME TO` — rename a view
- [ ] `ALTER INDEX ... RENAME TO` — rename an index
- [ ] `ALTER FUNCTION ... RENAME TO` — rename a function
- [ ] `ALTER TABLE ... RENAME COLUMN ... TO` — rename a column
- [ ] `ALTER TABLE ... RENAME CONSTRAINT ... TO` — rename a constraint

### Column operations (via `AlterTableStmt`)
- [ ] `ALTER TABLE ... ADD COLUMN` — add a new column
- [ ] `ALTER TABLE ... DROP COLUMN` — remove a column
- [ ] `ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE` — change column type
- [ ] `ALTER TABLE ... ALTER COLUMN ... SET DEFAULT` / `DROP DEFAULT` — set/remove default
- [ ] `ALTER TABLE ... ALTER COLUMN ... SET NOT NULL` / `DROP NOT NULL` — nullability

### Constraint operations
- [ ] `ALTER TABLE ... ADD CONSTRAINT` — add CHECK, UNIQUE, PRIMARY KEY, FOREIGN KEY
- [ ] `ALTER TABLE ... DROP CONSTRAINT` — remove a named constraint
- [ ] `ALTER TABLE ... VALIDATE CONSTRAINT` — validate a NOT VALID constraint

### Table-level operations
- [ ] `ALTER TABLE ... SET SCHEMA` — move table to different schema
- [ ] `ALTER TABLE ... OWNER TO` — change table owner
- [ ] `ALTER TABLE ... SET` / `RESET` — storage parameters

### Partitions
- [ ] `ALTER TABLE ... ATTACH PARTITION`
- [ ] `ALTER TABLE ... DETACH PARTITION`

### Triggers / Rules
- [ ] `ALTER TABLE ... ENABLE` / `DISABLE TRIGGER`
- [ ] `ALTER TABLE ... ENABLE` / `DISABLE RULE`
