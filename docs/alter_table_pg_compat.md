## ALTER TABLE -- PostgreSQL Compatibility

### RENAME (via `RenameStmt`)
- [x] `ALTER TABLE ... RENAME TO` -- rename a table
- [x] `ALTER VIEW ... RENAME TO` -- rename a view
- [x] `ALTER INDEX ... RENAME TO` -- rename an index
- [x] `ALTER FUNCTION ... RENAME TO` -- rename a function
- [x] `ALTER TABLE ... RENAME COLUMN ... TO` -- rename a column
- [x] `ALTER TABLE ... RENAME CONSTRAINT ... TO` -- rename a constraint

### Column operations (via `AlterTableStmt`)
- [ ] `ALTER TABLE ... ADD COLUMN` -- add a new column
- [ ] `ALTER TABLE ... DROP COLUMN` -- remove a column
- [ ] `ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE` -- change column type
- [ ] `ALTER TABLE ... ALTER COLUMN ... SET DEFAULT` / `DROP DEFAULT` -- set/remove default
- [ ] `ALTER TABLE ... ALTER COLUMN ... SET NOT NULL` / `DROP NOT NULL` -- nullability

### Constraint operations
- [x] `ALTER TABLE ... ADD CONSTRAINT CHECK` -- add CHECK constraint
- [x] `ALTER TABLE ... DROP CONSTRAINT` -- remove a named constraint
- [ ] `ALTER TABLE ... VALIDATE CONSTRAINT` -- validate a NOT VALID constraint

### Table-level operations
- [x] `ALTER TABLE ... SET SCHEMA` -- move table to different schema
- [ ] `ALTER TABLE ... OWNER TO` -- change table owner
- [ ] `ALTER TABLE ... SET` / `RESET` -- storage parameters

### Partitions
- [ ] `ALTER TABLE ... ATTACH PARTITION`
- [ ] `ALTER TABLE ... DETACH PARTITION`

### Triggers / Rules
- [ ] `ALTER TABLE ... ENABLE` / `DISABLE TRIGGER`
- [ ] `ALTER TABLE ... ENABLE` / `DISABLE RULE`
