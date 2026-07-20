# RBAC / auth problems

Tracked RBAC + auth gaps and bugs. Every entry is validated against the **current**
working tree (not a stale audit) — status, evidence, fix size, and the code seam.

Validated 2026-07-08 (fresh: 5 parallel code+smoke agents for the feature gaps; direct
code verification for the pasted security-audit list).

Fix-size key: **S** = one file / few lines · **M** = one subsystem, needs a test ·
**L** = new feature surface (grammar + transformer + catalog + tests).

---

## Open — real gaps (validated live)

### Security-relevant enforcement holes (PG denies, we don't)
- **Schema USAGE not enforced** — **M**. A role with SELECT on a table but no USAGE on
  its schema can still read it (PG raises "permission denied for schema X" first).
  Verified live. Seam: `server/catalog/catalog.cpp` snapshot getters `GetRelation`
  (~1159), `GetType` (~1143), `GetFunction` (~1177), `GetTokenizer` (~1192),
  `GetSequence` (~1223) — after resolving `schema_id`, and only when
  `ax.need != AclMode::NoRights`, do `RequireAccess(ax.role, *GetObject<Schema>(schema_id),
  AclMode::Usage)`. (Bulk listers `GetTables`/etc. are unchecked-by-design; callers filter.)
- **TEMP (CreateTemp) not enforced** — **M**. `CREATE TEMP TABLE` succeeds after
  `REVOKE TEMP ON DATABASE ... FROM r` (PG denies). Verified live. No `.Can(...,CreateTemp)`
  exists anywhere. Seam: `server/network/pg/pg_wire_session.cpp` where `IsTemporaryCreate`
  (~212) is computed — check `CreateTemp` on the connection's database, mirroring the
  `Connect` check in `connection_context.cpp:48`. (Temp tables bypass the catalog.)

### Missing PG features that break real deployments
- **ALTER DEFAULT PRIVILEGES never applied** — **M** (tables+seqs) / **L** (all types).
  ADP parses, persists, shows in `pg_default_acl`, but no `Create*` path reads it, so
  grantees get nothing (`has_table_privilege` → f after an ADP grant + CREATE TABLE;
  verified). Seam: in `Catalog::CreateTable` (~2560, after `owner = ax.role`) and
  `CreateSequence`, merge the owner role's `default_acls` for `(schema_id | kInvalid,
  objtype)` into the new object's `Permissions` via a shared `DefaultAclFor(...)` helper.
  REVOKE-defaults come for free if the full stored acl is merged.
- **Predefined roles: all 16 missing + `pg_` prefix not reserved** — tier (a) **S/M**,
  tier (b) **M/L**. `CREATE ROLE pg_test` wrongly succeeds; `GRANT pg_read_all_data TO r`
  confers nothing (verified). (a) bootstrap the 16 `pg_*` roles at fixed ids
  (`kMinSystem..kMaxSystem`, NOLOGIN/INHERIT, idempotent) + reject `CREATE ROLE pg_*` in
  `pg::CreateRole` (rbac.cpp:179). (b) enforce powers via new `RoleClosure` flags
  `read_all_data`/`write_all_data`/`monitor` set in `ComputeRoleClosure` (~108, inherited,
  like `is_superuser`) and short-circuited in `Can`/`CanAny`/`CanColumns`/`CanAnyColumn`,
  object-type-scoped. (a) is a prerequisite for (b); shipping (a) alone is a
  "silent privilege illusion" footgun — land them together.
- **DROP OWNED BY / REASSIGN OWNED BY missing** — **L**. Both are syntax errors; keywords
  exist but no grammar rules. Needed to offboard a role (DROP ROLE correctly refuses while
  the role owns objects). New statement family (grammar + transformer + pragma) + new
  `Catalog::DropOwned`/`ReassignOwned` traversing `RoleDependency::referencing_objects`
  (already maintained + counted) filtered to `GetOwner()==role`, plus grantor re-point for
  REASSIGN. DROP ROLE depe ndency refusal works + is reusable.
- **ALTER DATABASE / FUNCTION ... OWNER TO unsupported** — **S each**. Syntax errors;
  ownership frozen at creation. Add `DATABASE`/`FUNCTION` to `OwnerObjectType`
  (rbac.gram:39) + regen inlined grammar; the pragma/`FromPgObjectTypeName` already map
  both; add one `ChangeOwner` branch each in `catalog.cpp` (~3112): FUNCTION mirrors the
  `PgSqlType` branch (Type→Function), DATABASE mirrors `ChangeAcl`'s Database branch.
- **`SET LOCAL ROLE` / `SET LOCAL SESSION AUTHORIZATION` = syntax error** — **S**.
  `SET ROLE` and `SET LOCAL <generic setting>` work; only the LOCAL+ROLE combo fails.
  `set.gram`: add optional `'LOCAL'?` to `SetRole` (:20) + `SetSessionAuthorization` (:21),
  carry a `local` flag in the transformer, regen grammar. Used by connection poolers.
- **`password_encryption` GUC missing** — **S**. `SHOW password_encryption` errors; common
  libpq/`\password` probe. Add one entry to `config_variables.cpp` (mirror
  `scram_iterations` ~516; default `"scram-sha-256"`, allow SET).
- **`row_security` GUC missing** — **S**. Every `pg_dump` header (`SET row_security = off`)
  errors. Add the GUC to `config_variables.cpp`. (RLS itself is a separate feature.)
- **CREATE DOMAIN not implemented** — **L**. `CREATE DOMAIN d AS int` is a syntax error;
  no domain rule in the parser. Full feature: grammar + transformer + catalog domain type
  (base type + constraints) + constraint enforcement + `pg_type` `typtype='d'` wiring
  (enum already exists). Blocks the DOMAIN half of GRANT USAGE (a 1-line
  `FromPgObjectTypeName` "DOMAIN"→PgSqlType fix, but dead until CREATE DOMAIN lands).
- **RLS has no runtime** — **L** / half-landed. Policy DDL parses to unregistered pragmas
  and hard-errors (fail-closed — no silent all-rows leak). See [[project_rls_implementation]].
- **HBA methods peer/ident/ldap/gss/cert parse but never execute** — tiered. The full PG
  method vocabulary parses (`ParseMethod`, hba.cpp), but only trust/reject/scram-sha-256/
  md5/password run; a rule naming the others matches and then refuses the connection
  (`MethodExecutability` → `DeferredPeerIdent`/`RejectAtConnect`, fatal at the
  pg_wire_session `DKind::Unsupported`/`DKind::DeferredPeerIdent` cases). So a
  pg_hba.conf carried over from real PG (packaged default = `local all all peer`) locks
  every local user out instead of OS-authenticating them. By effort:
  - **peer** — **M**, the valuable one. `SO_PEERCRED` on the unix socket → `getpwuid` →
    compare against the role name. Seam: capture peer creds in `CapturePeerAddress`
    (unix branch), thread into `ClientInfo`, execute at the `DeferredPeerIdent` site.
    Full fidelity also needs `pg_ident.conf` user maps (`map=`), which we lack entirely.
  - **cert** — **M**. Listener TLS + `RequireClientCert` (sslmode=verify-*) already
    exist; wire the verified client-cert CN into the HBA decision instead of refusing.
  - **ldap / gss** — **L each**. New dependencies (LDAP bind client / Kerberos); only
    worth doing on demand.
  - **ident (RFC 1413)** — skip; PG keeps it for legacy, nobody deploys it.

### Corrections to the older audit (NOT gaps — do not re-report)
- **DATABASE CONNECT is enforced** at connect (`RequireLoginRole`,
  connection_context.cpp:48; called from pg_wire_session.cpp:400 + http/session.h:150).
- **TYPE USAGE is enforced** at type-use (verified: `'x'::mood` → permission denied after
  REVOKE USAGE). The `NoAccessCheck` `GetType` in `duckdb_entry_cache.cpp` is the
  metadata/DDL cache path, not the query path.

---

## Validated FIXED — pasted "🔴 exploitable today" audit was STALE (do NOT re-open)

All six were verified fixed against current code on 2026-07-08 (and predate the July-2026
six-fix work + the 2026-07-08 auth-flag session). Recorded here so the stale audit doesn't
resurface as live bugs.

1. **`disable_optimizer` RBAC bypass** — FIXED. `db.config.access_check_function =
   &CollectAndEnforce` (rbac.cpp:263) runs **unconditionally** at bind (client_context.cpp
   :520-521), *before* the `if (optimize && RequireOptimizer())` gate (:531).
2. **MERGE records no write privilege** — FIXED. bind_merge_into.cpp:288-312 records the
   write access each WHEN action needs via `RecordAccess`.
3. **CREATEROLE can grant SUPERUSER/etc.** — FIXED. `RequireAttributesGrantable` per-attribute
   gate (catalog.cpp:1895 create, 2952 alter; :2161 requires the actor to hold each attr).
4. **Passwordless roles trust-login under a scram HBA rule** — FIXED (+ hardened this
   session). Trust fallback is now `if (ruleset->is_default && loopback)` (pg_wire_session.cpp
   :992); remote/passwordless fails closed with "no password is set for role X".
5. **`SET hba` has no superuser gate** — FIXED. Callback gates on `!...is_superuser` →
   "permission denied to set parameter hba" (config_variables.cpp:101).
6. **Pre-hashed PASSWORD double-hashed** — FIXED. `MakePasswordVerifier` stores verbatim when
   `IsScramVerifier`/`IsMd5Verifier` (rbac.cpp:166).

Also confirmed already-fixed/invalid: `has_table_privilege` on system relations (has a
`ResolveSystemRelation` fallback); `REVOKE ... CASCADE` (parses, `TransformCascade`);
`CatalogStore::Shutdown` "settings prepared-statement leak" (no such `_setting*` members
exist in store.{h,cpp} — statements the audit references are gone).

---

## Lower-severity + code-quality claims — validated (3 agents + direct code check)

**Newly-confirmed OPEN (add to the fix backlog):**
- **`ALTER DEFAULT PRIVILEGES FOR ROLE a,b IN SCHEMA s1,s2` → only the first of each list**
  — **M**. The transformer takes `[0]` of each list (transform_rbac.cpp:985/991; the comment
  admits it) and the pragma carries scalars. Verified: `pg_default_acl` gets 1 row, PG gets 4.
  Fix: carry roles/schemas as LIST args, loop `roles × schemas` in `AlterDefaultPrivileges`.
- **DROP SCHEMA strands schema-scoped default-ACL rows → grantee role undroppable forever**
  — **M**. Default ACLs live on the Role keyed by schema; `DropSchema` (catalog.cpp:3810) never
  prunes them, so the `RoleDependency` edge persists and `DROP ROLE` refuses forever. Verified.
  Fix: in the DropSchema `Apply`, sweep roles' `default_acls` with `.schema == schema_id`, erase +
  `RebuildRoleClosures`.
- **`CREATE ROLE public` / `none` not reserved** — **S**. Both wrongly succeed (verified); no
  create-time guard. PG rejects (`42939 reserved_name`). Fix: guard in `pg::CreateRole` (rbac.cpp:179).
- **DROP ROLE refusal reports a bare dependency count, not PG's itemized DETAIL** — **S–M**.
  catalog.cpp:3737 emits `<N> object(s) … depend on role`. There's a ready-made
  `FormatDependentsDetail` (catalog.cpp:1734) used by every other DROP path to itemize; DROP ROLE
  was left on the count shortcut.

**INVALID / already-clean (do NOT add):**
- prepared statements re-run without RBAC re-check → **INVALID**. A membership GRANT/REVOKE bumps
  the snapshot version → `RequireRebind` fires on EXECUTE → `CollectAndEnforce` re-runs. Verified
  live (grant→3, revoke→denied).
- `SET hba` applies-before-persist → **INVALID**. It is persist-then-publish: `WriteConfigFile`
  (fsync+atomic rename) first, publish live only on success (hba.cpp SetHbaFromTextString). The
  claim is the exact inverse.
- `HasColumnPrivilege` dup + hidden-PK drift → **already cleaned** (consolidated into
  `ColumnPrivHeld`→`RoleClosure`; both filter on the same `kGeneratedPKId`).
- auth-header de-sprawl (a–e) → **already cleaned** (`AclModeHeld`/`IsGranteeInRoles` anon-namespace;
  `RoleClosure::Owns` everywhere; `absl::FunctionRef`; headers clean).

---

## 2026-07-13 deep audit — new gaps (4 parallel investigators, all live vs PG 18.3 oracle)

Fan-out over GRANT/REVOKE semantics, object-privilege enforcement, DDL ownership gates, and
catalog/ACL functions. Every item below was reproduced live against a real PG 18.3 oracle and
is NOT a duplicate of anything above. (The catalog/ACL sweep hit a session limit mid-run — its
one confirmed finding is recorded; the rest of that surface is only partially covered — see note.)

### Security holes (non-owner can act / privilege bypass)
- **DROP TEXT SEARCH DICTIONARY has NO ownership gate** — **S**. Any role can drop another
  role's TS dictionary. `DropTokenizer` (catalog.cpp:4374) is the only `Drop*` that takes no
  `AccessContext` and never calls `RequireObjectOwner`; caller
  `duckdb_tokenizer_function.cpp:108` passes no actor. (CREATE side is correctly gated.) Fix:
  thread `AccessContext` in + `RequireObjectOwner(*_snapshot, ax.role, *tokenizer_id)`; pass
  `ActingAs(conn_ctx.GetRoleId())` from the pragma. (ALTER TS DICT is a syntax error, so no ALTER hole.)
- **Native `VACUUM t` / `ANALYZE t` bypass the MAINTAIN check** — **M**. The `MayMaintain` gate
  (`duckdb_vacuum_function.cpp:285`) is reached ONLY by the SereneDB pragma forms
  `VACUUM (COMPACT_*/RECOMPUTE_STATS_*)`; standard PG syntax lowers to DuckDB's native
  `VacuumStatement` which never calls it — a SELECT-only role can trigger maintenance PG refuses.
  (problems.md had MAINTAIN as "partly known"; this is the concrete bypass path.) Fix: gate the
  native VacuumStatement/ANALYZE execution on owner-or-MAINTAIN, and switch the privilege case
  from hard-error to PG's warning-and-skip.
- **`REVOKE GRANT OPTION FOR` ignores dependent grants** — **M**. Neither RESTRICT-refuses nor
  CASCADE-removes: after a WITH-GRANT-OPTION grantee re-grants, `REVOKE GRANT OPTION FOR ...`
  downgrades the grantor but orphans the dependent grant (PG errors without CASCADE, removes it
  with). Seam: `rbac.cpp` `ApplyAclGrant` — the `grant_option_only` branch (598-599) runs before
  the `cascade` branch (600) and skips the `AclDependentPrivs` check the plain-revoke branch does.
- **Role-membership grantor never stored → GRANTED BY unvalidated + membership deps untracked**
  — **M** (store+validate grantor) / **L** (full dep tracking). `catalog::Membership` (role.h:38)
  has no grantor field; `pg_auth_members.grantor` is hardcoded to root (pg_auth_members.cpp:42).
  Four symptoms, all verified: (a) grantor mis-recorded as postgres when a member with ADMIN
  re-grants; (b) `GRANT ... GRANTED BY x` where x lacks ADMIN wrongly succeeds (PG denies);
  (c) `REVOKE ADMIN OPTION FOR` with a dependent membership doesn't RESTRICT; (d) `REVOKE role
  FROM member` / `DROP ROLE` don't detect dependent memberships (PG refuses, we orphan them).
  Seam: add grantor to `Membership` + persistence + `GrantRole`/`ChangeMembership`
  (rbac.cpp:831, catalog.cpp:3036) + emit real grantor in pg_auth_members.

### Correctness / false-refusals (over-restrictive, not holes)
- **SEQUENCE nextval/currval require BOTH privileges instead of PG's either-or** — **M**. PG:
  nextval needs USAGE **or** UPDATE, currval needs USAGE **or** SELECT. We AND the bits, so
  `GRANT USAGE ON SEQUENCE` (the standard serial idiom) is wrongly refused. ACL storage is
  correct (`has_sequence_privilege`=t); only the enforcement mask is wrong. Seam:
  `connector/functions/sequence.cpp` Nextval(106,154)/Currval(112) pass a combined mask into
  `RequireAccess`→`RoleClosure::Can` (hardcoded `PrivMatch::All`). `CanAny`/`PrivMatch::Any`
  already exist but the read-enforcement flow never reaches them. Fix: thread an `any` flag
  through `AccessContext`→`EnforceRead`→`RequireAccess` and set it for nextval/currval.
- **ALTER INDEX ... RENAME refuses the legitimate table owner** — **S**. The table owner can't
  rename their own index (superuser can); a true non-owner is still correctly denied, so it's a
  false-refusal not a hole. `RenameRelation` (catalog.cpp:2940) checks the index's own (unset,
  root-defaulted) owner; the DROP path already fixes this by checking the parent table
  (DropIndex, catalog.cpp:4081). Fix: use `index->GetRelationId()` as the owning object.

### Missing feature (adjacent to ADP)
- **`ALTER ROLE ... SET <guc>` stored but never applied at connect** — **M**. `rolconfig`
  persists (`{search_path=custom_sp}` verified) but a new session ignores it (`SHOW search_path`
  stays default). PG applies per-role (and per-db-role via `pg_db_role_setting`) GUCs after auth.
  Seam: the connect/startup path (`pg_wire_session.cpp` / `connection_context.cpp`) must read
  role config and apply the GUCs at session start.

### Cosmetic divergences (do NOT file as bugs — recorded for coverage)
- `GRANT ... TO PUBLIC WITH GRANT OPTION` accepted (PG rejects "grant options can only be granted
  to roles") — inert since PUBLIC can't grant, but a `pg_dump` reload diverges. **S** guard in `GrantObject`.
- PUBLIC not seeded USAGE on the `public` schema (`has_schema_privilege(...,'public','USAGE')`=f;
  PG=t via `=U/pg_database_owner`). Muted while schema-USAGE enforcement is itself open, but a real
  ACL-content divergence. **S**.
- `'postgres'::regrole` (string→OID) unsupported (numeric `10::regrole` works); blocks idiomatic
  `roleid::regrole` introspection. **M** (regrole input fn must resolve names). Check regclass/regproc too.
- COMMENT ON COLUMN says "must be owner of table X" (PG: "relation X"); VACUUM/ANALYZE non-owner
  hard-errors (PG warns+skips); CREATE DATABASE prints `ATTACH` tag; DROP CASCADE silent on
  `drop cascades to`. All cosmetic — both engines make the correct authorization decision.

### Confirmed PG-identical (audited, no action — coverage record)
- REVOKE RESTRICT/CASCADE on OBJECT grants (refuse/remove correctly, exact error text); WITH ADMIN
  OPTION enforcement; circular + self membership refused; grantor-scoped OBJECT revoke; grant-not-held
  → WARNING no-op; REVOKE ALL of partial; PUBLIC default EXECUTE/CONNECT/TEMP.
- TABLE SELECT/INSERT/UPDATE/DELETE/TRUNCATE/REFERENCES enforced; default-deny; UPDATE/DELETE-WHERE
  and RETURNING require SELECT; FUNCTION EXECUTE; DATABASE CREATE; SCHEMA CREATE; SEQUENCE setval(UPDATE).
- ALTER/DROP owner gates on TABLE/VIEW/SEQUENCE/SCHEMA/TYPE/FUNCTION/DATABASE + INDEX-drop
  (parent-owner) + all ALTER TABLE sub-commands + COMMENT ON; ALTER ... OWNER TO's three sub-checks
  (owner, member-of-new-owner, CREATE-on-schema); CREATEROLE-can-only-touch-roles-it-has-ADMIN-on
  (PG16+/18); creator-owns defaults incl. CTAS (not regressed).
- TRIGGER privilege = dead surface (no CREATE TRIGGER grammar — nothing to enforce, like DOMAIN USAGE).

**Coverage note:** the catalog/information_schema/ACL-function surface (pg_roles/authid/shadow
columns, information_schema role views, has_*_privilege overloads, acldefault/aclexplode, relacl/
datacl rendering, \dp) was only PARTIALLY audited — the investigator hit a session limit. The
`ALTER ROLE SET`-not-applied finding above is its one confirmed result; re-run that surface later.

---

## 2026-07-13b — SQL-command-reference clause gaps (deep PG-docs port audit)

Found while checking every clause of the GRANT/CREATE ROLE/SET/etc. command-reference pages
against the engine (the concept-chapter passes missed these). All effect-verified live. The
WORKS ones went into the docs (PR #248); the items below are the ones that can't be documented
because they don't work — recorded here as the doc "omit-list" + backlog.

### Unimplemented — syntax error / hard error (not already tracked above)
- **Legacy group vocabulary** `CREATE GROUP` / `ALTER GROUP ADD|DROP USER` / `ALTER GROUP RENAME` /
  `DROP GROUP` — all syntax errors. (`CREATE/DROP/ALTER USER` aliases DO work; and `IN GROUP` as a
  CREATE ROLE clause works. Only the standalone GROUP statements are missing.) **S/M** — grammar
  aliases mapping GROUP→role statements. Low priority (deprecated in PG too).
- **Multi-target GRANT/REVOKE** — `GRANT SELECT ON a, b TO r` and `GRANT SELECT ON t TO r1, r2`
  are syntax errors (object AND grantee lists; membership form too: `GRANT g TO r1, r2` fails).
  One object + one grantee per statement. **M** (grammar List() + loop in handler, like the
  DROP ROLE comma-list already done). Real ergonomics gap — users must split statements; worth a
  doc "limitation" note meanwhile.
- **`WITH INHERIT OPTION` / `WITH SET OPTION` keyword form** — syntax error; only `WITH INHERIT
  TRUE|FALSE` / `WITH SET TRUE|FALSE` parse (those WORK — documented). `WITH ADMIN OPTION` works.
  **S** — accept the `OPTION` spelling as a synonym in the membership-grant grammar.
- **`ALTER ROLE ... IN DATABASE db SET ...`** and **`ALTER ROLE ALL SET ...`** — syntax errors
  (per-db-role and cluster-wide GUC defaults). **M**, and moot until plain `ALTER ROLE SET` is
  actually applied at connect (that gap is in the 2026-07-13 section).
- **GRANT object forms** `ON LANGUAGE` / `ON TABLESPACE` / `ON FOREIGN DATA WRAPPER` /
  `ON FOREIGN SERVER` / `ON LARGE OBJECT` / `ON PARAMETER` (+ `SET`/`ALTER SYSTEM` param privs) —
  syntax errors. No object surface for most; **L**/omit unless the object type is added.
- **`SET SESSION ROLE` / `SET LOCAL ROLE`** — syntax error (the plain `SET ROLE` + `SET SESSION
  AUTHORIZATION` work; only the `SESSION`/`LOCAL` qualifier before ROLE fails). Ties to the
  already-tracked SET LOCAL ROLE gap. **S**.
- **`GRANT ... TO CURRENT_USER|CURRENT_ROLE|SESSION_USER`** and **`TO GROUP r`** — the
  role_specification keyword forms aren't recognized (treated as literal names → "role does not
  exist"). **S** — resolve the keyword grantees.
- **`COMMENT ON ROLE`** — syntax error (whole path unimplemented; `shobj_description(...,
  'pg_authid')` returns null). **S/M**.
- **`SECURITY LABEL [FOR provider] ON ROLE ...`** — syntax error (SECURITY LABEL command absent).
  **L**/omit (needs a label-provider subsystem).
- **`aclexplode(acl)`** errors ("Unsupported catalog type when binding function"); **`makeaclitem`**
  does not exist; **`has_parameter_privilege`** / **`has_largeobject_privilege`** absent. **M** —
  aclexplode is the useful one (idiomatic ACL introspection). `acldefault`/`pg_get_userbyid` DO work.
- **View options `security_barrier` / `check_option`** — rejected (only `security_invoker` +
  `defer_binding` accepted). `security_invoker` is enforced + now documented. **M** each.

### Parses-but-NOOP — accepted, no runtime effect (must-omit from docs; would be false)
- **Function `SECURITY DEFINER` / `SECURITY INVOKER`** — parses, but a SQL function is inlined and
  runs with the CALLER's rights, so `SECURITY DEFINER` never elevates (verified: a definer function
  reading the owner's table → `permission denied` for the grantee, identical to invoker). **L** —
  needs a real function-execution security boundary (functions currently inline). This is the
  function-security gap noted in memory; recording it here as a concrete parses-but-noop trap.
- **`ALTER ROLE ... SET <guc>`** — already in the 2026-07-13 section (stored in `rolconfig`, not
  applied at connect). Re-confirmed across `search_path`/`statement_timeout`.
- **`GRANT ... ON DOMAIN <name>`** — the `DOMAIN` object keyword parses (object-lookup failure, not
  syntax error), but `CREATE DOMAIN` is a syntax error so no domain can exist → unusable. Dead until
  CREATE DOMAIN lands (tracked above).
- **`REPLICATION` / `BYPASSRLS` / `SYSID`** role clauses — parse and persist the flag but inert (no
  replication subsystem; RLS unimplemented). CREATE ROLE docs already note "no effect" — leave as-is.

---

## Fixed this session (2026-07-08/11)
- **IPv4-mapped IPv6 HBA rule vs v4 client (fail-open)** — FIXED. A rule authored in `::ffff:x`
  form now matches a v4 client via PG-style promotion in `AddrMatcher::Matches` (factored the
  AND-compare into `AddrMatcher::Contains`; added `V4Mapped`). Tests: gtest
  `HbaMatch.V4MappedRuleMatchesV4Client` + python `hba_mask_test.py::v4-mapped-rule-matches-v4-client`
  (both green; full HBA gtest 35/35, network suite 29/29).

---

# TODO — consolidated backlog

Every open item above, in one prioritized checklist. Size: **S** = few lines · **M** =
subsystem + test · **L** = new feature surface. Details/seams live in the sections above.

> **Definition of done for every item here: a sqllogictest covering it must be written and
> passing.** No fix, feature, or bug ships without a `.test` under `tests/sqllogic/` that
> exercises it — both the positive path and the negative/error path (e.g. `statement error`
> for a refusal, a `query` asserting the enforced result). Run it on **both** engines
> (`pg-wire-simple` and `pg-wire-extended`). Put cross-engine behavior under `any/pg/rbac/`
> (verified against the PG oracle); serenedb-only behavior/divergences under `sdb/pg/rbac/`.
> A fix without a test is not complete.
>
> **`any/pg/...` tests must also pass against a real PostgreSQL oracle in Docker** — not just
> serenedb. An `any/` test is a compatibility claim, so it has to be green on both engines
> **and** on the same stock PG the CI runs (`postgres:18.3`, launched with
> `-e POSTGRES_HOST_AUTH_METHOD=trust`, per `tests/sqllogic/run.sh`). If PG and serenedb
> legitimately differ, the test belongs in `sdb/pg/...` (serenedb-only), not `any/`. So when
> writing an `any/` test: run it locally against a `docker run postgres:18.3` oracle to
> confirm the expected output is genuinely PG's behavior before committing.
>
> **Connection-layer items (HBA, auth methods, peer/loopback, TLS, timeouts) need the network
> tests, not sqllogic** — sqllogictest connects through one already-authenticated session, so it
> can't exercise the connect/auth path. Those belong in the Python **network tests**
> (`tests/network/`, e.g. `hba_mask_test.py`, run by `tests/network/run.sh` in CI) and/or the
> gtests (`tests/server/basics/network_pg_hba_test.cpp`). Pattern: parser/matcher logic → gtest;
> end-to-end "can/can't connect under this HBA rule / password / peer" → python network test.
> The IPv4-mapped HBA fix is the model (gtest `HbaMatch.*` + `hba_mask_test.py` case, both green).
> So per item, pick the right harness: **DDL/DML/privilege behavior → sqllogic; who-can-connect
> and how-they-authenticate → network tests + gtest.**

## Security holes (fix first — a non-owner can act, or a privilege is bypassed)
- [ ] **S** — DROP TEXT SEARCH DICTIONARY has no ownership gate; any role can drop another's.
- [ ] **M** — native `VACUUM t` / `ANALYZE t` bypass the MAINTAIN check (only the pragma forms gate).
- [ ] **M** — `REVOKE GRANT OPTION FOR` ignores dependent grants (no RESTRICT refusal, no CASCADE).
- [ ] **M/L** — role-membership grantor never stored → `GRANTED BY` unvalidated, membership deps untracked.

## Correctness / false-refusals (works but wrong result)
- [ ] **M** — SEQUENCE `nextval`/`currval` require USAGE **and** UPDATE/SELECT (PG: either-or); breaks `GRANT USAGE ON SEQUENCE`.
- [ ] **S** — `ALTER INDEX … RENAME` refuses the legitimate table owner (checks the index's unset owner).

## Enforcement holes (PG denies, we don't — parses+stores but no runtime check)
- [ ] **M** — Schema `USAGE` not enforced (SELECT on a table works without USAGE on its schema).
- [ ] **M** — Database `TEMP` not enforced (`CREATE TEMP TABLE` works after `REVOKE TEMP`).

## Missing features that break real deployments
- [x] **M** — `ALTER ROLE … SET <guc>` stored in `rolconfig` but never applied at connect. **DONE**
      (pg_wire_session, txn-wrapped; test `alter_role_set_at_connect.test`).
- [ ] **L (core-fork, regression-risky)** — empty `search_path` (`SET search_path=''`, as pg_dump and
      many poolers do) breaks 2-part `schema.table` resolution. CONFIRMED live: with empty path,
      `SELECT FROM s14.t` → "schema s14 does not exist", but the explicit 3-part `postgres.s14.t`
      **resolves** — so the object is reachable; DuckDB's binder just won't fall back to the
      connection's current database catalog when a 2-part name's schema isn't in the (empty) search
      path. PG keeps the database fixed at connect, so it always resolves. Common pg_dump *restore*
      (create-with-empty-path) works; the DUMP/qualified-read direction diverges. Fix = DuckDB fork
      binder: resolve a bare `schema.table` against the current DB catalog even when the search path
      is empty (seam: `Catalog::GetSchema` / `CatalogSearchPath` qualified fallback,
      third_party/duckdb/src/catalog/catalog_search_path.cpp). Deferred: touches every qualified
      lookup — do not patch the core resolver blind.
- [ ] **M** — `ALTER DEFAULT PRIVILEGES` parses/persists but is never applied to new objects.
- [ ] **M** — ADP `FOR ROLE a,b IN SCHEMA s1,s2` keeps only the first of each list.
- [ ] **S/M + M/L** — predefined `pg_*` roles (all 16) missing + `pg_` prefix not reserved (land together).
- [ ] **L** — `DROP OWNED BY` / `REASSIGN OWNED BY` missing (needed to offboard a role cleanly).
- [ ] **S** ×2 — `ALTER DATABASE … OWNER TO` / `ALTER FUNCTION … OWNER TO` unsupported.
- [ ] **S** — `SET LOCAL ROLE` / `SET LOCAL SESSION AUTHORIZATION` = syntax error (used by poolers).
- [x] **S** — `password_encryption` GUC missing. **DONE** (config_variables.cpp; `compat_gucs.test`).
- [x] **S** — `row_security` GUC missing. **DONE** (config_variables.cpp; `compat_gucs.test`).
- [ ] **L** — CREATE DOMAIN not implemented (also blocks DOMAIN USAGE grants).
- [ ] **L** — RLS has no runtime (policy DDL parses to unregistered pragmas and hard-errors).
- [x] **HBA `peer`** — LANDED (network-tested). SO_PEERCRED uid -> getpwuid_r name, compared to
      the requested role; unix sockets only (peer rules can't match TCP by construction).
      PeerAuthenticate at the session; DeferredPeerIdent now dispatches peer->run / ident->reject.
      Test tests/network/peer_auth_test.py (unix socket; skips if OS user is 'postgres' since the
      superuser safety rule shadows the peer path).
- [x] **HBA `cert`** — LANDED (network-tested). Verified client-cert CN (SSL_get_peer_certificate +
      SSL_get_verify_result==X509_V_OK) must equal the requested role; hostssl only. New
      Socket::PeerCertCommonName + CertAuthenticate at the session; MethodClass::DeferredCert +
      Decision::Kind::DeferredCert dispatch. Requires the listener to carry a CA (sslmode=verify-ca/
      -full → require_client_cert). Test tests/network/cert_auth_test.py (openssl-built CA + certs;
      skips if openssl CLI absent). `map=`/clientcert=verify-full options parse but the base impl is
      plain CN==role. STILL absent: `ident` (TCP, needs an ident server) / `ldap` / `gss`.
- [ ] **M** — multi-target GRANT/REVOKE (`ON a,b` / `TO r1,r2`) = syntax error; one object + one grantee only.
- [ ] **L** — function `SECURITY DEFINER` is a no-op (SQL functions inline, run as caller — no elevation).
- [ ] **M** — `aclexplode()` / `makeaclitem` / `has_parameter|largeobject_privilege` absent.
- [ ] **M** — view `security_barrier` / `check_option` rejected (only `security_invoker` works).
- [ ] **S/M** — legacy `CREATE/ALTER/DROP GROUP` statements = syntax error (aliases; low priority).
- [ ] **S** — `WITH INHERIT/SET OPTION` keyword form rejected (only `TRUE|FALSE` works).
- [ ] **S** — `GRANT … TO CURRENT_USER|CURRENT_ROLE|SESSION_USER` keyword grantees unrecognized.
- [ ] **S/M** — `COMMENT ON ROLE` = syntax error.
- [ ] **L** — `SECURITY LABEL ON ROLE` = syntax error (whole command absent).

## Lower-severity / cosmetic
- [ ] **M** — DROP SCHEMA strands schema-scoped default-ACL rows → grantee role undroppable forever.
- [ ] **S** — `CREATE ROLE public` / `none` not reserved (PG rejects `42939`).
- [ ] **S–M** — DROP ROLE refusal reports a bare dependency count, not PG's itemized DETAIL.
- [ ] **S** — `GRANT … TO PUBLIC WITH GRANT OPTION` accepted (PG rejects); inert but diverges on `pg_dump` reload.
- [ ] **S** — PUBLIC not seeded USAGE on the `public` schema (ACL-content divergence).
- [ ] **M** — `'name'::regrole` (string→OID) unsupported; blocks `roleid::regrole` introspection.
- [ ] **S** — `ALTER … RENAME` doesn't require CREATE on the schema (PG needs owner + schema CREATE;
      we check only ownership). More lenient; found writing `ddl_alter_index_rename_ownership.test`.
- [ ] error-text wording nits (COMMENT ON COLUMN "table" vs "relation"; VACUUM hard-error vs warn+skip). Cosmetic.

## 2026-07-13c — parallel verify-wave findings (7 effect-verified probes vs PG 18.3 oracle)
- **[SECURITY] COPY to/from a server-side FILE has no superuser gate** — **M**. A non-superuser
  ran `COPY t TO '/path'` (wrote an arbitrary file) and `COPY leak FROM '/etc/hostname'` (leaked
  it). PG refuses (`pg_read/write_server_files`/superuser). STDIN/STDOUT COPY correctly enforces
  table SELECT/INSERT. Seam: `duckdb_copy_filesystem.cpp:193` `CanHandleFile` only claims
  /dev/stdin,/dev/stdout; real paths fall through to LocalFileSystem with no role check (grep for
  a server-files gate = 0). Fix: gate server-side file COPY on superuser.
- **View-column privilege grants error out** — **M-L (persistence-schema change)**. `GRANT SELECT (a)
  ON <view>` → `relation "v" does not exist`; base-table column grants + whole-view SELECT grants are
  PG-identical. Confirmed live (PG: a_ok=t,b_ok=f; serened errors). NOTE after investigation: this is
  bigger than "M". `ChangeColumnAcl` stores per-column ACLs INSIDE the Table's `Column` objects
  (`table->ChangeColumnAcl`), but `PgSqlView` has no `Column` objects -- only `CreateViewInfo` names.
  Supporting it needs NEW per-column ACL storage on `PgSqlView` + a Serialize/Deserialize format bump
  (back-compat for existing views) + `PgSqlView::ChangeColumnAcl` + `has_column_privilege` view
  support + a per-column enforcement read-set mapping in optimizer/rbac.cpp (views are currently
  whole-view SELECT-checked). Deferred: a view-catalog serialization change is the risky layer;
  do it deliberately, not hastily. Seams: `rbac.cpp:670` (GrantObjectColumns non-Table reject),
  `catalog.cpp:3475` (ChangeColumnAcl GetObject<Table>), `view.h` (add column-ACL state).
- **pg_dump breaks on empty search_path** — **M**. pg_dump v18/19 sets `search_path=''` then
  issues schema-qualified queries; SDB can't resolve schema-qualified names with an empty
  search_path (duckdb binder error). `\du`/`\dp`/`\dn+` are PG-identical. Seam:
  `config.cpp:65-77` GetSearchPath → duckdb catalog_search_path.
- **RESOLVED (not a problem):** DATABASE CONNECT *is* enforced at connect (the doc "Corrections"
  is correct; the earlier doubt was the default PUBLIC CONNECT grant — REVOKE from PUBLIC too and
  SDB refuses exactly like PG). DELETE WHERE/RETURNING require SELECT (PG-identical). Role/closure
  lookup is O(1) per-snapshot cached (no per-query refetch). Bind-time write-privilege collection
  is complete across all write families (no MERGE-class gap remains).

## DONE (this run)
- [x] **DROP TEXT SEARCH DICTIONARY ownership gate** — `DropTokenizer` now takes `AccessContext` +
      `RequireObjectOwner`; caller passes `ActingAs(context)`. Test `sdb/pg/rbac/ddl_drop_tsdict_ownership.test` (both engines).
- [x] **ALTER INDEX RENAME owner check** — `RenameRelation` gates index rename on the parent table's
      owner (mirrors DropIndex). Test `any/pg/rbac/ddl_alter_index_rename_ownership.test` (both engines + PG oracle).
- [x] **REVOKE GRANT OPTION FOR dependents** — new `AclRemoveGrantOptionCascade` (rbac.cpp): RESTRICT
      refuses while dependents exist, CASCADE removes them + drops the grant option (keeps the priv).
      Test `any/pg/rbac/revoke_grant_option_dependents.test` (both engines + PG oracle).
- [x] **`password_encryption` + `row_security` GUCs** — settable-inert (config_variables.cpp); pg_dump
      `SET row_security = off` + `SHOW password_encryption` work. Test `any/pg/rbac/compat_gucs.test`.
- [x] **`CREATE ROLE public/none/pg_*` reserved** — 42939 in `pg::CreateRole`. Test
      `any/pg/rbac/ra_reserved_role_names.test` (both engines + PG oracle).
- [x] **SEQUENCE USAGE-or-UPDATE either-or** — added `match_any` to `AccessContext` +
      `RequireAccessAny` + `CanAny` path in `Snapshot::RequireAccess`/`EnforceRead`; set for
      nextval (Usage|Update) / currval (Usage|Select) in sequence.cpp — INCLUDING the
      constant-vector fast path in `NextvalFunction` (the real entry point; a second
      `ResolveSequence` call I initially missed). Test `any/pg/rbac/enf_sequence_usage.test`
      (both engines + PG oracle).
- [x] **[SECURITY] COPY server-file superuser gate** — `RequireCopyFileAccess` (pg_wire_session)
      gates `COPY ... TO/FROM '<file>'` on superuser at every pre-Prepare choke point
      (RunSimpleQuery, HandleParse, PendingStatementEnsured — COPY FROM's CSV sniff opens the file
      at Prepare). Non-super refused both directions (psql + sqllogic verified); superuser +
      STDIN/STDOUT unaffected. Test `sdb/pg/rbac/enf_copy_server_file.test`. (sqllogictest routes
      COPY FROM to its default superuser connection, so only the TO refusal is asserted there —
      same gate; FROM is psql-verified.)
- [x] **`GRANT ... TO PUBLIC WITH GRANT OPTION` rejected** — guard in `ApplyAclGrant` (42P01-style
      "grant options can only be granted to roles"). Test `any/pg/rbac/grant_public_grant_option.test`
      (both engines + PG oracle).
- [x] **#9a `ALTER ROLE ... SET name = value` applied at connect** — `Role::Config()` ("name=value"
      entries) applied in `ObtainConnection` (pg_wire_session.cpp) before the client startup params,
      wrapped in one `_conn` transaction because catalog-backed setters (search_path) need an active
      txn at connect. Stale/invalid stored setting is skipped, not fatal. Test
      `any/pg/rbac/alter_role_set_at_connect.test` (both engines + PG oracle). The ADP-applied-to-new-
      objects half of #9 remains open (default_acls stored but not consulted at CREATE).
- [x] **`ALTER DATABASE ... OWNER TO`** — grammar `OwnerObjectType` += `DATABASE` (regen inlined .hpp
      + .gram), new Database branch in `Catalog::ChangeOwner` (mirrors ChangeAcl: resolve by name,
      require owner + SET-ROLE-to-new, persist via ReplaceObject<Database> + PutDefinition), and
      `AlterOwner` resolves the target db by name. Test `any/pg/rbac/alter_database_owner.test` (both
      engines + PG oracle). NOTE: `ALTER FUNCTION ... OWNER TO` is NOT done -- it needs the arg-type
      signature (`ALTER FUNCTION f(int) OWNER TO`), which the `QualifiedName`-only grammar can't
      carry; genuinely more than S (signature grammar + overload resolution). Deferred.
- [x] **#9b ALTER DEFAULT PRIVILEGES applied to new objects (tables + sequences)** — new file-local
      `DefaultAclForOwner` in catalog.cpp reads the creating role's stored `DefaultAcls()`
      (schema-specific rule wins over global `id::kInvalid`), returns the storable grants via
      `AclForStorage`, and `CreateTable`/`CreateSequence` seed the new object's `Permissions` with
      them. Internal serial/PK sequences (registered directly, not via CreateSequence) are unaffected.
      Test `any/pg/rbac/adp_applied_to_new_objects.test` (both engines + PG oracle): global default,
      no-retroactive-leak, sequence default, REVOKE-default. Full rbac suite (287 tests) green both
      protocols after the change.
- [x] **[test correctness] `enf_authorization_denied.test`** — its COPY-to-*file* denial was masked
      once #12 landed: PostgreSQL (verified) also reports "permission denied to COPY to a file"
      *before* the table-privilege check, so the file COPY no longer surfaces the table denial the
      case is about. Switched it to `COPY ... TO STDOUT` (allowed for everyone → the SELECT check
      fires → "permission denied for table"), preserving intent. (Not a bug -- serened is now MORE
      PG-faithful on COPY error ordering.)
- NOTE: `drm_default_priv.test` flakes under `--jobs 8` (shared cluster-global roles race across
  parallel tests -- the known harness limitation); passes on both engines at `--jobs 1`. Not a
  regression.
- [x] **#5 Schema USAGE enforced** (+ PUBLIC-USAGE-on-`public` seed). Two changes: (1) bootstrap
      `public` schema now carries `PUBLIC=U` (catalog.cpp CreateDatabase) so schema-USAGE doesn't
      lock everyone out of it; (2) USAGE on the containing schema is required before the object's own
      privilege in BOTH `Snapshot::EnforceRead` (catalog-getter/DDL paths) AND
      `optimizer/rbac.cpp CollectAndEnforce` (the plan-level SELECT/DML read path -- the getters do
      NOT cover it, which is why the first EnforceRead-only attempt let SELECT through). System
      schemas (pg_catalog/information_schema) skipped; superuser + schema-owner bypass via the
      closure. Test `any/pg/rbac/enf_schema_usage.test` (both engines + PG oracle). Full 287-test
      suite green on both protocols. Closes the "DB CONNECT also unenforced" sibling only for schema;
      DB CONNECT is separate (colleague-owned auth).
- [x] **#10a Predefined roles `pg_read_all_data` / `pg_write_all_data`** — seeded idempotently at
      bootstrap (fixed ids `id::kPgReadAllData`/`kPgWriteAllData`, NOLOGIN + INHERIT), enforced via new
      `RoleClosure::read_all_data`/`write_all_data` flags set in `ComputeRoleClosure` from the
      membership closure and folded into `Can`/`CanAny` through `PredefinedModes` (SELECT on relations
      + USAGE on schemas for read; INSERT/UPDATE/DELETE + USAGE for write). Covers introspection
      (`has_table_privilege`) AND runtime enforcement. Test `any/pg/rbac/predefined_roles.test` (both
      engines + oracle); full rbac suite green. DELIBERATELY did NOT seed pg_monitor/pg_maintain/
      pg_database_owner (subtler semantics) -- leaving them absent means GRANT errors honestly rather
      than the "silent privilege illusion" of seeding-without-enforcing. `CREATE ROLE pg_*` reserved
      (done earlier). GOTCHA when testing: kill stale serened on reused ports (a pre-seed binary
      answering on a port made the test spuriously fail).
- [x] **#6 Database TEMP enforced** — new `RequireCreateTempAccess` (pg_wire_session.cpp) gates
      `CREATE TEMP TABLE/SEQUENCE` on `Can(database, CreateTemp)` at the same choke points as the COPY
      gate (RunSimpleQuery + PendingStatementEnsured), discriminating by catalog type so the bind-time
      PIVOT enum (a TYPE) is unaffected. No seed needed: `PublicDefaultPrivs(Database)` already grants
      PUBLIC temp until the ACL materializes on `REVOKE TEMP ... FROM PUBLIC`. Proven byte-identical to
      PG via psql; tested by **`tests/network/rbac_temp_test.py`** (added to network run.sh) --
      sqllogictest routes `CREATE`/DDL to its default *superuser* connection (same limitation that
      keeps COPY-from-file and this out of the sqllogic suite), so the network harness is the correct
      vehicle. Full rbac suite green.

- [x] **#8a GRANTED BY validated for role membership** — the parsed-but-dropped `GRANTED BY` on
      `GRANT <role> TO <member>` is now threaded transformer→pragma(param 7)→`MemberOptions.granted_by`
      →`ChangeMembership(…, granted_by)`, which enforces PG's rules: actor must act as the grantor
      (superuser bypasses) AND the grantor must hold ADMIN on the role — with the superuser-grantor
      implicit-ADMIN exception (existing oracle test `grant_granted_by.test` caught that: PG admits
      `GRANTED BY postgres`). New test `any/pg/rbac/grant_role_granted_by.test` (oracle + both
      engines); full suite green. NOTE: the edge still RECORDS the actor, not the named grantor —
      grantor STORAGE is the serialization-blocked half below.

- [x] **`SET LOCAL ROLE` / `SET LOCAL SESSION AUTHORIZATION`** — grammar `SetRole <- 'LOCAL'? 'ROLE'
      RoleSpec` (+ same for SESSION AUTHORIZATION), hand-updated the two trampolines in
      transform_generated.cpp (child indices shift by the leading optional) and threaded `is_local`
      → `SetScope::LOCAL`, which the existing settings framework transaction-scopes. Verified vs
      oracle: LOCAL reverts at COMMIT, plain SET ROLE persists. Test `any/pg/rbac/set_local_role.test`
      (oracle + both engines). Regen inlined grammar both files. Remaining cosmetic divergence:
      outside a txn PG warns+ignores, serened errors — pre-existing generic SET LOCAL behavior.
      Wide-blast regression: rbac suite + all 88 SET-exercising tests green EXCEPT
      `sdb/pg/index/inverted_index_ivf_pq.test` + `_sq4` — **pre-existing colleague-owned
      vector-search SIGSEGV** (`SearchFullScanTopKLocalState::OnSegment`, feat #846; no RBAC/SET/
      catalog frames; also fails on the July-4 pre-changes `build_perf` binary).

## Deferred — require a persistence-format migration (datadir-breaking otherwise)
- **#8 Role-membership grantor storage + GRANTED BY validation** — the grammar already parses
  `GRANTED BY` and `ResolveGrantedBy` resolves it, but `Membership` (role.h) has no `grantor` field,
  so it's dropped. Adding one is NOT a casual change: `Role` serializes via `basics::WriteTuple`
  (boost::pfr, POSITIONAL) and `ReadTuple` does a strict field-count check (serializer.h:402 --
  `count != expected` throws). A new `Membership` field makes new code expect N+1 fields while
  existing datadirs hold N → deserialization throws → **every existing role catalog fails to load on
  upgrade**. Needs a format-version bump + migration (or out-of-band grantor storage). Do deliberately.
- **#13 View-column ACL storage** (see above) — same blocker: needs new serialized state on
  `PgSqlView` + the same strict-count migration concern.
- NOTE: DROP ROLE itemized DETAIL is a **won't-fix (cosmetic)**: `RoleDependency.referencing_objects`
  records no edge kind, so serened cannot produce PG's "owner of table X" vs "privileges for X"
  distinction (only a bare "table X" list, which diverges from the oracle anyway). The current
  count-based DETAIL is functional; changing it risks existing tests for ~zero value.

## 2026-07-13 five-agent PG-18.3 parity audit (auth + RBAC), all live-probed
Full detail reported in chat; condensed here. FIXED on the spot: bootstrap `postgres` DB's `public`
schema missed the PUBLIC=USAGE seed (`CatalogStore::EnsureSystemDatabase` didn't mirror
CreateDatabase; per-test DBs masked it in the suite — psql on the default DB hit it).

Security/correctness bugs (no feature work needed):
- [ ] **[SECURITY/ops] superuser self-rename = permanent lockout** — no "session user cannot be
      renamed" guard; HBA safety rule keyed to bootstrap-superuser NAME. **S** (guard + key by id).
- [ ] **[SECURITY-pooler] DISCARD ALL resets nothing auth-wise** — SET ROLE/SET SESSION AUTH/GUCs
      survive; pgbouncer relies on it; tag says DEALLOCATE ALL. **M**.
- [ ] `PASSWORD ''` stores a REAL scram verifier for the empty string (PG: notice + clears). **S**.
- [ ] Failed auth leaves ZERO server-side log (PG logs FATAL + DETAIL + hba line). **S**.
- [ ] VACUUM/ANALYZE non-owner warn+skip notice never reaches the client (silent success). **S**.
- [ ] has_server/fdw_privilege stubs return `t` for NONEXISTENT objects. **S**.
- [ ] Owned serial seq: hidden from pg_class/pg_sequences AND direct ALTER SEQUENCE OWNER on it
      succeeds (PG refuses "linked to table"); pg_sequences view ALWAYS empty. **S** ×2.
- [ ] HBA silent-acceptance family: include/@file/map=/clientcert=/unknown `k=v` accepted+ignored
      (PG errors); make loud. **S**. hostname/samehost/samenet rules refuse at match (landmine). **M**.
- [ ] Auth error-detail leaks: md5-stored-vs-scram-line 28000 tells the client the credential form;
      HBA no-match message omits host addr + doesn't distinguish reject-vs-no-match. **S**.
- [ ] pg_authid.rolpassword hard-NULL breaks pg_dumpall migration. **S-M**.
- [ ] acl-array `::text` renders `['a', 'b']` (list style) not `{a,b}`; ordering diff too. **M**.
- [ ] pg_stat_activity: client_addr NULL on TCP; application_name startup param ignored. **M**.

Missing-feature ship-list (sizes from live probes): RLS runtime **XL** (grammar/transform DONE, 4
pragma handlers unregistered; checklist captured in chat); SECURITY DEFINER real execution **L**;
parameter privileges (GRANT SET/ALTER SYSTEM ON PARAMETER + has_parameter_privilege) **L**;
DROP/REASSIGN OWNED **L**; event triggers / SECURITY LABEL **L**; cert-as-identity + ident maps
**M-L**; GSS enc **XL**; RADIUS/PAM/OAuth **L-XL**; ALTER DATABASE SET + ALTER ROLE IN DATABASE SET
+ ALTER ROLE ALL SET **M** (pg_db_role_setting already exists, needs db-scoped rows + connect
merge); security_barrier + WITH CHECK OPTION **M**; LOCK TABLE **M**; multi-target GRANT **M**;
'name'::regrole **M**; remaining predefined roles **M**. Small: TO CURRENT_USER/CURRENT_ROLE/
SESSION_USER grantees; makeaclitem/pg_get_acl/acldefault-name-resolution/aclexplode-select-list;
has_column_privilege oid+attnum overloads; has_function_privilege regprocedure; COMMENT ON ROLE;
CREATE/ALTER/DROP GROUP; GRANT ON LANGUAGE/TABLESPACE; ALTER SEQUENCE RESTART grammar; direct-SSL
ALPN (PG17); pg_hba_file_rules line_number/error columns; information_schema minor row diffs.

Confirmed FULL PARITY (live): TLS + SCRAM-SHA-256-PLUS channel binding (offered only over TLS,
downgrade-guarded); VALID UNTIL enforced at login; hostssl/hostnossl + /regex/ HBA + samerole;
CREATEDB enforcement; SET SESSION AUTHORIZATION privilege rules; the whole non-owner ALTER matrix;
OWNER TO all 5 types incl SET-ROLE + schema-CREATE rules; \du/\dp/\z/\ddp/\drds; pg_auth_members
PG16 columns; pg_roles/pg_authid/pg_user/pg_group/pg_shadow column-exact; REVOKE CASCADE chains;
WGO re-grant grantor attribution; relacl byte-parity incl MAINTAIN 'm'; information_schema
table/column/role-grant views row-identical; GRANT TRIGGER/REFERENCES/MAINTAIN; ALL
TABLES/SEQUENCES/FUNCTIONS/PROCEDURES/ROUTINES IN SCHEMA.

## 2026-07-13 fix session (post-audit) — LANDED (each built + oracle/psql-verified, rbac suite green)
- [x] **All 16 PG-18 predefined roles seeded** (was 2) incl pg_signal_autovacuum_worker; pg_monitor
      member of read_all_settings/stats/stat_scan_tables. Enforced: pg_maintain→MAINTAIN (PredefinedModes),
      pg_read/write_server_files→COPY file gate (verified). Tests predefined_roles_full, enf_pg_maintain.
- [x] **Superuser self-rename lockout FIXED** — RenameRole guards session/current user
      (ERRCODE_FEATURE_NOT_SUPPORTED). Test ra_rename_session_user.
- [x] **Bootstrap `public` schema PUBLIC=USAGE seed** — store.cpp EnsureSystemDatabase (was missing
      → non-super locked out of default DB's public schema; two audit agents hit it).
- [x] **DISCARD ALL resets role + session authorization** — serenedb_discard pragma; byte-identical to
      oracle (TEMP keeps role, ALL resets). Test discard_all_resets_role. (GUC-reset deferred: no clean
      Config clear-all; RESET ALL works explicitly.)
- [x] **Auth bugs**: PASSWORD '' warns+clears; pg_authid.rolpassword visible to superuser + NULL when
      none (pg_dumpall); md5-vs-scram → generic client 28P01 + server log (no credential-form leak);
      HBA no-match msg includes host + logs; failed-auth server logging at every path. Test
      auth_password_edge + psql/HBA-network verified.
- [x] **has_tablespace_privilege 3-arg** NULL→false for pg_default (by name).

## 2026-07-13 "fix them all" wave 2 — LANDED (oracle-verified, rbac suite green)
- [x] **Keyword grantees** `GRANT ... TO CURRENT_USER/CURRENT_ROLE/SESSION_USER` (ResolveGranteeId+ctx).
- [x] **Multi-target GRANT/REVOKE** `ON a,b TO r1,r2` — grammar List(Grantee)+List(QualifiedName),
      transformer/pragma LIST params, handler cartesian loop; full suite green both protocols.
      Test grant_multi_and_keyword.test.
- [x] **`'name'::regrole`** in-cast (name→oid) + regrole→text out-cast (oid→name); dropped role renders
      numerically. Test regrole_cast.test.
- [x] **`CREATE/ALTER/DROP GROUP`** aliases (RoleOrUser += GROUP, NOLOGIN). Test group_aliases.test.
- [x] **pg_sequence catalog populated** (new pg_sequence.cpp GetTableData + specialization decl in .h;
      was empty → pg_sequences view empty). Test pg_sequences_view.test. Updated 2 sdb/system tests
      that had codified the empty-pg_sequences + NULL-public-nspacl (both now correct-behavior).
- [x] **REASSIGN OWNED BY / DROP OWNED BY** (new statement family: grammar + 2 trampolines +
      serenedb_reassign_owned/serenedb_drop_owned pragmas + Catalog::ReassignOwned/DropOwned).
      CollectOwnedTargets splits a role's RoleReferenceIds into owned (reassign via
      ChangeOwner(NoAccessCheck)/drop via Drop*) vs grant-refs (DROP OWNED revokes via ChangeAcl).
      Offboarding works: REASSIGN transfers ownership; DROP OWNED drops objects + revokes grants →
      role droppable. Oracle-identical, full suite green. Test reassign_drop_owned.test. GAP: owned
      FUNCTIONS/indexes skipped (ChangeOwner has no Function branch; indexes follow their table).

## Still open after this session (prioritized tail)
- [ ] **M** — general `<array>::text` renders `[1, 2, 3]` not PG's `{1,2,3}` (surfaced via relacl but
      affects ALL arrays; the pg_array_text cast path isn't applying to `::text`). Cross-cutting cast
      issue, NOT RBAC-specific — investigate the List/Array→VARCHAR cast in the duckdb fork; risky.
- [x] **pg_stat_activity.application_name + client_addr** — application_name read from the session ctx
      at snapshot time (SdbProgress + ProgressSnapshot + view); client_addr threaded
      PgWireSession._peer_addr → ConnectionContext → ProgressSource → snapshot → view (`::inet`, NULL
      for local). Verified: app_name via PGAPPNAME + SET; client_addr shows the IP as inet.
- [ ] **S** — owned serial sequence hidden from pg_class/pg_sequences + re-ownable by direct
      ALTER SEQUENCE OWNER (PG refuses "linked to table"); pg_sequences view empty.
- [ ] **S** — has_server/fdw_privilege return t (superuser) for nonexistent objects (FDW absent; low pri).
- [ ] **S ea** — grammar cheap wins: COMMENT ON ROLE; CREATE/ALTER/DROP GROUP; GRANT ON LANGUAGE/
      TABLESPACE; TO CURRENT_USER/CURRENT_ROLE/SESSION_USER grantees; ALTER SEQUENCE RESTART.
- [x] **makeaclitem(grantee,grantor,privileges,is_grantable)** — LANDED (oracle-verified). SQL macro
      in system_functions.h; builds PG's `grantee=privchars[*]/grantor` text form, canonical char
      order (not input order), '*' after grantable chars, grantee oid 0 → PUBLIC (empty), unrecognized
      privilege → PG's exact `unrecognized privilege type: "X"` error. Composes with aclexplode. Test
      any/pg/rbac/makeaclitem.test (::text cast needed — PG's aclitem has no binary output fn so the
      extended protocol can't serialize a bare aclitem). ~2s/call (CTE+correlated subqueries) — fine
      for introspection.
- [x] **has_column_privilege(name, regclass, col|attnum, priv)** — LANDED (oracle-verified). Added the
      two missing C++ overloads (VARCHAR role + REGCLASS table + VARCHAR-col-or-SMALLINT-attnum);
      resolves role by name, table by oid, honors column-level GRANT. Test
      any/pg/rbac/has_column_privilege_regclass.test.
- [~] **has_function_privilege(regprocedure)** — C++ overloads LANDED (VARCHAR/OID role + REGPROCEDURE
      obj; regprocedure is oid-backed, no implicit cast to oid, so needed explicit signatures). BUT
      NOT TESTABLE: serened's pg_proc is EMPTY (0 rows) AND there is no regprocedure/regproc INPUT cast
      (`'abs'::regproc` fails "Could not convert 'abs' to INT64"). So no oid to feed + no text→oid
      resolution. Plumbing is correct+harmless; deferred until pg_proc is populated + a regprocedure
      text cast (function-signature parse + overload resolution vs pg_proc) exists.
- [ ] **S ea** — remaining ACL fns: acldefault oid→name (the array→text OID-render issue);
      pg_get_acl (crashed at startup, reverted — needs lazy eval); aclexplode('...'::aclitem[])
      literal cast (no real aclitem[] type); regproc/regprocedure input cast; populate pg_proc.
- [ ] **S** — COMMENT ON ROLE: BLOCKED by serializer positional trap. PG stores a retrievable comment
      (shobj_description(oid,'pg_authid')) + errors on missing role. Retrieval needs a `comment` field
      on RoleData, which appends a 10th pfr field → serializer.h check_size throws on every existing
      datadir. Parse-only (accept/error, store nothing) = false-support partial, NOT shipped. Same
      tier as membership-grantor storage / view-column ACLs — needs the persistence migration.
- [ ] **S** — ALTER SEQUENCE RESTART [WITH n] / CACHE n: real duckdb sequence-state mutation (not a
      parse-noop); SequenceOption grammar lacks RESTART/CACHE; engine/storage work, outside auth scope.
- [ ] **S** — GRANT ON LANGUAGE / TABLESPACE: serened models neither as first-class objects; PG-faithful
      behavior would require modeling them (can't parse-noop without diverging on missing-object errors).
- [ ] **S** — HBA strict option validation (reject unknown k=v/include/@file/map=); risk: breaks
      rulesets that set now-ignored options.
- [ ] **S** — VACUUM/ANALYZE non-owner warn+skip notice not delivered to client.
- [x] **LOCK TABLE** — LANDED (oracle-verified, both protocols). New statement:
      `LOCK [TABLE] [ONLY] name,... [IN <mode> MODE] [NOWAIT]`, all 8 lock modes (LockMode alts
      PARENTHESIZED + longest-first — bare 'SHARE'/'EXCLUSIVE' last, else PEG choice/sequence
      precedence eats them). serened's MVCC makes the lock advisory (no-op), so pg::LockTable only
      validates: relation exists (else 42P01) + mode-appropriate privilege (ACCESS SHARE = any of
      SELECT/INSERT/UPDATE/DELETE/TRUNCATE/MAINTAIN; every stronger mode = UPDATE/DELETE/TRUNCATE/
      MAINTAIN; owner+superuser bypass). The "must be in a transaction block" 25P01 gate lives at the
      WIRE LAYER (pg_wire_session PendingStatementEnsured for simple, Bind for extended) via
      IsLockTablePragma + IsExplicitTransaction — NOT the pragma handler, because only the wire layer
      can tell a user BEGIN from the per-statement implicit block. FIXED a real divergence: extended
      protocol was treating the implicit per-statement block as explicit, so LOCK-outside-BEGIN
      wrongly succeeded on extended. Test any/pg/rbac/lock_table.test.
- [ ] **M ea** — ALTER DATABASE SET / ALTER ROLE IN DATABASE SET / ALTER ROLE ALL SET (+ connect
      merge; pg_db_role_setting exists) [NEW GucSetting object type]; security_barrier + WITH CHECK
      OPTION [needs updatable views]. (multi-target GRANT + 'name'::regrole already landed jul14.)
- [ ] **L** — parameter privileges (GRANT SET/ALTER SYSTEM ON PARAMETER + has_parameter_privilege).
- [ ] **L** — DROP OWNED / REASSIGN OWNED (new statement family).
- [ ] **L** — SECURITY DEFINER real execution (functions currently inline as caller).
- [x] **SECURITY LABEL** — LANDED (oracle + both serened engines green). New statement:
      grammar `SecurityLabelStatement <- 'SECURITY' 'LABEL' ('FOR' (StringLiteral / ColId))? 'ON'
      SecurityLabelObjType QualifiedName 'IS' SecurityLabelValue` (reserved-keyword obj types listed
      explicitly, ColId fallback); transformer walks the FOR group to its leaf (STRING or IDENTIFIER)
      → serenedb_security_label(provider) pragma. Handler mirrors stock PG with no provider loaded:
      named FOR provider → 22023 `security label provider "X" is not loaded`; no FOR → 0A000
      `no security label providers have been loaded`. Test any/pg/rbac/security_label.test (all 3
      forms). Grammar regen'd (.hpp + .gram). No regressions.
- [ ] **L** — event triggers.
- [ ] **XL** — RLS runtime (grammar/transform done; 4 unregistered pragma handlers + Policy catalog +
      plan-time injection; full checklist in the 2026-07-13 audit chat).
- [ ] pg_database_owner implicit-DB-owner membership + PG-15 public-owned-by-pg_database_owner model
      (seeded as a role; implicit membership needs closure-with-DB-context).

## Follow-ups / re-runs (not bugs)
- [ ] Re-run the catalog/information_schema/ACL-function audit — it was cut short by a session limit.
- [ ] Docs: add the ~11 working-but-undocumented items to PR #248 (`WITH INHERIT/SET`, `GRANTED BY`,
      `ENCRYPTED PASSWORD`, bulk GRANT, `IN ROLE/IN GROUP`, `SET SESSION AUTHORIZATION`, view
      `security_invoker`, `has_*`/`pg_auth_members` introspection). These WORK — doc-debt, not a bug.

## 2026-07-14 — What PostgreSQL 18.3 supports that SereneDB does NOT (live-verified)
Every line below was probed against a fresh serened AND a postgres:18.3 oracle this session
(GAP = serened errors where PG succeeds). This SUPERSEDES the stale checkboxes above — many
top-section "open" items are actually DONE (predefined roles, multi-target GRANT, CREATE GROUP,
WITH INHERIT, ::regrole, SET LOCAL ROLE, DROP/REASSIGN OWNED, ALTER DATABASE OWNER, aclexplode,
makeaclitem, SECURITY LABEL, LOCK TABLE, CREATE ROLE public rejection, PUBLIC-USAGE-on-public,
PUBLIC-grant-option rejection, native VACUUM MAINTAIN gate). Verified-remaining gaps only:

### DDL statements absent (parse = syntax error)
- **CREATE DOMAIN** (`CREATE DOMAIN d AS int CHECK (...)`) — no CreateDomainInfo in the duckdb fork;
  DOMAIN exists only as a keyword in GRANT/SECURITY-LABEL object lists. Blocks DOMAIN USAGE grants.
- **ALTER SEQUENCE ... RESTART [WITH n] / CACHE n** — SequenceOption grammar lacks both; serened's
  Sequence object HAS the machinery (Read/Write counter + SequenceOptions.cache), so this is
  grammar + transformer + route-to-catalog, not new storage. Smallest of the DDL gaps.
- **CREATE EVENT TRIGGER** — pg_event_trigger catalog table exists (empty) but no DDL-fire hook or
  trigger-function execution. (PG 18.3 oracle also errored here w/o a function, but the statement
  is a hard syntax error in serened — the whole command is absent.)
- **ALTER DATABASE ... SET <guc>** / **ALTER ROLE ... IN DATABASE ... SET** / **ALTER ROLE ALL SET**
  — per-(role,db) GUC defaults. Needs a pg_db_role_setting-style store (new object type). One family.
- **GRANT SET / ALTER SYSTEM ON PARAMETER ... TO role** — parameter privileges (needs pg_parameter_acl).
- **CREATE AGGREGATE**, **CREATE COLLATION** — absent.
- **CREATE TABLESPACE**, **CREATE EXTENSION**, **CREATE RULE**, **CREATE TRIGGER** — absent (some by design).
- **COMMENT ON ROLE** — syntax error; and full support is blocked by the RoleData serializer trap
  (retrievable comment needs a new field or a side-table object type).
- **GRANT ON LANGUAGE / TABLESPACE** — serened models neither object as first-class.

### Functions absent
- **has_parameter_privilege**, **has_largeobject_privilege** — missing.
- **to_regclass** (and likely the to_reg* family) — missing.
- **regproc / regprocedure input cast** (`'sum'::regproc`, `'sum(int)'::regprocedure`) — no
  name/signature->oid resolution (falls back to int-parse and fails). ROOT CAUSE COUPLED BELOW.
- **pg_proc is EMPTY (0 rows)** — functions are not surfaced in the system catalog at all. This is
  the keystone blocking: regprocedure cast, has_function_privilege by-oid, pg_get_function*,
  pg_get_functiondef, and any \df-style introspection.

### Behavioral divergences (parse OK, wrong result)
- **`<array>::text` renders `[1, 2, 3]` not PG's `{1,2,3}`** — cross-cutting cast issue (the
  pg_array_text path applies to relacl etc. but NOT to a plain `::text` cast). Also element ordering.
- **RLS has no runtime** — CREATE/ALTER/DROP POLICY + ALTER TABLE ENABLE ROW LEVEL SECURITY parse
  (grammar/transform done) but policies are never enforced at query time (no plan-time filter
  injection); the pragma handlers are unregistered so enforcement hard-errors.
- **SECURITY DEFINER is a no-op** — SQL functions inline and run as the caller; no privilege elevation.
- **has_server/fdw_privilege** return t for nonexistent objects (FDW subsystem absent).

### Auth / connection
- **HBA `peer` / `cert` / `ident` / `ldap` / `gss`** parse but never execute (only trust/password/
  scram/md5 + host matching are live). `peer` is the valuable one for local sockets.

### Small keyword/grammar gaps
- **GRANT <role> TO CURRENT_USER / CURRENT_ROLE / SESSION_USER** — the role-membership GRANT path
  treats the keyword as a literal role name ("role \"current_user\" does not exist"). Object grants
  already resolve these via ResolveGranteeId; the membership path does not.

## 2026-07-14 — Investigation: CREATE DOMAIN / event triggers / ALTER SEQUENCE RESTART-CACHE
Deep-dived all three (source-traced + live-probed). Effort/feasibility verdicts:

### ALTER SEQUENCE RESTART [WITH n] / CACHE n  — SMALL, fully completable, no new storage
- Grammar: add `SeqRestart <- 'RESTART' ('WITH'? Expression)?` and `SeqCache <- 'CACHE' Expression`
  to `SequenceOption` (create_sequence.gram); they then flow into ALTER via `SetSequenceOption <-
  SequenceOption+`. Add `SEQ_RESTART` / `SEQ_CACHE` to the `SequenceInfo` enum
  (create_sequence_info.hpp) + carry a Value.
- Transformer: `TransformSetSequenceOption` (transform_alter.cpp) currently THROWS
  NotImplementedException for everything except OWNED BY — so ALTER-of-any-option is unimplemented
  today. Build a real AlterSequenceInfo (RESTART value / CACHE value) instead of the throw.
- Catalog side: `SereneDBSchemaEntry::Alter` (duckdb_schema_entry.cpp) dispatches by AlterType; add
  an ALTER_SEQUENCE case. serened's `Sequence` ALREADY has `Read()`/`Write(value)` (counter) and
  `SequenceOptions.cache` — RESTART = `Write(start_value or given-increment)`, CACHE = update
  options.cache via a COW `ChangeSequence` (mirror ChangeTable). Need a new `catalog::ChangeSequence`
  method (none exists) + wire cache through CreateSequence too (CREATE path doesn't set options.cache
  today). Est: grammar + 1 enum + 1 transformer + 1 catalog method + 1 schema-entry case. ~half a day.
- Note: serened SequenceOptions.cache exists but is UNUSED on create; RESTART semantics = PG resets
  the counter so the next nextval returns the restart value.

### CREATE DOMAIN — MEDIUM (no-check alias) to LARGE (checked domain, PG-faithful)
- serened models user types as `PgSqlType` wrapping duckdb `CreateTypeInfo{name, LogicalType type}`.
  A domain is a pg_type row (typtype='d') = base type + optional NOT NULL + CHECK(VALUE ...).
- NO-CHECK alias domain (`CREATE DOMAIN d AS int`): cheap — a PgSqlType whose LogicalType is the
  base type + a grammar rule + CreateType route. But it wouldn't enforce anything (just an alias).
- CHECKED domain (PG's real value): the CHECK must run at cast/assignment time in ARBITRARY
  expression contexts (`'x'::d`, function args, casts) — not just table columns. duckdb's type system
  has no per-type constraint hook; CreateTypeInfo carries no check expr. Table CHECK constraints ARE
  enforced (via duckdb constraint machinery) but that's column-scoped, not type-scoped. A faithful
  checked domain needs a cast-time validation hook in the duckdb core = real type-system work.
  Verdict: shipping only the no-check alias would be a false-support partial (silently skips the
  constraint) — the exact anti-pattern to avoid. Full support is a core-fork feature.

### Event triggers — LARGEST, blocked on absent prerequisites
- pg_event_trigger catalog table exists but is a STUB (0 rows, empty GetTableData).
- NO DDL-fire hook anywhere (no ddl_command_start/end / sql_drop dispatch point).
- NO regular CREATE TRIGGER either (syntax error) — zero trigger-firing infrastructure to build on.
- NO plpgsql (`CREATE FUNCTION ... LANGUAGE plpgsql` = syntax error); only SQL functions execute,
  and event-trigger functions are conventionally plpgsql and receive event-trigger context
  (TG_EVENT/TG_TAG) that has no representation here.
- Verdict: this is build-a-subsystem territory (DDL dispatch hook + trigger-function execution +
  event-trigger context + likely plpgsql). Far larger than the other two combined; not a discrete fix.

RECOMMENDED ORDER if we proceed: ALTER SEQUENCE (small, clean, ship it) → then decide CREATE DOMAIN
(only if we accept it's a core-fork checked-domain feature, else skip the alias-only partial) →
event triggers is its own project, not a backlog item.

### PG-18-docs pass (authoritative semantics for the three; refines the probe-based verdicts above)

**ALTER SEQUENCE (sql-altersequence.html):**
- `RESTART [WITH n]` = "similar to calling setval with is_called = false: the specified value will be
  returned by the NEXT call of nextval". Bare `RESTART` = "equivalent to supplying the start value that
  was recorded by CREATE SEQUENCE or last set by ALTER SEQUENCE START WITH".
- `START WITH n` changes ONLY the recorded start ("has no effect on the current sequence value; it
  simply sets the value that future ALTER SEQUENCE RESTART commands will use") — so a full impl also
  wants START WITH on ALTER (currently every non-OWNED option throws NotImplemented).
- `CACHE n`: minimum 1; "if unspecified, the old cache value will be maintained"; other backends
  "use up all cached values prior to noticing the changed sequence generation parameters".
- Privilege: "You must own the sequence to use ALTER SEQUENCE."
- CAVEAT: "In contrast to a setval call, a RESTART operation on a sequence is TRANSACTIONAL and blocks
  concurrent transactions from obtaining numbers from the same sequence." serened DDL is
  non-transactional (sanctioned divergence) → serened RESTART will be immediate/non-rollbackable;
  document as falling under the existing non-txn-DDL sanction.
- Also in PG's ALTER SEQUENCE (already-supported or out of scope here): AS data_type, RENAME TO,
  OWNER TO, SET SCHEMA, SET LOGGED/UNLOGGED.

**CREATE DOMAIN (sql-createdomain.html + sql-alterdomain.html):**
- Core semantic confirmed: "Domain constraints are checked when CONVERTING a value to the domain
  type" — cast/assignment-time, any expression context. CHECK uses VALUE; TRUE/UNKNOWN pass, FALSE
  errors; no subqueries; multiple CHECKs fire in alphabetical order. Plus COLLATE, DEFAULT
  (domain default overrides type default, column default overrides domain), NOT NULL/NULL.
- Requires USAGE on the underlying type. Creates a pg_type row typtype='d'.
- ALTER DOMAIN is a whole family and is heavier than expected: ADD CONSTRAINT / SET NOT NULL must
  RECHECK ALL STORED COLUMNS of the domain type across every table (+ NOT VALID / VALIDATE
  CONSTRAINT two-phase); SET/DROP DEFAULT; RENAME/DROP CONSTRAINT; OWNER/RENAME/SET SCHEMA.
- Verdict unchanged but stronger: faithful domains = cast-time type-constraint hook in the duckdb
  core + stored-data revalidation machinery. The no-check alias slice stays a forbidden false partial.

**Event triggers (event-trigger-definition.html + sql-createeventtrigger.html):**
- FIVE events in PG 18: login, ddl_command_start, ddl_command_end, sql_drop, table_rewrite.
- Trigger functions "can be written in any procedural language ... or in C, but NOT in plain SQL" —
  serened has ONLY plain-SQL functions ⇒ plpgsql (or another PL) is a hard prerequisite, confirmed.
- Function must take no args + return type event_trigger; superuser-only to create; WHEN TAG IN (...)
  filter; alphabetical firing order; disabled via event_triggers=false / single-user mode.
- Does NOT fire for shared objects (databases, roles, tablespaces, parameter privileges, ALTER
  SYSTEM) nor for event-trigger commands themselves.
- NEW ARCHITECTURAL BLOCKER (beyond missing plumbing): "if a ddl_command_end trigger fails with an
  error, THE EFFECTS OF THE DDL STATEMENT WILL BE ROLLED BACK". serened DDL commits immediately
  (non-transactional) — a ddl_command_end error CANNOT roll the DDL back here. Event triggers'
  error contract assumes transactional DDL, so a faithful port conflicts with serened's DDL model
  itself, not just missing infrastructure. ddl_command_start-only (abort-before-execute works
  pre-commit) would be the only semantically-honest subset.

## 2026-07-14 — FULL PostgreSQL-vs-SereneDB feature survey (systematic, live-verified)
Method: took PG 18.3's complete SQL command list (psql \h, 180 commands) + major feature areas +
type system, probed EVERY family against a fresh serened AND the postgres:18.3 oracle, classified
by error kind (serened syntax error = grammar-absent vs semantic = functionally-absent). This is
the definitive "PG has it, we don't" inventory. Script: scratchpad/pg_feature_survey.py.

### TIER 1 — core SQL apps/ORMs/drivers rely on daily (ALL absent, syntax error)
- **SAVEPOINT / ROLLBACK TO SAVEPOINT / RELEASE SAVEPOINT** — no subtransactions at all.
  (Django atomic(), SQLAlchemy nested transactions, psycopg break on this.)
- **SELECT ... FOR UPDATE / FOR SHARE / NOWAIT / SKIP LOCKED** — no row-locking clause.
- **Cursors: DECLARE / FETCH / MOVE / CLOSE** — absent.
- **jsonb TYPE DOES NOT EXIST** — `'{"a":1}'::jsonb` fails. `json` type + to_json/row_to_json work,
  but jsonb operators, json_agg, jsonb_* functions, jsonpath (jsonb_path_query) all absent.
- **LISTEN / NOTIFY / UNLISTEN / pg_notify** — "LISTEN is not supported by SereneDB yet".
- **Materialized views** — CREATE/REFRESH/DROP MATERIALIZED VIEW all absent.
- **Declarative partitioning** (PARTITION BY / PARTITION OF) + **table inheritance** (INHERITS) — absent.
- **Identity columns** (GENERATED ... AS IDENTITY) — absent (serial + GENERATED STORED work).
- **Deferrable constraints** (DEFERRABLE INITIALLY DEFERRED) + **SET CONSTRAINTS** — absent.
- **TRIGGERS** — CREATE/ALTER/DROP TRIGGER entirely absent (no row triggers at all).
- **Advisory locks** (pg_advisory_lock family) — absent.
- **Two-phase commit** — PREPARE TRANSACTION / COMMIT PREPARED / ROLLBACK PREPARED absent.
- **ALTER TABLE ... SET SCHEMA** (and ALTER SEQUENCE SET SCHEMA; whole SET SCHEMA family) — absent.
- **ALTER TYPE — every form** absent, including ALTER TYPE ... ADD VALUE (can't extend an enum!).
- **CREATE TABLE (LIKE ...)** — absent. **UNLOGGED tables** — absent. **Exclusion constraints** — absent.

### TIER 2 — DDL object families absent (grammar)
DOMAIN (CREATE/ALTER/DROP) · EVENT TRIGGER · EXTENSION · FOREIGN DATA WRAPPER / SERVER /
USER MAPPING / FOREIGN TABLE / IMPORT FOREIGN SCHEMA · TABLESPACE (CREATE/ALTER/DROP) ·
AGGREGATE (CREATE/ALTER/DROP) · OPERATOR / OPERATOR CLASS / OPERATOR FAMILY · CAST · COLLATION ·
CONVERSION · LANGUAGE · RULE · STATISTICS (extended stats) · ACCESS METHOD · TRANSFORM ·
LARGE OBJECT (+ lo_* functions) · TEXT SEARCH CONFIGURATION / PARSER / TEMPLATE · ALTER ROUTINE ·
DO (anonymous blocks; needs plpgsql) · ALTER FUNCTION with (argtypes) parens form.
Functionally absent (parses, unimplemented): ALTER SCHEMA RENAME ("not yet supported");
CREATE PUBLICATION / SUBSCRIPTION (transform emits create_publication/create_subscription pragmas
that were never registered — same disease as the old RLS handlers); CREATE TEXT SEARCH DICTIONARY
(TEMPLATE = simple) errors "PRAGMA value cannot contain column names".

### TIER 3 — admin/maintenance absent
ALTER SYSTEM (any form) · CLUSTER · REINDEX · ALTER DATABASE SET guc / ALTER ROLE IN DATABASE SET /
ALTER ROLE ALL SET (known) · LOAD (sanctioned: extensions compiled in).

### TIER 4 — PG types/functions absent (expression level)
jsonb (type) · cidr · macaddr/macaddr8 · money · geometric types+fns (point/box/line/circle/...) ·
tsvector type (tsquery oddly EXISTS) · range types (int4range/tsrange/... no constructors) ·
full-text search fns (to_tsvector/to_tsquery/ts_rank/plainto_tsquery...) · json_agg/jsonb_agg +
jsonb_* fn family + jsonpath · xml* functions (xmlcomment etc.) · advisory-lock fn family ·
lo_* (large objects) · pg_notify · txid_current (exists but errors: "DuckTransaction::Get called
on non-DuckDB transaction" — broken, not absent).
WORKING for contrast: json, to_json/row_to_json, uuid+gen_random_uuid, inet, interval, bytea,
bit strings, arrays+operators, tsquery, num_nonnulls, enum+composite CREATE TYPE.

### Divergences noticed in passing (parse OK, semantics differ)
- TABLESAMPLE BERNOULLI(50): duckdb reads "50 rows" (PG: 50 percent); works with `50 PERCENT`.
- CREATE TYPE ... AS RANGE parses as generic type-mod and fails "Expected a constant".

### Confirmed WORKING (for the record — full families green on both engines)
Transactions (BEGIN/COMMIT/ROLLBACK/ABORT/SET TRANSACTION) · PREPARE/EXECUTE/DEALLOCATE ·
INSERT ON CONFLICT / RETURNING · MERGE · SELECT INTO / CTAS · WITH RECURSIVE · window fns ·
GROUPING SETS/CUBE/ROLLUP · FILTER · WITHIN GROUP · LATERAL · COLLATE expr · CREATE PROCEDURE +
CALL · CHECKPOINT · VACUUM/ANALYZE · ALTER TABLE ADD/DROP COLUMN · ALTER INDEX/VIEW RENAME ·
generated STORED columns · CREATE DATABASE/SCHEMA/ROLE/SEQUENCE/INDEX/VIEW/TYPE(enum,composite) ·
GRANT/REVOKE + full RBAC surface · LOCK TABLE · SECURITY LABEL · COMMENT (table/column).

### Actionable checklist (the survey as a todo backlog; S/M/L/XL = effort)
Tier 1 — core SQL (apps/ORMs/drivers break without these):
- [ ] **L** — SAVEPOINT / ROLLBACK TO / RELEASE (subtransactions; needs engine-level nested-txn or
      undo-scope support, not just grammar).
- [ ] **L** — SELECT ... FOR UPDATE / FOR SHARE [NOWAIT | SKIP LOCKED] (row locking; MVCC model
      decides whether this is real locks or PG-compatible no-op-with-semantics).
- [ ] **M** — cursors: DECLARE/FETCH/MOVE/CLOSE (duckdb has streaming results to back FETCH).
- [ ] **M/L** — jsonb: type alias→json gets the type + operators; real parity needs jsonb_* fn
      family + json_agg + jsonpath. Stageable: type+operators (M), fn family (M), jsonpath (L).
- [ ] **L** — LISTEN/NOTIFY/UNLISTEN + pg_notify (cross-connection delivery at the wire layer).
- [ ] **L** — materialized views (CREATE/REFRESH/DROP; storage-backed snapshot of a query).
- [ ] **XL** — declarative partitioning + INHERITS.
- [ ] **M** — identity columns (GENERATED AS IDENTITY; lower onto the existing serial/sequence path).
- [ ] **L** — deferrable constraints + SET CONSTRAINTS (needs commit-time constraint queue).
- [ ] **XL** — row TRIGGERS (CREATE/ALTER/DROP TRIGGER + firing in DML pipeline; prereq for event
      triggers and most migration tooling).
- [ ] **M** — advisory locks (pg_advisory_* fn family over a server-global lock table).
- [ ] **XL** — two-phase commit (PREPARE TRANSACTION family).
- [ ] **S/M** — ALTER TABLE/SEQUENCE/VIEW/FUNCTION ... SET SCHEMA (catalog re-parent; ChangeOwner-
      style COW seam exists).
- [ ] **M** — ALTER TYPE: ADD VALUE (enum extension) first, then RENAME/OWNER/SET SCHEMA forms.
- [ ] **S** — CREATE TABLE (LIKE ... [INCLUDING ...]).
- [ ] **S** — UNLOGGED tables (parse + treat as regular; document the durability divergence) — or
      reject honestly; decide.
- [ ] **M** — exclusion constraints (EXCLUDE (col WITH op)).
Tier 2 — DDL object families (each = grammar + object model; sizes assume faithful behavior):
- [ ] **L** — DOMAIN family (cast-time constraint hook in duckdb core; alias-only partial is banned).
- [ ] **L** — EXTENSION (even a stub registry for pg_dump/tooling compat: CREATE EXTENSION IF NOT
      EXISTS for whitelisted names like pgcrypto/uuid-ossp whose fns already exist).
- [ ] **M** — TABLESPACE family (parse + single-default-tablespace model like pg_default, real
      directories optional; unblocks pg_dump restores that carry TABLESPACE clauses).
- [ ] **M** — AGGREGATE (CREATE AGGREGATE over existing duckdb aggregate combinators).
- [ ] **L** — OPERATOR / OPERATOR CLASS / OPERATOR FAMILY.
- [ ] **M** — CAST (CREATE CAST wiring duckdb's cast registration).
- [ ] **S/M** — COLLATION (duckdb has collations; map CREATE COLLATION locale= onto them).
- [ ] **S** — fix unregistered-pragma zombies: CREATE PUBLICATION / CREATE SUBSCRIPTION transforms
      emit create_publication/create_subscription pragmas that don't exist — either register
      honest not-supported handlers (like SECURITY LABEL) or remove the grammar.
- [ ] **S** — CREATE TEXT SEARCH DICTIONARY (TEMPLATE = simple) → "PRAGMA value cannot contain
      column names" (serened HAS TS dictionaries; the PG option-form doesn't map).
- [ ] **M** — DO blocks (needs an inline SQL-block executor; full value only with plpgsql).
- [ ] rest of Tier 2 (EVENT TRIGGER, FDW/SERVER/FOREIGN TABLE, RULE, STATISTICS, ACCESS METHOD,
      TRANSFORM, CONVERSION, LANGUAGE, LARGE OBJECT): park until a concrete consumer appears.
Tier 3 — admin:
- [ ] **M** — ALTER SYSTEM SET/RESET (persist to a server-config store; interacts w/ HBA design).
- [ ] **S** — REINDEX (map to serened index rebuild).
- [ ] **S** — CLUSTER (accept + no-op with notice, or reject; decide honestly).
- [ ] **M** — ALTER DATABASE SET / ALTER ROLE IN DATABASE SET / ALTER ROLE ALL SET (needs the
      pg_db_role_setting-style new object type; also listed in the RBAC tail).
Tier 4 — types/functions:
- [ ] **S ea** — cidr, macaddr/macaddr8, money types (mostly parse/render over existing physics).
- [ ] **M** — range types (int4range/tsrange/... + operators).
- [ ] **M** — FTS functions (to_tsvector/to_tsquery/ts_rank/plainto_tsquery) + tsvector type — could
      lower onto serened's own search/inverted-index machinery.
- [ ] **S** — fix txid_current (BROKEN: "DuckTransaction::Get called on non-DuckDB transaction");
      also add pg_current_xact_id.
- [ ] **S ea** — geometric types (point/box/...) — park unless something needs them.
- [ ] **S** — TABLESAMPLE BERNOULLI(n) divergence: n = rows (duckdb) vs percent (PG).
