# PR review order

Branch: `pashandor789/inpiredbyserenedb` (uncommitted). Two repos: the superproject and the
`third_party/duckdb` submodule. ~1,394 lines in `server/` + ~258 hand-written in the fork + 30 new
test files.

Ordered by security-criticality × complexity — riskiest/most-load-bearing first, while attention is
fresh; each batch gives context for the next. The RBAC-into-binder refactor is NOT in this diff (it
was attempted and reverted; see `no_rule.md`).

Skip in the fork diff (generated, not hand-written): `inlined_grammar.hpp`, `inlined_grammar.gram`,
`keyword_map.cpp`.

---

## 1 — Catalog enforcement core  (highest stakes: authz decisions are made here)
A bug here is a silent privilege escalation. Focus on `DropOwned` drop-ordering + grant-revoke, and
schema-USAGE-before-object ordering.

- `server/catalog/catalog.cpp` (+410) — ReassignOwned / DropOwned / CollectOwnedTargets /
  DescribeTarget, predefined-role seeding, DefaultAclForOwner, schema-USAGE EnforceRead,
  RoleReferenceIds, index-rename ownership
- `server/catalog/catalog.h` (+26)
- `server/catalog/identifiers/object_id.h` (+21 — 16 predefined-role ids)
- `server/auth/role_closure.cpp` (+35) / `role_closure.h` (+11) — read/write_all_data, PredefinedModes
- `server/catalog/store/store.cpp` (+11) — bootstrap `public` PUBLIC=USAGE seed

## 2 — Auth / connection (network)  (who gets in; new attack surface)
Verify fail-closed paths: empty CN, failed cert verify, non-TLS, peer uid mismatch.

- `server/network/pg/pg_wire_session.cpp` (+234) — peer/cert auth, rolconfig-at-connect,
  auth-failure logging, RequireLockInTransactionBlock, CREATE TEMP gate (Parse path), client_addr
- `server/network/socket.h` (+24) — PeerCertCommonName
- `server/network/pg/hba.cpp` (+12) / `hba.h` (+14) — DeferredCert / DeferredPeerIdent
- `server/network/pg/pg_wire_session.h` (+6)
- `server/pg/connection_context.h` (+6)

## 3 — RBAC command layer
- `server/pg/commands/rbac.cpp` (+194) — ResolveRoleSpecName, GRANTED-BY validation, ApplyAclGrant
  revoke triage, LockTable, ReassignOwned/DropOwned wrappers
- `server/pg/commands/rbac.h` (+9)
- `server/connector/optimizer/rbac.cpp` (+9) — comment trim only (the reverted hooks are gone)

## 4 — Grammar + transformers (fork, hand-written only)
Loud-failure / heavily test-covered → lower silent-risk than authz logic.

- `src/parser/peg/transformer/transform_rbac.cpp` (+183) — SECURITY LABEL, LOCK, multi-target GRANT,
  TransformOwnerRoleSpec
- `src/parser/peg/grammar/statements/rbac.gram` (+32), `common.gram` (+4), `set.gram` (+4)
- `src/parser/peg/transformer/transform_set.cpp` (+20), `transform_generated.cpp` (+35 — SET SESSION
  ROLE), `transform_deallocate.cpp` (+18)
- `src/include/duckdb/parser/peg/transformer/peg_transformer.hpp` (+15),
  `peg_transformer_factory.cpp` (+4)

## 5 — Introspection functions / pg_catalog  (read-only surface)
- `server/connector/functions/system.cpp` (+162) — has_column_privilege overloads,
  HasObjectPrivilegeNameOid3
- `server/connector/functions/inout.cpp` (+67) — regrole + regprocedure→oid casts
- `server/connector/functions/sequence.cpp` (+22), `duckdb_rbac_function.cpp` (+103),
  `duckdb_tokenizer_function.cpp` (+4), `duckdb_client_state.cpp` (+1)
- `server/pg/system_functions.h` (+35 — makeaclitem, has_tablespace_privilege),
  `system_views.h` (+4), `pg_catalog/pg_authid.cpp` (+12), `pg_catalog/pg_sequence.{cpp,h}`,
  `pg_catalog/sdb_progress.{cpp,h}`, `progress_registry.{cpp,h}`, `pg/CMakeLists.txt`
- `server/query/config_variables.cpp` (+21) — password_encryption / row_security GUCs

## 6 — Tests  (read alongside the code they cover, or last as a batch)
- 27 new sqllogic: `tests/sqllogic/any/pg/rbac/*` + `tests/sqllogic/sdb/pg/rbac/*`
- 3 network: `tests/network/{peer_auth,cert_auth,rbac_temp}_test.py`
- gtest: `tests/server/basics/network_pg_hba_test.cpp` (Cert deferred + MethodExecutability)

---

## Notes
- A 6-agent automated review already swept this diff once; 8 real bugs were fixed + ~12
  simplifications applied. This pass is human confirmation — spend energy on batches 1–2.
- Verification state at PR: `any/pg/rbac` 284/284, `sdb/pg/rbac` + system suites green (both
  protocols, `--label serenedb`), full network suite + 36 HBA gtests green.
