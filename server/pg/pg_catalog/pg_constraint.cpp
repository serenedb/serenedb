////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "pg/pg_catalog/pg_constraint.h"

#include <deque>
#include <duckdb/parser/constraints/list.hpp>
#include <span>
#include <string_view>
#include <utility>

#include "auth/role_closure.h"
#include "basics/containers/flat_hash_map.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "connector/duckdb_catalog_sets.h"
#include "connector/duckdb_table_entry.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgConstraint::conbin),
  GetIndex(&PgConstraint::confkey),
  GetIndex(&PgConstraint::conpfeqop),
  GetIndex(&PgConstraint::conppeqop),
  GetIndex(&PgConstraint::conffeqop),
  GetIndex(&PgConstraint::confdelsetcols),
  GetIndex(&PgConstraint::conexclop),
});

// FOREIGN KEY rows additionally populate confkey (the referenced columns), so
// clear its NULL bit for them.
constexpr uint64_t kFkNullMask =
  kNullMask & ~(uint64_t{1} << GetIndex(&PgConstraint::confkey));
// CHECK rows carry the deparsed body in conbin.
constexpr uint64_t kCheckNullMask =
  kNullMask & ~(uint64_t{1} << GetIndex(&PgConstraint::conbin));
// FKs carry no stored ObjectId; synthesize a constraint OID. Bit 61 keeps it
// clear of raw ObjectIds and the bit-62 synthetic PK index OIDs.

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgConstraint>::GetTableData() {
  std::vector<PgConstraint> values;
  std::deque<std::string> conname_storage;
  std::deque<std::string> conbin_storage;
  std::vector<std::vector<int16_t>> conkey_storage;
  std::vector<std::vector<int16_t>> confkey_storage;

  auto& context = _config.GetClientContext();

  // A foreign key reports the oid of the relation it references and the index
  // backing that relation's key, so every table is collected up front. By the
  // id the constraint carries and not by the qualified name it also carries:
  // the name is only what it was when the definition was written, and a rename
  // since has moved it.
  containers::FlatHashMap<ObjectId, const connector::SereneDBTableEntry*>
    tables_by_id;
  connector::VisitTableEntries(context, GetDatabaseId(),
                               [&](const catalog::CreateSchemaInfo&,
                                   const connector::SereneDBTableEntry& table) {
                                 tables_by_id.emplace(catalog::IdOf(table),
                                                      &table);
                               });

  // The index enforcing the key a foreign key points at: its primary key,
  // which is the only key a foreign key may reference.
  const auto referenced_index =
    [](const connector::SereneDBTableEntry& referenced) -> Oid {
    for (const auto& constraint : referenced.GetConstraints()) {
      if (constraint->type != duckdb::ConstraintType::UNIQUE) {
        continue;
      }
      const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
      if (unique.IsPrimaryKey()) {
        return unique.host_index_id;
      }
    }
    return 0;
  };

  // A key's columns are positions in the entry's own column list, which is
  // what attnum counts.
  const auto attnums = [](std::span<const duckdb::PhysicalIndex> keys) {
    std::vector<int16_t> out;
    out.reserve(keys.size());
    for (const auto key : keys) {
      out.push_back(static_cast<int16_t>(key.index + 1));
    }
    return out;
  };

  connector::VisitTableEntries(
    context, GetDatabaseId(),
    [&](const catalog::CreateSchemaInfo& schema,
        const connector::SereneDBTableEntry& table) {
      const auto relid = catalog::IdOf(table).id();
      const auto namespace_id = schema.GetId().id();
      const auto base = [&](PgConstraint::Contype contype, Oid oid,
                            std::string_view name) {
        return PgConstraint{
          .oid = oid,
          .conname = name,
          .connamespace = namespace_id,
          .contype = contype,
          .condeferrable = false,
          .condeferred = false,
          .conenforced = true,
          .convalidated = true,
          .conrelid = relid,
          .contypid = 0,
          .conindid = 0,
          .conparentid = 0,
          .confrelid = 0,
          .confupdtype = PgConstraint::Confchgtype::NoAction,
          .confdeltype = PgConstraint::Confchgtype::NoAction,
          .confmatchtype = PgConstraint::Confmatchtype::Simple,
          .conislocal = true,
          .coninhcount = 0,
          .connoinherit = false,
          .conperiod = false,
        };
      };

      for (const auto& constraint : table.GetConstraints()) {
        if (constraint->type == duckdb::ConstraintType::INVALID) {
          continue;
        }
        conname_storage.emplace_back(constraint->constraint_name);
        auto row = base(PgConstraint::Contype::Check, constraint->oid,
                        conname_storage.back());
        if (constraint->type == duckdb::ConstraintType::CHECK) {
          conbin_storage.push_back(
            constraint->Cast<duckdb::CheckConstraint>().expression->ToString());
          row.conbin = conbin_storage.back();
        }
        switch (constraint->type) {
          case duckdb::ConstraintType::UNIQUE: {
            const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
            row.contype = unique.IsPrimaryKey()
                            ? PgConstraint::Contype::PrimaryKey
                            : PgConstraint::Contype::Unique;
            row.conindid = unique.host_index_id;
            conkey_storage.push_back(
              connector::KeyConstraintAttnums(table, unique));
            break;
          }
          case duckdb::ConstraintType::NOT_NULL: {
            // PostgreSQL exposes NOT NULL as contype 'n' with the column in
            // conkey.
            row.contype = PgConstraint::Contype::NotNull;
            conkey_storage.push_back({static_cast<int16_t>(
              constraint->Cast<duckdb::NotNullConstraint>().index.index + 1)});
            break;
          }
          case duckdb::ConstraintType::FOREIGN_KEY: {
            const auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
            const auto referenced =
              tables_by_id.find(ObjectId{fk.host_referenced_id});
            const auto& target =
              referenced == tables_by_id.end() ? table : *referenced->second;
            row.contype = PgConstraint::Contype::ForeignKey;
            row.conindid = referenced_index(target);
            row.confrelid = catalog::IdOf(target).id();
            conkey_storage.push_back(attnums(fk.info.fk_keys));
            confkey_storage.push_back(attnums(fk.info.pk_keys));
            row.confkey = confkey_storage.back();
            break;
          }
          default:
            conkey_storage.emplace_back();
            break;
        }
        row.conkey = conkey_storage.back();
        values.push_back(std::move(row));
      }
    });

  auto result = CreateColumns<PgConstraint>(values.size());

  for (size_t row = 0; row < values.size(); ++row) {
    // FK rows carry confkey and CHECK rows conbin, so each clears its own bit.
    auto mask = kNullMask;
    if (!values[row].confkey.empty()) {
      mask = kFkNullMask;
    } else if (values[row].contype == PgConstraint::Contype::Check) {
      mask = kCheckNullMask;
    }
    WriteData(result, values[row], mask, row,
              *sdb::auth::RolesOf(&_config.GetClientContext()));
  }

  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
