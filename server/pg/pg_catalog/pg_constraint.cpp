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

#include "catalog/catalog.h"
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
// FKs carry no stored ObjectId; synthesize a constraint OID. Bit 61 keeps it
// clear of raw ObjectIds and the bit-62 synthetic PK index OIDs.
constexpr uint64_t kSyntheticFkBit = uint64_t{1} << 61;
constexpr uint64_t kSyntheticUniqueBit = uint64_t{1} << 60;

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgConstraint>::GetTableData() {
  auto catalog = _config.EnsureCatalogSnapshot();

  std::vector<PgConstraint> values;
  std::deque<std::string> conname_storage;
  std::vector<std::vector<int16_t>> conkey_storage;
  std::vector<std::vector<int16_t>> confkey_storage;

  for (const auto& schema : catalog->GetSchemas(GetDatabaseId())) {
    for (const auto& table :
         catalog->GetTables(GetDatabaseId(), schema->GetName())) {
      auto& pk_columns = table->PKColumns();
      auto& columns = table->Columns();

      // Primary key constraint
      if (!pk_columns.empty()) {
        std::vector<int16_t> conkey;
        conkey.reserve(pk_columns.size());
        for (auto pk_id : pk_columns) {
          const auto pos = table->ColumnPosById(pk_id);
          if (pos < columns.size()) {
            conkey.push_back(static_cast<int16_t>(pos + 1));
          }
        }

        conname_storage.push_back(table->PKName().empty()
                                    ? std::string{table->GetName()} + "_pkey"
                                    : std::string{table->PKName()});
        conkey_storage.push_back(std::move(conkey));
        values.push_back({
          .oid = table->GetId().id(),
          .conname = conname_storage.back(),
          .connamespace = schema->GetId().id(),
          .contype = PgConstraint::Contype::PrimaryKey,
          .condeferrable = false,
          .condeferred = false,
          .conenforced = true,
          .convalidated = true,
          .conrelid = table->GetId().id(),
          .contypid = 0,
          // Synthetic PK index OID (see PkIndexOid in fwd.h and pg_index.cpp).
          .conindid = PkIndexOid(table->GetId().id()),
          .conparentid = 0,
          .confrelid = 0,
          .confupdtype = PgConstraint::Confchgtype::NoAction,
          .confdeltype = PgConstraint::Confchgtype::NoAction,
          .confmatchtype = PgConstraint::Confmatchtype::Simple,
          .conislocal = true,
          .coninhcount = 0,
          .connoinherit = false,
          .conperiod = false,
          .conkey = conkey_storage.back(),
        });
      }

      // Check constraints
      for (const auto& check : table->CheckConstraints()) {
        conname_storage.emplace_back(check.GetName());
        // PostgreSQL exposes a NOT NULL constraint as contype 'n' with the
        // column in conkey, even though serenedb stores it as an
        // OPERATOR_IS_NOT_NULL check.
        auto not_null_col = check.IsNotNull(table->Columns());
        conkey_storage.emplace_back();
        if (not_null_col) {
          conkey_storage.back().push_back(
            static_cast<int16_t>(*not_null_col + 1));
        }
        values.push_back({
          .oid = check.GetId().id(),
          .conname = conname_storage.back(),
          .connamespace = schema->GetId().id(),
          .contype = not_null_col ? PgConstraint::Contype::NotNull
                                  : PgConstraint::Contype::Check,
          .condeferrable = false,
          .condeferred = false,
          .conenforced = true,
          .convalidated = true,
          .conrelid = table->GetId().id(),
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
          .conkey = conkey_storage.back(),
        });
      }

      // Foreign key constraints (contype 'f'). A native duckdb table surfaces
      // these in pg_constraint; the facade must too.
      const auto& fks = table->ForeignKeys();
      for (size_t fk_idx = 0; fk_idx < fks.size(); ++fk_idx) {
        const auto& fk = fks[fk_idx];
        // referenced_table unset == self-referencing -> this table.
        auto ref_obj = catalog->GetObject<catalog::Table>(fk.referenced_table);
        const catalog::Table& ref = ref_obj ? *ref_obj : *table;

        std::vector<int16_t> conkey;
        conkey.reserve(fk.columns.size());
        std::string first_col;
        for (auto col_id : fk.columns) {
          const auto pos = table->ColumnPosById(col_id);
          if (pos < columns.size()) {
            conkey.push_back(static_cast<int16_t>(pos + 1));
            if (first_col.empty()) {
              first_col = std::string{columns[pos].GetName()};
            }
          }
        }
        std::vector<int16_t> confkey;
        confkey.reserve(fk.referenced_columns.size());
        for (auto col_id : fk.referenced_columns) {
          const auto pos = ref.ColumnPosById(col_id);
          if (pos < ref.Columns().size()) {
            confkey.push_back(static_cast<int16_t>(pos + 1));
          }
        }

        conname_storage.push_back(fk.name.empty()
                                    ? std::string{table->GetName()} + "_" +
                                        first_col + "_fkey"
                                    : fk.name);
        conkey_storage.push_back(std::move(conkey));
        confkey_storage.push_back(std::move(confkey));
        values.push_back({
          .oid = kSyntheticFkBit | (table->GetId().id() * 256 + fk_idx),
          .conname = conname_storage.back(),
          .connamespace = schema->GetId().id(),
          .contype = PgConstraint::Contype::ForeignKey,
          .condeferrable = false,
          .condeferred = false,
          .conenforced = true,
          .convalidated = true,
          .conrelid = table->GetId().id(),
          .contypid = 0,
          .conindid = 0,
          .conparentid = 0,
          .confrelid = ref.GetId().id(),
          .confupdtype = PgConstraint::Confchgtype::NoAction,
          .confdeltype = PgConstraint::Confchgtype::NoAction,
          .confmatchtype = PgConstraint::Confmatchtype::Simple,
          .conislocal = true,
          .coninhcount = 0,
          .connoinherit = false,
          .conperiod = false,
          .conkey = conkey_storage.back(),
          .confkey = confkey_storage.back(),
        });
      }

      // Unique constraints (contype 'u'). Non-PK UNIQUE; a native duckdb table
      // surfaces these in pg_constraint, so the facade must too.
      const auto& uniques = table->UniqueConstraints();
      for (size_t uq_idx = 0; uq_idx < uniques.size(); ++uq_idx) {
        const auto& uq = uniques[uq_idx];
        std::vector<int16_t> conkey;
        conkey.reserve(uq.columns.size());
        std::string first_col;
        for (auto col_id : uq.columns) {
          const auto pos = table->ColumnPosById(col_id);
          if (pos < columns.size()) {
            conkey.push_back(static_cast<int16_t>(pos + 1));
            if (first_col.empty()) {
              first_col = std::string{columns[pos].GetName()};
            }
          }
        }
        conname_storage.push_back(uq.name.empty()
                                    ? std::string{table->GetName()} + "_" +
                                        first_col + "_key"
                                    : uq.name);
        conkey_storage.push_back(std::move(conkey));
        values.push_back({
          .oid = kSyntheticUniqueBit | (table->GetId().id() * 256 + uq_idx),
          .conname = conname_storage.back(),
          .connamespace = schema->GetId().id(),
          .contype = PgConstraint::Contype::Unique,
          .condeferrable = false,
          .condeferred = false,
          .conenforced = true,
          .convalidated = true,
          .conrelid = table->GetId().id(),
          .contypid = 0,
          .conindid = UniqueIndexOid(table->GetId().id(), uq_idx),
          .conparentid = 0,
          .confrelid = 0,
          .confupdtype = PgConstraint::Confchgtype::NoAction,
          .confdeltype = PgConstraint::Confchgtype::NoAction,
          .confmatchtype = PgConstraint::Confmatchtype::Simple,
          .conislocal = true,
          .coninhcount = 0,
          .connoinherit = false,
          .conperiod = false,
          .conkey = conkey_storage.back(),
        });
      }
    }
  }

  auto result = CreateColumns<PgConstraint>(values.size());

  for (size_t row = 0; row < values.size(); ++row) {
    // FK rows carry confkey and so need its NULL bit cleared.
    const auto mask = values[row].confkey.empty() ? kNullMask : kFkNullMask;
    WriteData(result, values[row], mask, row);
  }

  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
