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

#include "pg/catalog/builtin/builtin.h"

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_map.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <deque>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/collate_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <string>
#include <vector>

#include "pg/catalog/tables/tables.h"
#include "pg/catalog/views/system_views.h"
#include "pg/types.h"

namespace sdb::pg {
namespace {

constexpr BuiltinType kPgTypes[] = {
#include "pg/catalog/generated/builtin_types.gen.inc"
};

constexpr BuiltinType SdbType(int32_t oid, std::string_view name,
                              int32_t array) {
  return {.oid = oid,
          .name = name,
          .nsp = kPgCatalogSchema,
          .len = -1,
          .byval = false,
          .type = 'b',
          .category = 'U',
          .preferred = false,
          .delim = ',',
          .array = array,
          .align = 'i',
          .storage = 'x',
          .typmod = -1};
}

constexpr const BuiltinType& PgType(int32_t oid) {
  return *absl::c_find_if(
    kPgTypes, [&](const BuiltinType& type) { return type.oid == oid; });
}

constexpr BuiltinType SdbArray(int32_t oid, std::string_view name,
                               int32_t elem) {
  auto row = PgType(kInt4Array);
  row.oid = oid;
  row.name = name;
  row.elem = elem;
  return row;
}

constexpr BuiltinType kSdbTypes[] = {
  SdbType(kVariant, "variant", kVariantArray),
  SdbArray(kVariantArray, "_variant", kVariant),
  SdbType(kTsquery, "tsquery", kTsqueryArray),
  SdbArray(kTsqueryArray, "_tsquery", kTsquery),
  SdbType(kUnion, "union", kUnionArray),
  SdbArray(kUnionArray, "_union", kUnion),
  SdbType(kGeometry, "geometry", kGeometryArray),
  SdbArray(kGeometryArray, "_geometry", kGeometry),
};

constexpr auto kTypes = [] {
  std::array<BuiltinType, std::size(kPgTypes) + std::size(kSdbTypes)> types{};
  absl::c_copy(kSdbTypes, absl::c_copy(kPgTypes, types.begin()));
  return types;
}();

static_assert([] {
  for (const auto& type : kTypes) {
    if ((type.nsp != kPgCatalogSchema && type.nsp != kPgInformationSchema) ||
        type.oid < 0 || static_cast<duckdb::idx_t>(type.oid) >= kMaxSystem) {
      return false;
    }
  }
  return true;
}());

constexpr BuiltinProc kProcs[] = {
#include "pg/catalog/generated/builtin_procs.gen.inc"
};

constexpr BuiltinCollation kCollations[] = {
#include "pg/catalog/generated/builtin_collations.gen.inc"
};

constexpr auto kByOid = [](const auto& lhs, const auto& rhs) {
  return lhs.oid < rhs.oid;
};

static_assert(absl::c_is_sorted(kTypes, kByOid) &&
              absl::c_is_sorted(kProcs, kByOid) &&
              absl::c_is_sorted(kCollations, kByOid));

const std::vector<BuiltinType>& AllTypes() {
  static std::deque<std::string> array_names;
  static const auto kAll = [] {
    std::vector<BuiltinType> types{std::begin(kTypes), std::end(kTypes)};
    const auto add_row_type = [&](duckdb::idx_t relid, std::string_view schema,
                                  std::string_view name, int32_t oid,
                                  int32_t array) {
      if (oid == 0) {
        return;
      }
      const auto nsp = FindSystemObject(kSystemNamespaces, schema)->oid;
      auto row = PgType(kRecord);
      row.oid = oid;
      row.name = name;
      row.nsp = nsp;
      row.type = 'c';
      row.category = 'C';
      row.array = array;
      row.relid = static_cast<int32_t>(relid);
      types.emplace_back(row);
      auto element = PgType(kInt4Array);
      element.oid = array;
      element.name = array_names.emplace_back(absl::StrCat("_", name));
      element.nsp = nsp;
      element.elem = oid;
      element.align = 'd';
      types.emplace_back(element);
    };
    for (const auto* sql : kGeneratedTables) {
      add_row_type(sql->oid, sql->schema, sql->name, sql->row_type,
                   sql->row_array_type);
    }
    for (const auto& view : kExternalViews) {
      add_row_type(view.oid, view.schema, view.name, view.row_type,
                   view.row_array_type);
    }
    absl::c_sort(types, kByOid);
    SDB_ASSERT(
      absl::c_adjacent_find(types, [](const auto& lhs, const auto& rhs) {
        return lhs.oid == rhs.oid;
      }) == types.end());
    return types;
  }();
  return kAll;
}

const std::vector<BuiltinCollation>& Collations() {
  static const auto kAll = [] {
    auto& db = irs::DuckDBEngine::Instance().instance();
    std::vector<const duckdb::CollateCatalogEntry*> entries;
    duckdb::Catalog::GetSystemCatalog(db)
      .GetSchema(duckdb::CatalogTransaction::GetSystemTransaction(db),
                 duckdb::Identifier{DEFAULT_SCHEMA})
      .Scan(duckdb::CatalogType::COLLATION_ENTRY,
            [&](duckdb::CatalogEntry& entry) {
              entries.emplace_back(&entry.Cast<duckdb::CollateCatalogEntry>());
            });
    absl::c_sort(entries, [](const auto* lhs, const auto* rhs) {
      return lhs->name.GetIdentifierName() < rhs->name.GetIdentifierName();
    });
    std::vector<BuiltinCollation> collations{std::begin(kCollations),
                                             std::end(kCollations)};
    for (const auto* entry : entries) {
      const std::string_view name = entry->name.GetIdentifierName();
      const bool icu =
        absl::StartsWith(entry->function.name.GetIdentifierName(), "collate_");
      collations.emplace_back(BuiltinCollation{
        .oid = static_cast<int32_t>(kFirstCollation + collations.size() -
                                    std::size(kCollations)),
        .name = name,
        .provider = icu ? 'i' : 'b',
        .locale = icu ? name : std::string_view{},
        .deterministic = entry->not_required_for_equality});
    }
    return collations;
  }();
  return kAll;
}

static_assert(absl::c_all_of(kTypes, [](const BuiltinType& type) {
  return type.type != 'd' ||
         absl::c_any_of(kTypes, [&](const BuiltinType& base) {
           return base.oid == type.basetype;
         });
}));

template<typename Key, typename Row>
const Row* Find(const absl::flat_hash_map<Key, const Row*>& index, Key key) {
  const auto it = index.find(key);
  return it == index.end() ? nullptr : it->second;
}

struct TypeIndex {
  absl::flat_hash_map<std::pair<duckdb::idx_t, std::string_view>,
                      const BuiltinType*>
    by_name;
  absl::flat_hash_map<duckdb::idx_t, const BuiltinType*> by_relid;
};

const TypeIndex& Types() {
  static const TypeIndex kIndex = [] {
    TypeIndex index;
    for (const auto& type : AllTypes()) {
      index.by_name.emplace(std::pair{type.nsp, type.name}, &type);
      if (type.relid != 0) {
        index.by_relid.emplace(type.relid, &type);
      }
    }
    return index;
  }();
  return kIndex;
}

}  // namespace

std::span<const BuiltinType> BuiltinTypes() { return AllTypes(); }

const BuiltinType* FindBuiltinType(int32_t oid) {
  return FindByOid(AllTypes(), oid);
}

const BuiltinType* FindBuiltinType(duckdb::idx_t nsp, std::string_view name) {
  return Find(Types().by_name, std::pair{nsp, name});
}

const BuiltinType* FindBuiltinRowType(duckdb::idx_t relid) {
  return Find(Types().by_relid, relid);
}

std::span<const BuiltinProc> BuiltinProcs() { return kProcs; }

std::span<const BuiltinCollation> BuiltinCollations() { return Collations(); }

const BuiltinCollation* FindBuiltinCollation(int64_t oid) {
  return FindByOid(Collations(), oid);
}

int32_t CollationOid(std::string_view collation) {
  if (collation == "default") {
    return static_cast<int32_t>(kDefaultCollation);
  }
  if (absl::EqualsIgnoreCase(collation, "c") ||
      absl::EqualsIgnoreCase(collation, "binary")) {
    return static_cast<int32_t>(kCCollation);
  }
  if (absl::EqualsIgnoreCase(collation, "posix")) {
    return static_cast<int32_t>(kPosixCollation);
  }
  const auto& collations = Collations();
  const auto it = absl::c_find_if(collations, [&](const BuiltinCollation& row) {
    return row.provider != 'd' && row.provider != 'c' &&
           absl::EqualsIgnoreCase(row.name, collation);
  });
  return it == collations.end() ? 0 : it->oid;
}

}  // namespace sdb::pg
