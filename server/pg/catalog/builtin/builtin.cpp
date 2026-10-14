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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/collate_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <vector>

#include "pg/catalog/tables/tables.h"
#include "pg/catalog/views/system_views.h"
#include "pg/types.h"

namespace sdb::pg {
namespace {

constexpr BuiltinProc kProcs[] = {
#include "pg/catalog/generated/builtin_procs.gen.inc"
};

constexpr BuiltinCollation kCollations[] = {
#include "pg/catalog/generated/builtin_collations.gen.inc"
};

constexpr auto kByOid = [](const auto& lhs, const auto& rhs) {
  return lhs.oid < rhs.oid;
};

static_assert(absl::c_is_sorted(kBuiltinTypes, kByOid) &&
              absl::c_is_sorted(kProcs, kByOid) &&
              absl::c_is_sorted(kCollations, kByOid));

constexpr void VisitRowTypes(auto&& visit) {
  for (const auto* sql : kGeneratedTables) {
    if (sql->row_type != 0) {
      visit(sql->oid, sql->schema, sql->name, sql->row_type,
            sql->row_array_type);
    }
  }
  for (const auto& view : kExternalViews) {
    if (view.row_type != 0) {
      visit(view.oid, view.schema, view.name, view.row_type,
            view.row_array_type);
    }
  }
}

constexpr auto kRowTypeNames = [] {
  size_t rows = 0;
  size_t chars = 0;
  VisitRowTypes([&](duckdb::idx_t, std::string_view, std::string_view name,
                    int32_t, int32_t) {
    ++rows;
    chars += name.size() + 1;
  });
  return std::pair{rows, chars};
}();

constexpr auto kArrayNames = [] {
  std::array<char, kRowTypeNames.second> names{};
  size_t at = 0;
  VisitRowTypes([&](duckdb::idx_t, std::string_view, std::string_view name,
                    int32_t, int32_t) {
    names[at++] = '_';
    for (const char c : name) {
      names[at++] = c;
    }
  });
  return names;
}();

constexpr auto kAllTypes = [] {
  std::array<BuiltinType, std::size(kBuiltinTypes) + 2 * kRowTypeNames.first>
    types{};
  auto out = absl::c_copy(kBuiltinTypes, types.begin());
  const auto* array_name = kArrayNames.data();
  VisitRowTypes([&](duckdb::idx_t relid, std::string_view schema,
                    std::string_view name, int32_t oid, int32_t array) {
    const auto nsp = FindSystemObject(kSystemNamespaces, schema)->oid;
    auto row = *FindByOid(kPgTypes, kRecord);
    row.oid = oid;
    row.name = name;
    row.nsp = nsp;
    row.type = 'c';
    row.category = 'C';
    row.array = array;
    row.relid = static_cast<int32_t>(relid);
    *out++ = row;
    auto element = *FindByOid(kPgTypes, kInt4Array);
    element.oid = array;
    element.name = std::string_view{array_name, name.size() + 1};
    array_name += name.size() + 1;
    element.nsp = nsp;
    element.elem = oid;
    element.align = 'd';
    *out++ = element;
  });
  absl::c_sort(types, kByOid);
  return types;
}();

static_assert([] {
  for (size_t i = 0; i < kAllTypes.size(); ++i) {
    const auto& type = kAllTypes[i];
    if ((type.nsp != kPgCatalogSchema && type.nsp != kPgInformationSchema) ||
        type.oid <= 0 || type.oid >= kBuiltinTypeOidLimit ||
        (i != 0 && kAllTypes[i - 1].oid == type.oid)) {
      return false;
    }
  }
  return true;
}());

constexpr auto kTypeIndex = OidIndex<uint16_t>(kAllTypes);

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

static_assert(absl::c_all_of(kBuiltinTypes, [](const BuiltinType& type) {
  return type.type != 'd' || FindByOid(kBuiltinTypes, type.basetype);
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
    for (const auto& type : kAllTypes) {
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

std::span<const BuiltinType> BuiltinTypes() { return kAllTypes; }

const BuiltinType* FindBuiltinType(int32_t oid) {
  return FindByOid(kAllTypes, kTypeIndex, oid);
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
