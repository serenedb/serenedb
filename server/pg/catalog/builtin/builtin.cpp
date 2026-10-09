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

#include <iresearch/utils/assert.hpp>

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

constexpr BuiltinType SdbArray(int32_t oid, std::string_view name,
                               int32_t elem) {
  auto row = *absl::c_find_if(
    kPgTypes, [](const BuiltinType& type) { return type.oid == kInt4Array; });
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

static_assert(absl::c_all_of(kTypes, [](const BuiltinType& type) {
  return type.type != 'd' ||
         absl::c_any_of(kTypes, [&](const BuiltinType& base) {
           return base.oid == type.basetype;
         });
}));

const auto* FindByOid(const auto& rows, int64_t oid) {
  const auto it = absl::c_lower_bound(
    rows, oid, [](const auto& row, int64_t key) { return row.oid < key; });
  return it != std::end(rows) && it->oid == oid ? &*it : nullptr;
}

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
    for (const auto& type : kTypes) {
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

std::span<const BuiltinType> BuiltinTypes() { return kTypes; }

const BuiltinType* FindBuiltinType(int32_t oid) {
  return FindByOid(kTypes, oid);
}

const BuiltinType* FindBuiltinType(duckdb::idx_t nsp, std::string_view name) {
  return Find(Types().by_name, std::pair{nsp, name});
}

const BuiltinType* FindBuiltinRowType(duckdb::idx_t relid) {
  return Find(Types().by_relid, relid);
}

std::span<const BuiltinProc> BuiltinProcs() { return kProcs; }

int32_t BuiltinProcOid(std::string_view name) {
  const auto it = absl::c_find_if(
    kProcs, [&](const BuiltinProc& proc) { return proc.name == name; });
  SDB_ASSERT(it != std::end(kProcs));
  return it->oid;
}

std::span<const BuiltinCollation> BuiltinCollations() { return kCollations; }

const BuiltinCollation* FindBuiltinCollation(int64_t oid) {
  return FindByOid(kCollations, oid);
}

}  // namespace sdb::pg
