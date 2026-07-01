////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#pragma once

#include <boost/pfr.hpp>
#include <boost/pfr/core_name.hpp>
#include <initializer_list>
#include <iresearch/utils/string.hpp>
#include <span>
#include <type_traits>

namespace sdb::pg {

#define SDB_PG_OID_TYPE(name)                                  \
  struct name {                                                \
    constexpr name(uint64_t v = 0) noexcept : v{v} {}          \
    constexpr operator uint64_t() const noexcept { return v; } \
    uint64_t v;                                                \
  }
SDB_PG_OID_TYPE(Oid);
SDB_PG_OID_TYPE(Xid);
SDB_PG_OID_TYPE(Regproc);
SDB_PG_OID_TYPE(Regtype);
SDB_PG_OID_TYPE(Regclass);
SDB_PG_OID_TYPE(Cid);
SDB_PG_OID_TYPE(Xid8);
SDB_PG_OID_TYPE(Tid);
#undef SDB_PG_OID_TYPE

using Text = std::string_view;
using Bytea = irs::bytes_view;
struct Name {
  constexpr Name() = default;
  constexpr Name(const char* v) noexcept : v{v} {}
  constexpr Name(std::string_view v) noexcept : v{v} {}
  Name(const std::string& v) noexcept : v{v} {}
  constexpr operator std::string_view() const { return v; }
  std::string_view v;
};

struct Empty {};
using PgNodeTree = Empty;
struct Aclitem {};
using Anyarray = Empty;
using Timestamptz = Empty;
using PgNdDistinct = Empty;
using PgDependencies = Empty;
using PgMcvList = Empty;
using PgLsn = Empty;

using CharacterData = std::string_view;
using YesOrNo = bool;
using CardinalNumber = int64_t;

template<typename T>
using Array = std::span<const T>;

template<typename T>
using Vector = Array<T>;

template<typename T>
struct IsArray : std::false_type {};

template<typename T>
struct IsArray<Array<T>> : std::true_type {};

constexpr uint64_t MaskFromNulls(std::span<const size_t> indicies) {
  uint64_t mask = 0;
  for (size_t index : indicies) {
    mask |= (uint64_t{1} << index);
  }
  return mask;
}

constexpr uint64_t MaskFromNonNulls(std::span<const size_t> indicies) {
  return ~MaskFromNulls(indicies);
}

// Synthetic OID for the backing index of a primary key constraint.
// PG allocates a separate pg_class entry for every PK index; SereneDB does
// not, so derive a deterministic distinct OID from the table's OID using the
// top bit. Real catalog OIDs never set this bit.
constexpr uint64_t kSyntheticPkIndexBit = uint64_t{1} << 62;
constexpr uint64_t PkIndexOid(uint64_t table_oid) {
  return table_oid | kSyntheticPkIndexBit;
}

// Synthetic OID for the backing index of a UNIQUE constraint. PG allocates a
// pg_class entry for each UNIQUE constraint's index; SereneDB derives a
// deterministic distinct OID from the table OID and the constraint's ordinal.
constexpr uint64_t kSyntheticUniqueIndexBit = uint64_t{1} << 59;
constexpr uint64_t UniqueIndexOid(uint64_t table_oid, uint64_t unique_ordinal) {
  return (table_oid * 256 + unique_ordinal) | kSyntheticUniqueIndexBit;
}

template<typename ClassType, typename MemberType>
constexpr size_t GetIndex(MemberType ClassType::* member_ptr) {
  size_t index = -1;
  size_t i = 0;
  ClassType object{};
  const auto& target = object.*member_ptr;
  boost::pfr::for_each_field(object, [&](const auto& field) {
    if constexpr (std::is_same_v<decltype(target), decltype(field)>) {
      if (&target == &field) {
        index = i;
      }
    }
    ++i;
  });
  return index;
}

};  // namespace sdb::pg
