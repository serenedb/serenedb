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

using Oid = uint64_t;
using Xid = uint64_t;
using Regproc = Oid;
using Text = std::string_view;
using Bytea = irs::bytes_view;
using Name = std::string_view;
struct Empty {};
using PgNodeTree = Empty;
using Aclitem = Empty;
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

inline constexpr Oid kNone = 0;

};  // namespace sdb::pg
