////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <magic_enum/magic_enum.hpp>

#include "basics/containers/flat_hash_map.h"
#include "basics/reboot_id.h"

namespace sdb {

using Tick = uint64_t;

enum class EdgeDirection : uint8_t {
  Any = 0,  // can only be used for searching
  In = 1,
  Out = 2,
};

enum class TableType : uint8_t {
  Unknown = 0,
  Document = 2,
  Edge = 3,
};

enum IndexType {
  kTypeUnknown = 0,
  kTypePrimaryIndex,
  kTypeEdgeIndex,
  kTypeTtlIndex,
  kTypeSecondaryIndex,
  kTypeNoAccessIndex,
  kTypeInvertedIndex,
};

enum class IndexSerialization : uint8_t {
  /// serialize index
  Basics = 0,
  /// serialize figures
  Figures = 2,
  /// serialize selectivity estimates
  Estimates = 4,
  /// serialize for persistence
  Internals = 8,
  /// serialize for inventory
  Inventory = 16,
};

enum class WriteConflictPolicy : uint8_t {
  EmitError,
  DoNothing,
  Replace,
};

using IndexEstMap = containers::FlatHashMap<std::string, double>;

struct PeerState {
  std::string server_id;
  RebootId reboot_id{0};

  friend bool operator==(const PeerState&, const PeerState&) noexcept = default;
};

}  // namespace sdb

namespace magic_enum {

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<sdb::EdgeDirection>(sdb::EdgeDirection value) noexcept {
  switch (value) {
    case sdb::EdgeDirection::Any:
      return "any";
    case sdb::EdgeDirection::Out:
      return "out";
    case sdb::EdgeDirection::In:
      return "in";
  }
  return default_tag;
}

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<sdb::TableType>(sdb::TableType value) noexcept {
  switch (value) {
    case sdb::TableType::Document:
      return "document";
    case sdb::TableType::Edge:
      return "edge";
    default:
      return invalid_tag;
  }
}

}  // namespace magic_enum
