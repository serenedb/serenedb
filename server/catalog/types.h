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

enum class TableType : uint8_t {
  Unknown = 0,
  RocksDB = 1,
};

enum class FileFormat : uint8_t {
  None = 0,
  Text = 1,
  Parquet = 2,
  Dwrf = 3,
  Orc = 4,
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
customize::enum_name<sdb::WriteConflictPolicy>(
  sdb::WriteConflictPolicy value) noexcept {
  switch (value) {
    case sdb::WriteConflictPolicy::EmitError:
      return "emit_error";
    case sdb::WriteConflictPolicy::DoNothing:
      return "do_nothing";
    case sdb::WriteConflictPolicy::Replace:
      return "replace";
  }
  return default_tag;
}

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<sdb::TableType>(sdb::TableType value) noexcept {
  switch (value) {
    case sdb::TableType::RocksDB:
      return "rocksdb";
    default:
      return invalid_tag;
  }
}

}  // namespace magic_enum
