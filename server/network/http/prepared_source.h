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

#pragma once

#include <cstdint>
#include <duckdb/common/error_data.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <exception>
#include <iresearch/utils/assert.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "network/http/handler.h"

namespace sdb::network {

class PreparedCache {
 public:
  PreparedEntry& Get(std::string_view sql, size_t capacity) {
    SDB_ASSERT(capacity != 0);
    ++_tick;
    Slot* victim = nullptr;
    for (auto& slot : _slots) {
      if (slot.entry.sql == sql) {
        slot.last_use = _tick;
        return slot.entry;
      }
      if (victim == nullptr || slot.last_use < victim->last_use) {
        victim = &slot;
      }
    }
    if (_slots.size() < capacity) {
      _slots.reserve(capacity);
      victim = &_slots.emplace_back();
    } else {
      victim->entry.statement.reset();
    }
    victim->entry.sql = sql;
    victim->last_use = _tick;
    return victim->entry;
  }

  size_t Size() const noexcept { return _slots.size(); }

 private:
  struct Slot {
    PreparedEntry entry;
    uint64_t last_use = 0;
  };

  std::vector<Slot> _slots;
  uint64_t _tick = 0;
};

inline std::optional<duckdb::ErrorData> EnsurePrepared(RequestContext& ctx,
                                                       PreparedEntry& entry,
                                                       const std::string& sql) {
  try {
    auto& connection = ctx.Connection();
    if (entry.statement == nullptr) {
      auto statement = connection.Prepare(sql);
      if (statement->HasError()) {
        return statement->GetErrorObject();
      }
      entry.statement = std::move(statement);
    }
  } catch (const std::exception& error) {
    return duckdb::ErrorData{error};
  }
  return std::nullopt;
}

}  // namespace sdb::network
