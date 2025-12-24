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

#include <velox/core/QueryCtx.h>

#include <memory>
#include <type_traits>
#include <utility>

#include "basics/fwd.h"
#include "pg/sql_collector.h"
#include "query/config.h"

namespace sdb::query {

template<typename CommandType>
class EnumType {
 public:
  using ValueType = std::underlying_type_t<CommandType>;

  template<typename... Commands>
  void Add(Commands... commands) {
    const auto mask = (... | std::to_underlying(commands));
    _commands |= mask;
  }

  template<typename... Commands>
  bool Has(Commands... commands) const {
    const auto mask = (... | std::to_underlying(commands));
    return (_commands & mask) == mask;
  }

  template<typename... Commands>
  bool HasAnyNot(Commands... commands) const {
    const auto mask = (... | std::to_underlying(commands));
    return _commands & ~mask;
  }

 private:
  ValueType _commands = 0;
};

enum class CommandType : uint64_t {
  None = 0,
  Query = 1 << 0,
  Explain = 1 << 1,
  Show = 1 << 2,
  External = 1 << 3,
};

enum class ExplainWith : uint64_t {
  None = 0,

  // plans
  Logical = 1 << 1,
  InitialQueryGraph = 1 << 2,
  FinalQueryGraph = 1 << 3,
  Physical = 1 << 4,
  Execution = 1 << 5,

  // parameters
  Registers = 1 << 6,
  Oneline = 1 << 7,
  Cost = 1 << 8,
  Stats = 1 << 9,
};

struct QueryContext {
  using OptionValue = std::variant<bool, int, std::string, double>;

  explicit QueryContext(std::shared_ptr<velox::core::QueryCtx> velox_query_ctx,
                        const pg::Objects& objects)
    : velox_query_ctx{std::move(velox_query_ctx)},
      query_memory_pool{
        this->velox_query_ctx->pool()->addLeafChild("query_memory_pool")},
      objects{objects} {}

  std::shared_ptr<velox::core::QueryCtx> velox_query_ctx;
  // To allocate memory for VALUES clause processing.
  std::shared_ptr<velox::memory::MemoryPool> query_memory_pool;
  const pg::Objects& objects;

  EnumType<CommandType> command_type;
  EnumType<ExplainWith> explain_params;
};

using QueryContextPtr = std::shared_ptr<QueryContext>;
using ConstQueryContextPtr = std::shared_ptr<const QueryContext>;

}  // namespace sdb::query
