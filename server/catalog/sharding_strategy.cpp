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

#include "sharding_strategy.h"

#include <vpack/builder.h>

#include "basics/containers/flat_hash_map.h"
#include "basics/exceptions.h"
#include "basics/static_strings.h"
#include "general_server/state.h"

namespace sdb {
namespace {

using FactoryFunction =
  std::unique_ptr<ShardingStrategy> (*)(const ShardingStrategyOptions& options);

const containers::FlatHashMap<std::string_view, FactoryFunction> kFactories{
  {
    ShardingStrategyNone::kName,
    [](const ShardingStrategyOptions&) -> std::unique_ptr<ShardingStrategy> {
      return std::make_unique<ShardingStrategyNone>();
    },
  },
};

}  // namespace

std::unique_ptr<ShardingStrategy> ShardingStrategy::make(
  std::string_view name, const ShardingStrategyOptions& options) {
  auto it = kFactories.find(name);

  if (it == kFactories.end()) {
    SDB_THROW(ERROR_BAD_PARAMETER, "unknown sharding type: ", name);
  }

  return it->second(options);
}

ShardingStrategyNone::ShardingStrategyNone() {
  if (ServerState::instance()->IsCoordinator()) {
    SDB_THROW(ERROR_BAD_PARAMETER, "sharding strategy ", kName,
              " cannot be used for sharded collections");
  }
}

ResultOr<ShardID> ShardingStrategyNone::getResponsibleShard(
  vpack::Slice slice, bool doc_complete, bool& uses_default_shard_keys,
  std::string_view key) {
  SDB_THROW(ERROR_INTERNAL, "unexpected invocation of ShardingStrategyNone");
}

}  // namespace sdb
