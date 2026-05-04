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

#include <vpack/slice.h>

#include "catalog/identifiers/object_id.h"
#include "catalog/identifiers/shard_id.h"

namespace sdb {

struct ShardingStrategyOptions {
  std::span<const std::string> shard_keys;
  ObjectId object_id;
};

struct ShardingStrategy {
  static std::unique_ptr<ShardingStrategy> make(
    std::string_view name, const ShardingStrategyOptions& options);

  virtual ~ShardingStrategy() = default;

  virtual std::string_view name() const = 0;

  // TODO(gnusi): remove
  virtual bool usesDefaultShardKeys() const noexcept = 0;

  // find the shard that is responsible for a document, which is given
  // as a VPackSlice.
  //
  // There are two modes, one assumes that the document is given as a
  // whole (`docComplete`==`true`), in this case, the non-existence of
  // values for some of the sharding attributes is silently ignored
  // and treated as if these values were `null`. In the second mode
  // (`docComplete`==false) leads to an error which is reported by
  // returning ERROR_SERVER_DATA_SOURCE_NOT_FOUND, which is the only
  // error code that can be returned.
  //
  // In either case, if the collection is found, the variable
  // shardID is set to the ID of the responsible shard and the flag
  // `usesDefaultShardingAttributes` is used set to `true` if and only if
  // `_key` is the one and only sharding attribute.
  virtual ResultOr<ShardID> getResponsibleShard(vpack::Slice slice,
                                                bool doc_complete,
                                                bool& uses_default_shard_keys,
                                                std::string_view key) = 0;

  ResultOr<ShardID> getResponsibleShard(vpack::Slice slice, bool doc_complete,
                                        std::string_view key) {
    [[maybe_unused]] bool unused;
    return getResponsibleShard(slice, doc_complete, unused, key);
  }
};

// TODO(gnusi): remove
class ShardingStrategyNone final : public ShardingStrategy {
 public:
  static constexpr std::string_view kName = "none";

  ShardingStrategyNone();

  std::string_view name() const final { return kName; }

  bool usesDefaultShardKeys() const noexcept final { return true; }

  ResultOr<ShardID> getResponsibleShard(vpack::Slice slice, bool doc_complete,
                                        bool& uses_default_shard_keys,
                                        std::string_view key) final;
};

}  // namespace sdb
