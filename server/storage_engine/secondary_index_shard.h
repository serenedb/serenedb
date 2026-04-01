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

#include "storage_engine/index_shard.h"
#include "vpack/vpack.h"

namespace sdb {

struct SecondaryIndexShardOptions : public IndexShardOptions {
  struct Base {
    bool unique = false;
  } base;
};

class SecondaryIndexShard : public IndexShard {
 public:
  // Existing shard (loaded from catalog)
  SecondaryIndexShard(ObjectId id, ObjectId index_id,
                      SecondaryIndexShardOptions options)
    : IndexShard{id, index_id, IndexType::Secondary},
      _options{std::move(options)} {}

  // New shard
  SecondaryIndexShard(ObjectId index_id, SecondaryIndexShardOptions options)
    : IndexShard{index_id, IndexType::Secondary},
      _options{std::move(options)} {}

  void WriteInternal(vpack::Builder& builder) const override {
    vpack::WriteTuple(builder, _options.base);
  }

 private:
  SecondaryIndexShardOptions _options;
};

}  // namespace sdb
