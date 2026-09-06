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

#include <absl/synchronization/mutex.h>

#include <duckdb/main/database.hpp>
#include <duckdb/storage/buffer/buffer_pool_reservation.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <duckdb/storage/shared_object_cache.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/analysis/tokenizer.hpp"

namespace irs::analysis {

// The reuse pool of constructed tokenizer instances for one dictionary: an
// entry of the database's SharedObjectCache keyed by the dictionary's id and
// retained by the catalog object, so it lives exactly as long as some catalog
// snapshot (or an outstanding lease) references the dictionary. Parked idle
// instances are owned by the pool; their memory is reserved in the buffer
// pool, and a per-pool shrink handle in the ObjectCache lets memory pressure
// dump a pool's idle instances wholesale.
class TokenizerPool final : public duckdb::ObjectCacheEntry {
 public:
  static constexpr std::string_view ObjectType() {
    return "sdb_tokenizer_pool";
  }

  std::string GetObjectType() final { return std::string{ObjectType()}; }

  static duckdb::shared_ptr<TokenizerPool> Get(duckdb::DatabaseInstance& db,
                                               std::string_view id);

  TokenizerPool(duckdb::DatabaseInstance& db, std::string id,
                size_t max_idle = MaxIdle());
  ~TokenizerPool() noexcept final;

  duckdb::optional_idx GetEstimatedCacheMemory() const final { return 0u; }

  Tokenizer::ptr Acquire();

  void Release(Tokenizer::ptr tokenizer) noexcept;

  size_t IdleCount() const;

 private:
  class ShrinkHandle;
  struct Core;

  static size_t MaxIdle() noexcept;

  duckdb::weak_ptr<duckdb::DatabaseInstance> _db;
  std::string _handle_key;
  duckdb::shared_ptr<Core> _core;
};

}  // namespace irs::analysis
