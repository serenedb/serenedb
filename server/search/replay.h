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

#include <absl/functional/function_ref.h>

#include <cstdint>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/storage/table/table_log_storage.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/tracked.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "search/feeder.h"
#include "search/store.h"

namespace sdb::search {

class Replay {
 public:
  Replay(duckdb::AttachedDatabase& db, Store& store);

  irs::IndexWriter::Transaction& Slot() noexcept { return _slots.front(); }

  void FeedInParallel(
    absl::FunctionRef<Feeder::Feed(std::span<irs::IndexWriter::Transaction>)>
      make_feed);
  void Push(std::unique_ptr<duckdb::DataChunk> batch) {
    _feeder->Push(std::move(batch));
  }

  void RemoveKey(int64_t key) { _keys.push_back(key); }
  void RemovePositions(
    std::shared_ptr<const duckdb::data_t> entry,
    std::span<const duckdb::WALFrozenRowPositionGroup> groups);
  void MapPositions(std::shared_ptr<const duckdb::data_t> entry,
                    uint64_t source,
                    std::span<const duckdb::WALFrozenRowPositionMap> maps);
  void Truncate();
  void Adopt(std::span<const std::string> segments);

  void Finish();

 private:
  duckdb::AttachedDatabase& _db;
  Store& _store;
  std::vector<irs::IndexWriter::Transaction> _slots;
  std::optional<Feeder> _feeder;
  irs::IndexWriter::Transaction _head;
  irs::IndexWriter::Transaction _tail;
  irs::TrackedVector<int64_t> _keys;
  irs::DocRemoval _positions;
  irs::containers::FlatHashMap<uint64_t, std::vector<irs::DocMap>> _doc_maps;
};

}  // namespace sdb::search
