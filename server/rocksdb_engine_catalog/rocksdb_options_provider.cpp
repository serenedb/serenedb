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

#include "rocksdb_engine_catalog/rocksdb_options_provider.h"

#include <rocksdb/slice_transform.h>

#include "catalog/table_options.h"
#include "rocksdb_engine_catalog/rocksdb_prefix_extractor.h"

namespace sdb {

RocksDBOptionsProvider::RocksDBOptionsProvider()
  : _vpack_cmp(std::make_unique<RocksDBVPackComparator>()) {}

const rocksdb::Options& RocksDBOptionsProvider::getOptions() const {
  if (!_options) {
    _options = doGetOptions();
  }
  return *_options;
}

const rocksdb::BlockBasedTableOptions& RocksDBOptionsProvider::getTableOptions()
  const {
  if (!_table_options) {
    _table_options = doGetTableOptions();
  }
  return *_table_options;
}

rocksdb::ColumnFamilyOptions RocksDBOptionsProvider::getColumnFamilyOptions(
  RocksDBColumnFamilyManager::Family family) const {
  rocksdb::ColumnFamilyOptions result(getOptions());

  auto make_column_optimized_for_get = [&] {
    result.prefix_extractor.reset(
      rocksdb::NewFixedPrefixTransform(RocksDBKey::objectIdSize()));

    rocksdb::BlockBasedTableOptions table_options(getTableOptions());
    table_options.data_block_index_type =
      rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
    result.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(table_options));
  };

  // TODO(mbkkt) current column families are wrong
  // Instead we want to have such column families:
  // clang-format off
  // | name      | key             | value      | prefix        | filter        | index    | data blocks |
  // | Documents | objID, pk       | rev, data  | objID         | hits + key    | binary*  | hash        |
  // | Unique    | objID, data     | pk         | objID         | key           | binary*  | hash        |
  // | Sorted    | objID, data, pk |            | objID         | hits + prefix | firstKey | binary      |
  // | Lookup    | objID, data, pk |            | objID, data   | prefix        | firstKey | binary      |
  // clang-format on
  // * -- maybe hash if shouldn't be sorted
  // Documents and PrimaryIndex  => Documents
  // PrimaryIndex and VPackIndex => Unique
  // VPackIndex                  => Sorted
  // EdgeIndex                   => Lookup
  switch (family) {
    case sdb::RocksDBColumnFamilyManager::Family::Data: {
      result.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(
        RocksDBKey::objectIdSize() + sizeof(catalog::Column::Id)));

      auto table_options = getTableOptions();
      result.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options));
    } break;
    case RocksDBColumnFamilyManager::Family::Definitions:
    case RocksDBColumnFamilyManager::Family::Invalid:
      break;
    case RocksDBColumnFamilyManager::Family::Documents: {
      result.optimize_filters_for_hits = true;
      make_column_optimized_for_get();
    } break;
    case RocksDBColumnFamilyManager::Family::PrimaryIndex: {
      make_column_optimized_for_get();
    } break;
    case RocksDBColumnFamilyManager::Family::EdgeIndex: {
      // we don't expect a lot of source vertexes with 0 outbound edges
      result.optimize_filters_for_hits = true;
      result.prefix_extractor = std::make_shared<RocksDBPrefixExtractor>();

      rocksdb::BlockBasedTableOptions table_options(getTableOptions());
      // there is not a lot of sense in bloom filter for each edge
      // because it can be used only to remove single edge
      table_options.whole_key_filtering = false;
      // kBinarySearchWithFirstKey + kNoShortening best for short scans
      table_options.index_type =
        rocksdb::BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
      table_options.index_shortening =
        rocksdb::BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
      result.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options));
    } break;
    case RocksDBColumnFamilyManager::Family::VPackIndex: {
      // we expect that index exist
      result.optimize_filters_for_hits = true;
      result.prefix_extractor.reset(
        rocksdb::NewFixedPrefixTransform(RocksDBKey::objectIdSize()));
      // vpack based index variants with custom comparator
      // TODO(mbkkt) in general it's unnecessary, we should write vpack value in
      // another format, like icu::Collator::getSortKey
      result.comparator = _vpack_cmp.get();
      rocksdb::BlockBasedTableOptions table_options(getTableOptions());
      // there is not any sense in bloom filter for each value
      // because we never makes Get or full key Seek
      table_options.whole_key_filtering = false;
      result.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options));
    } break;
  }

  // set TTL for .sst file compaction
  result.ttl = periodicCompactionTtl();

  return result;
}
}  // namespace sdb
