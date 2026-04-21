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

#include "connector/duckdb_pk_range_scan.hpp"

#include <duckdb/common/types/data_chunk.hpp>

#include "basics/assert.h"
#include "connector/duckdb_key_builder.hpp"
#include "connector/duckdb_table_function.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {
namespace {

// Increments the last byte of `key` to produce an exclusive prefix upper bound.
// Clears `key` to empty (= unbounded) when all bytes are 0xff.
void MakeExclusiveUpperBound(std::string& key) {
  for (int i = static_cast<int>(key.size()) - 1; i >= 0; --i) {
    if (static_cast<uint8_t>(key[i]) < 0xff) {
      ++reinterpret_cast<uint8_t&>(key[i]);
      key.resize(static_cast<size_t>(i) + 1);
      return;
    }
  }
  key.clear();
}

// Iterates a single RocksDB column across N sorted, non-overlapping ranges
// using ONE underlying iterator. Uses a mutable iterate_upper_bound so RocksDB
// enforces per-range upper bounds natively -- hot-path Next() is just
// _iter->Next(). When a range is exhausted, updates _current_upper_bound
// in-place and seeks to the next range's lower bound.
class RocksDBPrefixRangeColumnIterator final : public rocksdb::Iterator {
 public:
  using IteratorFactory =
    std::function<std::unique_ptr<rocksdb::Iterator>(rocksdb::ReadOptions)>;

  RocksDBPrefixRangeColumnIterator(
    IteratorFactory factory, rocksdb::ReadOptions base_opts,
    std::span<const std::string> lower_keys,
    std::span<const std::string> upper_bound_keys,
    const rocksdb::Slice& col_upper_bound)
    : _lower_keys{lower_keys},
      _upper_bound_keys{upper_bound_keys},
      _column_upper_bound{col_upper_bound} {
    SDB_ASSERT(_lower_keys.size() == _upper_bound_keys.size());
    if (!_lower_keys.empty()) {
      _current_upper_bound = EffectiveUpperBound(0);
      base_opts.iterate_upper_bound = &_current_upper_bound;
    }
    _iter = factory(std::move(base_opts));
  }

  bool Valid() const final {
    if (!_seeked) {
      SeekToRange();
    }
    return _valid;
  }

  void Next() final {
    SDB_ASSERT(_valid, "RocksDBPrefixRangeColumnIterator::Next on invalid");
    _iter->Next();
    if (!_iter->Valid()) {
      _valid = false;
      AdvanceToNextRange();
    }
  }

  rocksdb::Slice key() const final { return _iter->key(); }
  rocksdb::Slice value() const final { return _iter->value(); }
  rocksdb::Status status() const final { return _iter->status(); }

  void SeekToFirst() final { SDB_ASSERT(false, "not supported"); }
  void SeekToLast() final { SDB_ASSERT(false, "not supported"); }
  void Seek(const rocksdb::Slice&) final { SDB_ASSERT(false, "not supported"); }
  void SeekForPrev(const rocksdb::Slice&) final {
    SDB_ASSERT(false, "not supported");
  }
  void Prev() final { SDB_ASSERT(false, "not supported"); }

 private:
  void SeekToRange() const {
    _seeked = true;
    _valid = false;
    _cur = 0;
    if (_lower_keys.empty()) {
      return;
    }
    _iter->Seek(_lower_keys[0]);
    AdvanceToNextRange();
  }

  void AdvanceToNextRange() const {
    while (_cur < _lower_keys.size()) {
      if (_iter->Valid()) {
        _valid = true;
        return;
      }
      ++_cur;
      if (_cur < _lower_keys.size()) {
        _current_upper_bound = EffectiveUpperBound(_cur);
        _iter->Seek(_lower_keys[_cur]);
      }
    }
  }

  [[nodiscard]] rocksdb::Slice EffectiveUpperBound(size_t i) const {
    const auto& ub = _upper_bound_keys[i];
    return ub.empty() ? _column_upper_bound : rocksdb::Slice{ub};
  }

  std::unique_ptr<rocksdb::Iterator> _iter;
  std::span<const std::string> _lower_keys;
  std::span<const std::string> _upper_bound_keys;
  const rocksdb::Slice& _column_upper_bound;
  mutable rocksdb::Slice _current_upper_bound;
  mutable size_t _cur = 0;
  mutable bool _valid = false;
  mutable bool _seeked = false;
};

}  // namespace

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> PKRangeScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<PKRangeScanGlobalState>();

  InitCommonState(*state, context, bind_data, input);
  auto column_keys = InitPKScanColumns(*state, bind_data);
  const auto num_scan = column_keys.size();

  auto& engine = GetServerEngine();
  auto* db = engine.db();
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(cf);

  auto& range_source = bind_data.scan_source->Cast<PkRangeScan>();
  const auto& ranges = range_source.ranges;

  // Build column-independent lower/upper suffix pairs for each non-empty range.
  struct RangeBounds {
    std::string lower;
    std::string upper;  // empty = no right bound; use column-level fallback
  };

  std::vector<RangeBounds> range_bounds;
  range_bounds.reserve(ranges.size());

  for (const auto& range : ranges) {
    if (range.IsEmpty()) {
      continue;
    }

    std::string lower;
    for (const auto& v : range.prefix) {
      AppendDuckDBValueToKey(lower, v);
    }
    const size_t prefix_len = lower.size();

    if (range.range_column.HasLeft()) {
      AppendDuckDBValueToKey(lower, range.range_column.LeftValue());
      if (!range.range_column.IsLeftInclusive()) {
        MakeExclusiveUpperBound(lower);
      }
    }

    std::string upper(lower, 0, prefix_len);

    if (range.range_column.HasRight()) {
      AppendDuckDBValueToKey(upper, range.range_column.RightValue());
      if (range.range_column.IsRightInclusive()) {
        MakeExclusiveUpperBound(upper);
      }
    } else if (prefix_len > 0) {
      MakeExclusiveUpperBound(upper);
    }
    // else: no right bound and no prefix -> upper stays empty -> column-level
    // fallback

    range_bounds.push_back({std::move(lower), std::move(upper)});
  }

  const size_t n_ranges = range_bounds.size();

  // Populate flat key arrays: [col*n_ranges + range_idx].
  // Reserve first so spans taken below remain stable.
  state->split_prefix_keys.reserve(num_scan * n_ranges);
  state->split_upper_bound_keys.reserve(num_scan * n_ranges);

  for (size_t col_i = 0; col_i < num_scan; ++col_i) {
    for (const auto& rb : range_bounds) {
      state->split_prefix_keys.push_back(column_keys[col_i] + rb.lower);
      state->split_upper_bound_keys.push_back(
        rb.upper.empty() ? std::string{} : column_keys[col_i] + rb.upper);
    }
  }

  // Create one RocksDBPrefixRangeColumnIterator per column.
  rocksdb::ReadOptions base_opts;
  base_opts.snapshot = state->snapshot;
  base_opts.async_io = false;
  base_opts.adaptive_readahead = true;
  base_opts.auto_prefix_mode = true;

  for (size_t col_i = 0; col_i < num_scan; ++col_i) {
    const size_t off = col_i * n_ranges;
    std::span<const std::string> lower_span{
      state->split_prefix_keys.data() + off, n_ranges};
    std::span<const std::string> upper_span{
      state->split_upper_bound_keys.data() + off, n_ranges};

    auto factory =
      [&, txn = state->txn,
       cf](rocksdb::ReadOptions opts) -> std::unique_ptr<rocksdb::Iterator> {
      return std::unique_ptr<rocksdb::Iterator>(
        txn ? txn->GetIterator(opts, cf) : db->NewIterator(opts, cf));
    };

    state->iterators.push_back(
      std::make_unique<RocksDBPrefixRangeColumnIterator>(
        std::move(factory), base_opts, lower_span, upper_span,
        state->upper_bound_slices[col_i]));
  }

  return state;
}

void PKRangeScanFunction(duckdb::ClientContext& /*context*/,
                         duckdb::TableFunctionInput& data,
                         duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<PKRangeScanGlobalState>();
  PKScanFunctionImpl(gstate, gstate.iterators, output);
}

}  // namespace sdb::connector
