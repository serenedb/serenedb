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

#include "data_source.hpp"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>
#include <velox/vector/ComplexVector.h>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>

#include "basics/assert.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "connector/primary_key.hpp"
#include "key_utils.hpp"
#include "parquet_materializer.hpp"
#include "rocksdb_column_decoder.hpp"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "text_materializer.hpp"

namespace sdb::connector {
namespace {

// TODO(mkornaukhov) split this file, too many things are here

constexpr uint64_t kInitialVectorSize = 1;  // arbitrary value

// Dispatch once per column: Kind is resolved by the caller via
// VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH, so the MultiGet callback is fully typed.
template<typename Ctx, typename Source>
void FillColumnFromMultiGet(Ctx& ctx, Source& source,
                            std::span<const rocksdb::Slice> slices,
                            RocksDBColumnDecoder& decoder, size_t& collected) {
  ctx.MultiGet(source, slices, [&](const auto&, auto& val, const auto& st) {
    SDB_ENSURE(st.ok(), rocksutils::ConvertStatus(st));
    decoder.Add(collected++, val.ToStringView());
  });
}

// Advances the last byte of `key` to make it an exclusive upper bound that
// covers all keys sharing `key` as a prefix. Clears `key` to empty if all
// bytes are 0xff (meaning no upper bound; caller interprets empty as
// unbounded).
void MakeExclusiveUpperBound(std::string& key) {
  for (int i = static_cast<int>(key.size()) - 1; i >= 0; --i) {
    if (static_cast<unsigned char>(key[i]) < 0xff) {
      ++reinterpret_cast<unsigned char&>(key[i]);
      key.resize(static_cast<size_t>(i) + 1);
      return;
    }
  }
  key.clear();  // all 0xff -- no upper bound
}

}  // namespace

void PointLookupPKColumnBuilder::Init(const velox::TypePtr& type,
                                      size_t capacity,
                                      velox::memory::MemoryPool& pool) {
  _decoder = MakeRocksDBColumnDecoder(type, capacity, pool);
  _present_rows.reset(capacity);
}

void PointLookupPKColumnBuilder::Fill(size_t batch_idx, size_t found_idx,
                                      const rocksdb::PinnableSlice& val) {
  _present_rows.set(batch_idx);
  _decoder->Add(found_idx, val.ToStringView());
}

velox::VectorPtr PointLookupPKColumnBuilder::Finish(size_t found_count) {
  SDB_ASSERT(_present_rows.count() == found_count);
  return _decoder->Finish(found_count);
}

template<typename Source>
RocksDBPerColumnIteratorDataSource<Source>::RocksDBPerColumnIteratorDataSource(
  velox::memory::MemoryPool& memory_pool, Source& source,
  rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr read_type,
  std::vector<catalog::Column::Id> column_oids,
  catalog::Column::Id effective_column_id, ObjectId object_key,
  size_t output_column_count, const rocksdb::Snapshot* snapshot,
  velox::core::TypedExprPtr remaining_filter,
  velox::core::ExpressionEvaluator* evaluator)
  : RocksDBBaseDataSource{memory_pool,
                          cf,
                          std::move(read_type),
                          object_key,
                          std::move(column_oids),
                          output_column_count,
                          std::move(remaining_filter),
                          evaluator},
    _source{source},
    _snapshot{snapshot},
    _effective_column_id(std::move(effective_column_id)) {
  SDB_ASSERT(_read_type, "RocksDBDataSource: row type is null");
  SDB_ASSERT(_object_key.isSet(), "RocksDBDataSource: object key is empty");
  SDB_ASSERT(!_column_ids.empty(),
             "RocksDBDataSource: at least one column must be requested");
  SDB_ASSERT(
    _read_type->size() == 0 || _read_type->size() == _column_ids.size(),
    "RocksDBDataSource: number of columns does not match row type");

  std::string key = key_utils::PrepareTableKey(_object_key);

  const auto num_columns = _column_ids.size();

  _column_keys.reserve(num_columns);
  _upper_bound_keys_data.reserve(key_utils::kKeyPrefixSize * num_columns);
  _upper_bound_slices.reserve(num_columns);

  for (const auto column_id : _column_ids) {
    auto read_column_id = column_id;
    if (column_id == catalog::Column::kGeneratedPKId) {
      SDB_ASSERT(_effective_column_id != catalog::Column::kGeneratedPKId,
                 "RocksDBDataSource: generated PK column is not effective");
      read_column_id = _effective_column_id;
    }

    SDB_ASSERT(read_column_id !=
               std::numeric_limits<catalog::Column::Id>::max());

    basics::StrResize(key, key_utils::kTablePrefixSize);

    _upper_bound_keys_data.append(key);
    key_utils::AppendColumnKey(_upper_bound_keys_data, read_column_id + 1);

    key_utils::AppendColumnKey(key, read_column_id);
    _column_keys.push_back(key);
  }

  for (size_t i = 0; i < num_columns; ++i) {
    _upper_bound_slices.emplace_back(
      _upper_bound_keys_data.data() + i * key_utils::kKeyPrefixSize,
      key_utils::kKeyPrefixSize);
  }
}

template<typename Source>
void RocksDBFullScanDataSource<Source>::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  SDB_ENSURE(split, ERROR_INTERNAL, "RocksDBDataSource: split is null");
  if (this->_current_split) {
    SDB_THROW(ERROR_INTERNAL,
              "RocksDBDataSource: a split is already being processed");
  }
  this->_current_split = std::move(split);
  if constexpr (std::is_same_v<Source, rocksdb::Transaction>) {
    InitIterators([&](const rocksdb::ReadOptions& options) {
      return std::unique_ptr<rocksdb::Iterator>(
        this->_source.GetIterator(options, &this->_cf));
    });
  } else {
    InitIterators([&](const rocksdb::ReadOptions& options) {
      return std::unique_ptr<rocksdb::Iterator>(
        this->_source.NewIterator(options, &this->_cf));
    });
  }
}

template<typename Source>
template<typename CreateFn>
void RocksDBFullScanDataSource<Source>::InitIterators(CreateFn&& create_iter) {
  static_assert(std::invocable<CreateFn, const rocksdb::ReadOptions&>);
  // Creating iterator API expects options by const reference, but all
  // implementations copies this argument, so it should be safe.
  rocksdb::ReadOptions options;
  options.snapshot = this->_snapshot;
  options.async_io = false;
  options.adaptive_readahead = true;
  options.auto_prefix_mode = true;

  this->_iterators.clear();
  this->_iterators.reserve(this->_column_keys.size());
  for (size_t i = 0; i < this->_column_keys.size(); ++i) {
    options.iterate_upper_bound = &this->_upper_bound_slices[i];
    auto it = create_iter(options);
    it->Seek(this->_column_keys[i]);
    this->_iterators.push_back(std::move(it));
  }
}

template<typename Source>
std::optional<velox::RowVectorPtr>
RocksDBPerColumnIteratorDataSource<Source>::next(
  uint64_t size, velox::ContinueFuture& future) {
  SDB_ASSERT(size);
  if (!_current_split) {
    return nullptr;
  }

  const auto num_columns = _read_type->size();
  std::vector<velox::VectorPtr> columns;
  columns.reserve(num_columns);

  if (num_columns > 0) {
    columns.reserve(num_columns);
    for (size_t column_idx = 0; column_idx < num_columns; ++column_idx) {
      columns.emplace_back(ReadColumn(column_idx, size));

      // All rows are read
      if (columns[column_idx]->size() == 0) {
        SDB_ASSERT(column_idx == 0,
                   "RocksDBDataSource: inconsistent number of columns");
        _current_split.reset();
        return nullptr;
      }
    }
    SDB_ASSERT(absl::c_all_of(columns,
                              [&](const velox::VectorPtr& vec) {
                                return vec->size() == columns.front()->size();
                              }),
               "RocksDBDataSource: inconsistent number of rows among columns");
    auto batch = ApplyRemainingFilter(std::make_shared<velox::RowVector>(
      &_memory_pool, _read_type, nullptr, columns.front()->size(),
      std::move(columns)));

    _produced += batch->size();
    return batch;
  }
  SDB_ASSERT(_column_ids.size() == 1);
  SDB_ASSERT(!_remaining_expr_set,
             "RocksDBFullScanDataSource: count(*) with filter should set up "
             "filter columns in _read_type");
  const auto read = IterateColumn(
    *_iterators[0], size, [](uint64_t, std::string_view, std::string_view) {});

  // All rows are read
  if (read == 0) {
    _current_split.reset();
    return nullptr;
  }
  _produced += read;
  return velox::BaseVector::create<velox::RowVector>(_read_type, read,
                                                     &_memory_pool);
}

void RocksDBBaseDataSource::addDynamicFilter(
  velox::column_index_t, const std::shared_ptr<velox::common::Filter>&) {
  VELOX_UNSUPPORTED();
}

uint64_t RocksDBBaseDataSource::getCompletedBytes() {
  // TODO: implement completed bytes tracking
  return 0;
}

uint64_t RocksDBBaseDataSource::getCompletedRows() { return _produced; }

std::unordered_map<std::string, velox::RuntimeMetric>
RocksDBBaseDataSource::getRuntimeStats() {
  // TODO: implement runtime stats reporting
  return {};
}

void RocksDBBaseDataSource::cancel() {
  // TODO: implement cancellation logic
}

template<typename Source>
velox::VectorPtr RocksDBPerColumnIteratorDataSource<Source>::ReadColumn(
  velox::column_index_t col_idx, uint64_t max_size) {
  auto& it = *_iterators[col_idx];
  auto column_id = _column_ids[col_idx];

  // special case possible only here
  if (column_id == catalog::Column::kGeneratedPKId) {
    return ReadColumnFromKey(it, max_size);
  }

  const auto& type = _read_type->childAt(col_idx);
  auto decoder = MakeRocksDBColumnDecoder(
    type, static_cast<velox::vector_size_t>(max_size), _memory_pool);
  const auto vector_size = IterateColumn(
    it, max_size, [&](uint64_t idx, std::string_view, std::string_view value) {
      decoder->Add(static_cast<velox::vector_size_t>(idx), value);
    });
  return decoder->Finish(static_cast<velox::vector_size_t>(vector_size));
}

template<typename Source>
velox::VectorPtr RocksDBPerColumnIteratorDataSource<Source>::ReadColumnFromKey(
  rocksdb::Iterator& it, uint64_t max_size) {
  // For now only generated PK column is supported
  auto result = velox::BaseVector::create<velox::FlatVector<int64_t>>(
    velox::BIGINT(), kInitialVectorSize, &_memory_pool);
  result->resize(max_size, true);

  const auto vector_size = IterateColumn(
    it, max_size,
    [&](uint64_t value_idx, std::string_view key, std::string_view value) {
      auto val = primary_key::ReadSigned<int64_t>(
        std::string_view{key.begin() + key_utils::kKeyPrefixSize, key.end()});
      result->set(value_idx, val);
    });
  if (vector_size != result->size()) {
    SDB_ASSERT(
      vector_size < result->size(),
      "RocksDBDataSource: inconsistent vector size of fake primary column");
    result->resize(vector_size, true);
  }
  return result;
}

template<typename Source>
template<std::invocable<uint64_t, std::string_view, std::string_view> Callback>
uint64_t RocksDBPerColumnIteratorDataSource<Source>::IterateColumn(
  rocksdb::Iterator& it, uint64_t max_size, const Callback& func) {
  uint64_t vector_size = 0;

  while (it.Valid() && max_size > vector_size) {
    func(vector_size, it.key().ToStringView(), it.value().ToStringView());
    ++vector_size;
    it.Next();
  }

  rocksutils::CheckIteratorStatus(it);

  return vector_size;
}

template<typename Policy>
void RocksDBPointLookupDataSource<Policy>::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  SDB_ENSURE(split, ERROR_INTERNAL, "RocksDBDataSource: split is null");
  if (_current_split) {
    SDB_THROW(ERROR_INTERNAL,
              "RocksDBDataSource: a split is already being processed");
  }
  _current_split = std::move(split);
  _values_offset = 0;
}

velox::RowVectorPtr RocksDBBaseDataSource::ApplyRemainingFilter(
  velox::RowVectorPtr batch) {
  if (!_remaining_expr_set) {
    SDB_ASSERT(batch->childrenSize() == _output_column_count);
    return batch;
  }

  const velox::vector_size_t num_rows = batch->size();
  velox::SelectivityVector rows(num_rows);
  velox::VectorPtr result;
  _evaluator->evaluate(_remaining_expr_set.get(), rows, *batch, result);

  velox::DecodedVector decoded(*result, rows);
  velox::vector_size_t passing = 0;
  auto indices = velox::allocateIndices(num_rows, &_memory_pool);
  auto* raw_indices = indices->template asMutable<velox::vector_size_t>();
  for (velox::vector_size_t i = 0; i < num_rows; ++i) {
    if (!decoded.isNullAt(i) && decoded.valueAt<bool>(i)) {
      raw_indices[passing++] = i;
    }
  }

  auto out_type = velox::ROW(
    std::vector<std::string>(_read_type->names().begin(),
                             _read_type->names().begin() +
                               static_cast<ptrdiff_t>(_output_column_count)),
    std::vector<velox::TypePtr>(
      _read_type->children().begin(),
      _read_type->children().begin() +
        static_cast<ptrdiff_t>(_output_column_count)));

  if (passing == 0) {
    return velox::BaseVector::create<velox::RowVector>(out_type, 0,
                                                       &_memory_pool);
  }

  if (passing != batch->size()) {
    std::vector<velox::VectorPtr> filtered;
    filtered.reserve(_output_column_count);
    for (size_t i = 0; i < _output_column_count; ++i) {
      filtered.push_back(velox::BaseVector::wrapInDictionary(
        nullptr, indices, passing, batch->childAt(static_cast<int32_t>(i))));
    }

    batch =
      std::make_shared<velox::RowVector>(&_memory_pool, std::move(out_type),
                                         nullptr, passing, std::move(filtered));
  }

  if (batch->childrenSize() > _output_column_count) {
    auto output_type =
      velox::ROW(std::vector<std::string>(
                   _read_type->names().begin(),
                   _read_type->names().begin() + _output_column_count),
                 std::vector<velox::TypePtr>(
                   _read_type->children().begin(),
                   _read_type->children().begin() + _output_column_count));
    std::vector<velox::VectorPtr> output_children(
      batch->children().begin(),
      batch->children().begin() + _output_column_count);
    batch = std::make_shared<velox::RowVector>(
      &_memory_pool, std::move(output_type), batch->nulls(), batch->size(),
      std::move(output_children));
  }
  return batch;
}

template<typename Policy>
std::optional<velox::RowVectorPtr> RocksDBPointLookupDataSource<Policy>::next(
  uint64_t size, velox::ContinueFuture& future) {
  SDB_ASSERT(size);
  if (!_current_split) {
    return nullptr;
  }

  SDB_ASSERT(!_values.empty(),
             "Case of empty filters should be processed in connector");

  const auto num_columns = _read_type->size();
  const size_t batch_size =
    std::min(static_cast<size_t>(size), _values.size() - _values_offset);
  SDB_ASSERT(batch_size > 0);

  // Step 1: fetch column 0 in sorted order to fill _present_rows_batch and,
  // when num_columns >= 1, also populate the column 0 vector directly.
  const size_t least_column_index = _sorted_col_indices[0];
  const auto least_column_id = _column_ids[least_column_index];

  if (num_columns == 0) {
    // count(*) path: only need presence count, no column data or mask needed.
    SDB_ASSERT(_column_ids.size() == 1);
    SDB_ASSERT(!_remaining_expr_set);
    size_t found_count = 0;
    const auto slices = _key_builder.BuildKeys(
      least_column_id, std::span{_values}.subspan(_values_offset, batch_size));
    _ctx.MultiGet(
      _source, slices, [&](const auto&, const auto&, const auto& st) {
        if (st.ok()) {
          ++found_count;
        } else {
          SDB_ENSURE(st.IsNotFound(), rocksutils::ConvertStatus(st));
        }
      });
    _values_offset += batch_size;
    if (_values_offset >= _values.size()) {
      _current_split.reset();
    }
    _produced += found_count;
    return velox::BaseVector::create<velox::RowVector>(_read_type, found_count,
                                                       &_memory_pool);
  }

  _collector.Init(_read_type->childAt(least_column_index), batch_size,
                  _memory_pool);

  size_t found_count = 0;
  {
    const auto slices = _key_builder.BuildKeys(
      least_column_id, std::span{_values}.subspan(_values_offset, batch_size));
    size_t batch_idx = 0;
    _ctx.MultiGet(_source, slices, [&](const auto&, auto& val, const auto& st) {
      if (st.ok()) {
        _collector.Fill(batch_idx, found_count++, val);
      } else {
        SDB_ENSURE(st.IsNotFound(), rocksutils::ConvertStatus(st));
      }
      ++batch_idx;
    });
  }

  irs::Finally _ = [&] noexcept {
    _values_offset += batch_size;
    if (_values_offset >= _values.size()) {
      _current_split.reset();
    }
  };

  if (found_count == 0) {
    return nullptr;
  }

  // Materializer path: PKs collected, materialize and return.
  if constexpr (kIsSecondaryIndex) {
    _produced += found_count;
    return _collector.Finish(found_count);
  } else {
    // Column path: col0 done via collector, fetch remaining columns.
    auto least_column_vec = _collector.Finish(found_count);

    // Step 2: patch key prefixes for present rows and fetch remaining columns.
    // Type dispatch happens once per column via FillColumnFromMultiGet.
    std::vector<velox::VectorPtr> columns(num_columns);
    columns[least_column_index] = std::move(least_column_vec);
    for (size_t col_idx : std::span{_sorted_col_indices.begin() + 1,
                                    _sorted_col_indices.end()}) {
      const auto col_id = _column_ids[col_idx];
      const auto& type = _read_type->childAt(col_idx);

      auto decoder = MakeRocksDBColumnDecoder(type, found_count, _memory_pool);

      const auto slices =
        _key_builder.BuildPresentKeys(col_id, _collector.PresentRows());
      size_t collected = 0;
      FillColumnFromMultiGet(_ctx, _source, slices, *decoder, collected);
      columns[col_idx] = decoder->Finish(found_count);
    }

    SDB_ASSERT(
      absl::c_all_of(
        columns, [&](const auto& col) { return col->size() == found_count; }),
      "RocksDBPointLookupDataSource: inconsistent columns");

    auto batch = ApplyRemainingFilter(std::make_shared<velox::RowVector>(
      &_memory_pool, _read_type, nullptr, found_count, std::move(columns)));

    _produced += batch->size();
    return batch;
  }
}

// Iterates a single RocksDB column across a sorted, non-overlapping list of
// ranges using ONE underlying iterator. Uses a mutable iterate_upper_bound so
// RocksDB enforces per-range upper bounds natively -- hot-path Next() is just
// _iter->Next() + Valid(). When a range is exhausted, updates _cur_upper_bound
// in-place and seeks to the next range's lower bound; RocksDB picks up the new
// bound on the next Valid() / Next() call.
class RocksDBPrefixRangeColumnIterator : public rocksdb::Iterator {
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
      // Tricky place, it's OK according to code,
      // but nothing in docs is guaranteed here.
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

  // Falls back to column-level bound when the range has no right boundary.
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

template<typename Source>
RocksDBPrefixRangeDataSource<Source>::RocksDBPrefixRangeDataSource(
  velox::memory::MemoryPool& memory_pool, Source& source,
  rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr read_type,
  std::vector<catalog::Column::Id> column_ids,
  catalog::Column::Id effective_column_id, ObjectId object_key,
  size_t output_column_count, const rocksdb::Snapshot* snapshot,
  std::vector<ResolvedRange> ranges, velox::RowTypePtr pk_type,
  velox::core::TypedExprPtr remaining_filter,
  velox::core::ExpressionEvaluator* evaluator)
  : RocksDBPerColumnIteratorDataSource<Source>{memory_pool,
                                               source,
                                               cf,
                                               std::move(read_type),
                                               std::move(column_ids),
                                               effective_column_id,
                                               object_key,
                                               output_column_count,
                                               snapshot,
                                               std::move(remaining_filter),
                                               evaluator},
    _pk_type{std::move(pk_type)} {
  SDB_ASSERT(absl::c_is_sorted(ranges), "ranges must be sorted");

  // Build per-range lower and upper keys (column-independent suffixes).
  // These encode the seek target and exclusive upper bound for each range.
  struct RangeBounds {
    std::string lower;
    std::string upper;  // empty = no right bound, use column-level fallback
  };

  std::vector<RangeBounds> range_bounds;
  for (const auto& range : ranges) {
    if (range.IsEmpty()) {
      continue;
    }
    const size_t prefix_size = range.prefix.size();

    std::string lower;
    for (size_t i = 0; i < prefix_size; ++i) {
      const auto col_type =
        velox::ROW({_pk_type->nameOf(i)}, {_pk_type->childAt(i)});
      primary_key::Create({range.prefix[i]}, *col_type, lower);
    }
    const size_t prefix_len = lower.size();

    if (range.range_column.HasLeft()) {
      const auto column_type = velox::ROW({_pk_type->nameOf(prefix_size)},
                                          {_pk_type->childAt(prefix_size)});
      primary_key::Create({range.range_column.LeftValue()}, *column_type,
                          lower);
      if (!range.range_column.IsLeftInclusive()) {
        MakeExclusiveUpperBound(lower);
      }
    }

    std::string upper;
    upper.assign(lower, 0, prefix_len);

    if (range.range_column.HasRight()) {
      const auto column_type = velox::ROW({_pk_type->nameOf(prefix_size)},
                                          {_pk_type->childAt(prefix_size)});
      primary_key::Create({range.range_column.RightValue()}, *column_type,
                          upper);
      if (range.range_column.IsRightInclusive()) {
        MakeExclusiveUpperBound(upper);
      }
    } else if (prefix_size > 0) {
      MakeExclusiveUpperBound(upper);
    }

    range_bounds.push_back({std::move(lower), std::move(upper)});
  }

  const size_t ranges_count = range_bounds.size();
  const size_t n_cols = this->_column_keys.size();
  _split_prefix_keys.reserve(n_cols * ranges_count);
  _split_upper_bound_keys.reserve(n_cols * ranges_count);
  for (size_t col_i = 0; col_i < n_cols; ++col_i) {
    for (const auto& range_bound : range_bounds) {
      _split_prefix_keys.push_back(this->_column_keys[col_i] +
                                   range_bound.lower);
      _split_upper_bound_keys.push_back(range_bound.upper.empty()
                                          ? std::string{}
                                          : this->_column_keys[col_i] +
                                              range_bound.upper);
    }
  }
}

template<typename Source>
void RocksDBPrefixRangeDataSource<Source>::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  SDB_ENSURE(split, ERROR_INTERNAL, "RocksDBDataSource: split is null");
  if (this->_current_split) {
    SDB_THROW(ERROR_INTERNAL,
              "RocksDBDataSource: a split is already being processed");
  }
  this->_current_split = std::move(split);
  if constexpr (std::is_same_v<Source, rocksdb::Transaction>) {
    InitIterators([&](const rocksdb::ReadOptions& options) {
      return std::unique_ptr<rocksdb::Iterator>(
        this->_source.GetIterator(options, &this->_cf));
    });
  } else {
    InitIterators([&](const rocksdb::ReadOptions& options) {
      return std::unique_ptr<rocksdb::Iterator>(
        this->_source.NewIterator(options, &this->_cf));
    });
  }
}

template<typename Source>
template<typename CreateFn>
void RocksDBPrefixRangeDataSource<Source>::InitIterators(
  CreateFn&& create_iter) {
  static_assert(std::invocable<CreateFn, const rocksdb::ReadOptions&>);
  SDB_ASSERT(!_pk_type->names().empty(),
             "RocksDBPrefixRangeDataSource: pk_type has no columns");

  rocksdb::ReadOptions base_opts;
  base_opts.snapshot = this->_snapshot;
  base_opts.async_io = false;
  base_opts.adaptive_readahead = true;
  base_opts.auto_prefix_mode = true;

  const size_t columns_count = this->_column_keys.size();
  const size_t ranges_count =
    columns_count > 0 ? _split_prefix_keys.size() / columns_count : 0;
  this->_iterators.clear();
  this->_iterators.reserve(columns_count);
  for (size_t column_index = 0; column_index < columns_count; ++column_index) {
    const size_t off = column_index * ranges_count;
    std::span<const std::string> lower_span{_split_prefix_keys.data() + off,
                                            ranges_count};
    std::span<const std::string> upper_bound_span{
      _split_upper_bound_keys.data() + off, ranges_count};
    this->_iterators.push_back(
      std::make_unique<RocksDBPrefixRangeColumnIterator>(
        create_iter, base_opts, lower_span, upper_bound_span,
        this->_upper_bound_slices[column_index]));
  }
}

template class RocksDBPerColumnIteratorDataSource<rocksdb::Transaction>;
template class RocksDBPerColumnIteratorDataSource<rocksdb::DB>;

template class RocksDBFullScanDataSource<rocksdb::Transaction>;
template class RocksDBFullScanDataSource<rocksdb::DB>;

template class RocksDBPrefixRangeDataSource<rocksdb::Transaction>;
template class RocksDBPrefixRangeDataSource<rocksdb::DB>;

template class RocksDBPointLookupDataSource<PKLookupPolicy<true>>;
template class RocksDBPointLookupDataSource<PKLookupPolicy<false>>;
template class RocksDBPointLookupDataSource<
  SKLookupPolicy<true, RocksDBMaterializer>>;
template class RocksDBPointLookupDataSource<
  SKLookupPolicy<false, RocksDBMaterializer>>;
template class RocksDBPointLookupDataSource<
  SKLookupPolicy<false, ParquetMaterializer>>;
template class RocksDBPointLookupDataSource<
  SKLookupPolicy<false, TextMaterializer>>;

}  // namespace sdb::connector
