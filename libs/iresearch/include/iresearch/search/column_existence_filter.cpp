////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "column_existence_filter.hpp"

#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/validity_mask.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/storage/storage_info.hpp>

#include "basics/bit_utils.hpp"
#include "basics/memory.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

static_assert(sizeof(duckdb::validity_t) == sizeof(uint64_t));

class ColumnExistenceIterator : public DocIterator {
 public:
  ColumnExistenceIterator(const ColumnReader& reader,
                          const ColReader& col_reader,
                          CostAttr::Type cost) noexcept
    : _reader{&reader},
      _ctx{col_reader},
      _scan_data{reader.NullsInData()},
      _null_reader{_scan_data ? &reader : reader.Validity()},
      _scan{_null_reader->InitScan(_ctx)},
      _batch{
        _scan_data ? reader.Type()
                   : duckdb::LogicalType{duckdb::LogicalTypeId::VALIDITY},
        _scan_data ? duckdb::idx_t{STANDARD_VECTOR_SIZE} : duckdb::idx_t{0}} {
    SDB_ASSERT(cost != 0);
    _batch.BufferMutable().GetValidityMask().Initialize(STANDARD_VECTOR_SIZE);
    std::get<CostAttr>(_attrs) = CostAttr{cost};
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return irs::GetMutable(_attrs, id);
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& /*ctx*/) final {
    return ScoreFunction::Default();
  }

  doc_id_t advance() noexcept final {
    while (true) {
      if (_word != 0) {
        const auto bit = std::countr_zero(_word);
        _word = PopBit(_word);
        return _doc = _word_base + bit;
      }
      while (_word_idx < _word_count) {
        _word_base = _chunk_base + _word_idx * 64;
        _word = _chunk_words[_word_idx++];
        if (_word != 0) {
          break;
        }
      }
      if (_word != 0) {
        continue;
      }
      if (_block_remaining > 0) {
        LoadChunk();
        continue;
      }
      if (!OpenNextRg()) {
        return _doc = doc_limits::eof();
      }
    }
  }

  doc_id_t seek(doc_id_t target) noexcept final {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    const uint64_t row = target - doc_limits::min();
    if (row >= _block_row) {
      if (!SeekBlock(row)) {
        return _doc = doc_limits::eof();
      }
      LoadChunk();
    }
    while (_doc < target && !doc_limits::eof(_doc)) {
      advance();
    }
    return _doc;
  }

  doc_id_t LazySeek(doc_id_t target) noexcept final { return seek(target); }

  uint32_t count() noexcept final {
    uint32_t c = CountLoadedChunk();
    while (true) {
      if (_block_remaining == 0) {
        if (!NextBlock()) {
          break;
        }
      }
      if (!_scan_data && _block_is_empty) {
        c += static_cast<uint32_t>(_block_remaining);
        _block_remaining = 0;
        continue;
      }
      LoadChunk();
      c += CountLoadedChunk();
    }
    _word = 0;
    _word_count = 0;
    _word_idx = 0;
    _doc = doc_limits::eof();
    return c;
  }

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final {
    CollectImpl(*this, scorer, fetcher, collector);
  }

  std::pair<doc_id_t, bool> FillBlock(doc_id_t min, doc_id_t max,
                                      uint64_t* mask,
                                      FillBlockScoreContext score,
                                      FillBlockMatchContext match) final {
    return FillBlockImpl(*this, min, max, mask, score, match);
  }

 private:
  bool NextBlock() noexcept {
    if (_scan_data) {
      if (_next_validity_block != 0) {
        return false;
      }
      _next_validity_block = 1;
      _block_row = 0;
      _block_remaining = _reader->RowCount();
      _block_is_empty = false;
      return true;
    }
    while (_next_validity_block < _reader->ValidityRgCount()) {
      const auto vrg = _next_validity_block++;
      const auto rg_count = _reader->ValidityRgRowCount(vrg);
      if (rg_count == 0) {
        continue;
      }
      _block_row = _reader->ValidityRgFirstRow(vrg);
      _block_remaining = rg_count;
      _block_is_empty = _reader->IsValidityRgEmpty(vrg);
      return true;
    }
    return false;
  }

  bool OpenNextRg() noexcept {
    if (!NextBlock()) {
      return false;
    }
    LoadChunk();
    return true;
  }

  bool SeekBlock(uint64_t row) noexcept {
    while (row >= _block_row + _block_remaining) {
      if (!NextBlock()) {
        return false;
      }
    }
    const uint64_t chunk = (row - _block_row) / STANDARD_VECTOR_SIZE;
    const uint64_t skip = chunk * STANDARD_VECTOR_SIZE;
    _block_row += skip;
    _block_remaining -= skip;
    return true;
  }

  uint32_t CountLoadedChunk() noexcept {
    uint32_t c = static_cast<uint32_t>(std::popcount(_word));
    while (_word_idx < _word_count) {
      c += static_cast<uint32_t>(std::popcount(_chunk_words[_word_idx++]));
    }
    _word = 0;
    return c;
  }

  void LoadChunk() noexcept {
    const auto take =
      std::min<duckdb::idx_t>(_block_remaining, STANDARD_VECTOR_SIZE);
    _word_count = (take + 63) / 64;
    auto* words = _batch.BufferMutable().GetValidityMask().GetData();
    if (_scan_data || _block_is_empty) {
      std::memset(words, 0xFF, _word_count * sizeof(*words));
    }
    if (_scan_data || !_block_is_empty) {
      const uint64_t cur = _null_reader->CursorRow(_scan);
      if (_block_row > cur) {
        _null_reader->Skip(_scan, _block_row - cur);
      }
      if (_scan_data) {
        _null_reader->ScanCount(_scan, _batch, take, /*result_offset=*/0);
      } else {
        _null_reader->Scan(_scan, _batch, take);
      }
    }
    if (const auto tail = (_word_count * 64) - take; tail != 0) {
      words[_word_count - 1] &= (~uint64_t{0}) >> tail;
    }
    _chunk_words = words;
    _chunk_base = doc_limits::min() + _block_row;
    _block_row += take;
    _block_remaining -= take;
    _word_idx = 0;
    _word = 0;
  }

  using Attributes = std::tuple<CostAttr>;

  const ColumnReader* _reader;
  ReadContext _ctx;
  bool _scan_data;
  const ColumnReader* _null_reader;
  ColumnReader::ScanState _scan;
  duckdb::Vector _batch;
  Attributes _attrs;

  size_t _next_validity_block = 0;
  uint64_t _block_row = 0;
  uint64_t _block_remaining = 0;
  bool _block_is_empty = false;

  const duckdb::validity_t* _chunk_words = nullptr;
  uint64_t _chunk_base = 0;  // doc-id of bit 0 of word 0 in current chunk
  size_t _word_count = 0;
  size_t _word_idx = 0;
  duckdb::validity_t _word = 0;
  doc_id_t _word_base = 0;
};

class AllDocsExistenceIterator : public DocIterator {
 public:
  explicit AllDocsExistenceIterator(uint32_t docs_count) noexcept
    : _max_doc{doc_limits::min() + docs_count - 1} {
    std::get<CostAttr>(_attrs).reset(_max_doc);
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return irs::GetMutable(_attrs, id);
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& /*ctx*/) final {
    return ScoreFunction::Default();
  }

  doc_id_t advance() noexcept final {
    _doc = _doc < _max_doc ? _doc + 1 : doc_limits::eof();
    return _doc;
  }

  doc_id_t seek(doc_id_t target) noexcept final {
    _doc = target <= _max_doc ? target : doc_limits::eof();
    return _doc;
  }

  doc_id_t LazySeek(doc_id_t target) noexcept final { return seek(target); }

  uint32_t count() noexcept final {
    if (doc_limits::eof(_doc)) {
      return 0;
    }
    const auto c = _max_doc - _doc;
    _doc = doc_limits::eof();
    return c;
  }

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final {
    CollectImpl(*this, scorer, fetcher, collector);
  }

  std::pair<doc_id_t, bool> FillBlock(doc_id_t min, doc_id_t max,
                                      uint64_t* mask,
                                      FillBlockScoreContext score,
                                      FillBlockMatchContext match) final {
    return FillBlockImpl(*this, min, max, mask, score, match);
  }

 private:
  using Attributes = std::tuple<CostAttr>;
  doc_id_t _max_doc;
  Attributes _attrs;
};

class ColumnExistenceQuery : public QueryBuilder {
 public:
  ColumnExistenceQuery(const SubReader& segment, field_id id,
                       score_t boost) noexcept
    : QueryBuilder{segment}, _id{id}, _boost{boost} {}

  DocIterator::ptr Execute(const ExecutionContext&,
                           const StatsBuffer&) const final {
    const auto* column = _segment.Column(_id);
    if (column == nullptr) {
      return DocIterator::empty();
    }
    const uint64_t row_count = column->RowCount();
    if (row_count == 0) {
      return DocIterator::empty();
    }
    if (!column->HasValidity() && !column->NullsInData()) {
      return memory::make_managed<AllDocsExistenceIterator>(
        static_cast<uint32_t>(row_count));
    }
    const auto* col_reader = _segment.GetColReader();
    SDB_ENSURE(col_reader, sdb::ERROR_INTERNAL,
               "column_existence_filter: segment has no .col reader");
    return memory::make_managed<ColumnExistenceIterator>(
      *column, *col_reader, static_cast<CostAttr::Type>(row_count));
  }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 private:
  field_id _id;
  score_t _boost;
};

}  // namespace

QueryBuilder::ptr ByColumnExistence::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  return memory::make_tracked<ColumnExistenceQuery>(ctx.memory, segment, _id,
                                                    ctx.boost * Boost());
}

}  // namespace irs
