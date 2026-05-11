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

#include "connector/search_pk_lookup.h"

#include <duckdb/common/vector/flat_vector.hpp>

#include "catalog/table_options.h"

namespace sdb::connector {
namespace {

const irs::columnstore::ColumnReader* OpenPkColumn(
  const irs::IndexReader& reader, size_t seg_idx, duckdb::DatabaseInstance& db,
  std::optional<irs::columnstore::Reader>& out_reader) {
  out_reader.reset();
  const auto* dir_reader = dynamic_cast<const irs::DirectoryReader*>(&reader);
  if (!dir_reader) {
    return nullptr;
  }
  const auto& impl = *dir_reader->GetImpl();
  const auto& segments = impl.Meta().index_meta.segments;
  if (seg_idx >= segments.size()) {
    return nullptr;
  }
  out_reader.emplace(impl.Dir(), segments[seg_idx].meta.name, db);
  return out_reader->Column(
    static_cast<irs::field_id>(catalog::Column::kGeneratedPKId));
}

}  // namespace

bool SegmentPkSequentialFetcher::Open(const irs::IndexReader& reader,
                                      size_t seg_idx,
                                      duckdb::DatabaseInstance& db) {
  Close();
  _pk_col = OpenPkColumn(reader, seg_idx, db, _reader);
  return _pk_col != nullptr;
}

void SegmentPkSequentialFetcher::Fetch(
  std::span<const irs::doc_id_t> sorted_docs, duckdb::Vector& out,
  duckdb::idx_t out_start) {
  if (sorted_docs.empty() || !_pk_col) {
    return;
  }
  // Contract: `sorted_docs` strictly ascending. Both the rg-window math
  // (`row_n != row_k + run_len`) and the codec Skip target (in_rg_k > cursor)
  // would mis-derive runs if a duplicate or out-of-order doc slipped in.
  SDB_ASSERT(absl::c_is_sorted(sorted_docs));

  // Translate 1-based iresearch doc_ids to 0-based row positions on the
  // fly so the shared ScanRowsBatched helper (which speaks row_pos) can
  // drive. The view is non-owning; only [] is needed.
  struct RowView {
    std::span<const irs::doc_id_t> docs;
    size_t size() const noexcept { return docs.size(); }
    uint64_t operator[](size_t i) const noexcept {
      return static_cast<uint64_t>(docs[i]) -
             static_cast<uint64_t>(irs::doc_limits::min());
    }
  };
  irs::columnstore::ColumnReader::RangeScan range{*_pk_col};
  irs::columnstore::ColumnReader::ScanRowsBatched(range, RowView{sorted_docs},
                                                  out, out_start);
}

void SegmentPkSequentialFetcher::Close() noexcept {
  _reader.reset();
  _pk_col = nullptr;
}

bool SegmentPkRandomFetcher::Open(const irs::IndexReader& reader,
                                  size_t seg_idx,
                                  duckdb::DatabaseInstance& db) {
  Close();
  _pk_col = OpenPkColumn(reader, seg_idx, db, _reader);
  return _pk_col != nullptr;
}

std::string_view SegmentPkRandomFetcher::Fetch(irs::doc_id_t doc_id) {
  if (!_pk_col) {
    return {};
  }
  const uint64_t row = doc_id - irs::doc_limits::min();
  const auto window = _pk_col->Locate(row);
  if (window.rg >= _pk_col->RowGroupCount()) {
    return {};
  }
  if (window.rg != _cur_rg) {
    _seg = _pk_col->OpenSegment(window.rg);
    _fetch_state = duckdb::ColumnFetchState{};
    _cur_rg = window.rg;
  }
  const uint64_t in_rg = row - window.begin;
  _seg->FetchRow(_fetch_state, static_cast<duckdb::row_t>(in_rg), _value_vec,
                 0);
  auto* data = duckdb::FlatVector::GetData<duckdb::string_t>(_value_vec);
  return {data[0].GetData(), static_cast<size_t>(data[0].GetSize())};
}

void SegmentPkRandomFetcher::Close() noexcept {
  _reader.reset();
  _pk_col = nullptr;
  _seg.reset();
  _fetch_state = duckdb::ColumnFetchState{};
  _cur_rg = std::numeric_limits<size_t>::max();
}

}  // namespace sdb::connector
