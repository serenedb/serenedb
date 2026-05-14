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

#include <duckdb/common/types/vector.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <iresearch/columnstore/column_reader.hpp>
#include <iresearch/columnstore/format.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/directory_reader_impl.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_reader.hpp>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "query/duckdb_engine.h"

namespace sdb::connector {

class SegmentPkSequentialFetcher {
 public:
  bool Open(const irs::IndexReader& reader, size_t seg_idx);

  explicit operator bool() const noexcept { return _pk_col != nullptr; }

  void Fetch(std::span<const irs::doc_id_t> sorted_docs, duckdb::Vector& out,
             duckdb::idx_t out_start);

  void Close() noexcept;

 private:
  const irs::columnstore::ColumnReader* _pk_col = nullptr;
};

class SegmentPkRandomFetcher {
 public:
  bool Open(const irs::IndexReader& reader, size_t seg_idx);

  explicit operator bool() const noexcept { return _pk_col != nullptr; }

  std::string_view Fetch(irs::doc_id_t doc_id);

  void Close() noexcept;

 private:
  const irs::columnstore::ColumnReader* _pk_col = nullptr;
  duckdb::unique_ptr<duckdb::ColumnSegment> _seg;
  duckdb::ColumnFetchState _fetch_state;
  size_t _cur_rg = std::numeric_limits<size_t>::max();
  duckdb::Vector _value_vec{duckdb::LogicalType::BLOB, 1};
};

template<typename Hits, typename Proj, typename OnSegment, typename OnDoc>
void WalkSegmentsSorted(const Hits& hits, Proj&& proj,
                        std::vector<uint32_t>& scratch_idx,
                        OnSegment&& on_segment, OnDoc&& on_doc) {
  const size_t n = std::ranges::size(hits);
  scratch_idx.resize(n);
  absl::c_iota(scratch_idx, uint32_t{0});
  std::ranges::sort(scratch_idx, {}, [&](uint32_t i) { return proj(hits[i]); });

  size_t i = 0;
  while (i < n) {
    const auto [seg_id, _] = proj(hits[scratch_idx[i]]);
    if (!on_segment(seg_id)) {
      while (i < n && proj(hits[scratch_idx[i]]).first == seg_id) {
        ++i;
      }
      continue;
    }
    while (i < n) {
      auto [seg, doc] = proj(hits[scratch_idx[i]]);
      if (seg != seg_id) {
        break;
      }
      on_doc(scratch_idx[i], seg, doc);
      ++i;
    }
  }
}

template<typename Hits, typename Proj, typename Sink>
void LookupSegmentsValues(const Hits& hits, Proj&& proj,
                          const irs::IndexReader& reader,
                          std::vector<uint32_t>& scratch_idx, Sink&& sink) {
  const size_t n = std::ranges::size(hits);
  if (n == 0) {
    return;
  }
  scratch_idx.resize(n);
  absl::c_iota(scratch_idx, uint32_t{0});
  std::ranges::sort(scratch_idx, {}, [&](uint32_t i) { return proj(hits[i]); });

  SegmentPkSequentialFetcher fetcher;
  std::vector<irs::doc_id_t> seg_docs;

  size_t i = 0;
  while (i < n) {
    const auto seg_id = proj(hits[scratch_idx[i]]).first;
    const size_t seg_begin = i;
    while (i < n && proj(hits[scratch_idx[i]]).first == seg_id) {
      ++i;
    }
    const size_t seg_count = i - seg_begin;

    if (!fetcher.Open(reader, seg_id)) {
      continue;
    }

    seg_docs.resize(seg_count);
    for (size_t k = 0; k < seg_count; ++k) {
      seg_docs[k] = static_cast<irs::doc_id_t>(
        proj(hits[scratch_idx[seg_begin + k]]).second);
    }

    duckdb::Vector seg_pk_vec{duckdb::LogicalType::BLOB,
                              static_cast<duckdb::idx_t>(seg_count)};
    fetcher.Fetch(seg_docs, seg_pk_vec, 0);

    auto* data = duckdb::FlatVector::GetData<duckdb::string_t>(seg_pk_vec);
    for (size_t k = 0; k < seg_count; ++k) {
      sink(scratch_idx[seg_begin + k],
           std::string_view{data[k].GetData(),
                            static_cast<size_t>(data[k].GetSize())});
    }
  }
}

}  // namespace sdb::connector
