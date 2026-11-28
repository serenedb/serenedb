////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/statistics/distinct_statistics.hpp>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/segment_reader.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/containers/node_hash_map.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct FieldTermRange {
  bytes_view min;
  bytes_view max;
  uint64_t distinct = 0;
  uint64_t docs = 0;
};

struct PatchedReader {
  SegmentReader before;
  SegmentReader after;
};

class CompositeReader final : public IndexReader {
 public:
  CompositeReader() = default;
  CompositeReader(std::vector<SegmentReader> readers, bool stats);
  CompositeReader(const CompositeReader& base,
                  std::vector<SegmentReader> readers,
                  std::span<const SegmentReader> added,
                  std::span<const PatchedReader> patched,
                  std::span<const SegmentReader> removed);
  CompositeReader(CompositeReader&&) noexcept = default;
  CompositeReader& operator=(CompositeReader&&) noexcept = default;

  const SubReader& operator[](size_t i) const final { return *_readers[i]; }

  size_t size() const noexcept final { return _readers.size(); }

  uint64_t docs_count() const noexcept final { return _docs; }

  uint64_t live_docs_count() const noexcept final { return _live; }

  uint64_t PatchedDocs() const noexcept { return _patched; }

  const duckdb::BaseStatistics* GetColumnStats(field_id field) const;

  const FieldTermRange* GetTermRange(field_id field, DictRef dict) const;

 private:
  struct ColumnStats {
    duckdb::unique_ptr<duckdb::BaseStatistics> merged;
    duckdb::unique_ptr<duckdb::DistinctStatistics> distinct;
    duckdb::unique_ptr<duckdb::BaseStatistics> published;
    uint64_t docs = 0;
  };

  struct SegmentTerms {
    const TermReader* field = nullptr;
    bytes_view min;
    bytes_view max;
    uint64_t terms = 0;
    uint64_t docs = 0;
  };

  struct TermStats {
    std::vector<SegmentTerms> segments;
    FieldTermRange range;
  };

  void Count() noexcept;
  void Build();
  void AddTerms(const SubReader& reader, bool keep_sorted);
  void RemoveTerms(const SubReader& reader);
  void AddColumns(const SubReader& reader, bool new_docs);
  void RemoveColumns(const SubReader& reader);
  void RecomputeLost(std::span<const SegmentReader> added,
                     std::span<const SegmentReader> removed);
  void Publish();

  std::vector<SegmentReader> _readers;
  uint64_t _docs = 0;
  uint64_t _live = 0;
  uint64_t _patched = 0;
  bool _stats = false;
  containers::FlatHashMap<field_id, ColumnStats> _columns;
  containers::NodeHashMap<std::pair<field_id, DictRef>, TermStats> _terms;
};

}  // namespace irs
