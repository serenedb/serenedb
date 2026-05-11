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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <faiss/impl/HNSW.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/formats/column/hnsw_index.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"

namespace irs {

class DataOutput;
class IndexInput;

namespace columnstore {

// Builds a faiss::HNSW for an ARRAY<FLOAT, N> column. Construction is
// deferred until after the typed column is durable on disk: Build() reads
// vector slices on demand from the ARRAY child column through a small
// MRU chunk cache, so no flat in-memory copy of the segment's vectors is
// ever held -- not during ingest, not during graph construction.
class HNSWWriter final {
 public:
  HNSWWriter(HNSWInfo info);
  ~HNSWWriter();

  HNSWWriter(const HNSWWriter&) = delete;
  HNSWWriter& operator=(const HNSWWriter&) = delete;

  // Build the graph from the typed ARRAY column. Iterates the column in
  // STANDARD_VECTOR_SIZE-floats chunks; the distance computer reads
  // neighbor slices through an MRU chunk cache. Must be called before
  // Serialize().
  void Build(const ColumnReader& vector_column);

  void Serialize(DataOutput& out);

  const HNSWInfo& Info() const noexcept { return _info; }

 private:
  HNSWInfo _info;
  faiss::HNSW _hnsw;
};

// Reader-side counterpart: loads the faiss::HNSW graph from the .cs
// footer side-payload and exposes Search / RangeSearch that fetch
// vectors on demand from the ARRAY ColumnReader's child via the same
// MRU chunk cache used during Build. No per-Reader full-segment cache.
class HNSWReader final {
 public:
  HNSWReader(field_id id, std::string name, faiss::HNSW&& hnsw, HNSWInfo info,
             const ColumnReader& vector_column);
  ~HNSWReader();

  HNSWReader(const HNSWReader&) = delete;
  HNSWReader& operator=(const HNSWReader&) = delete;

  field_id Id() const noexcept { return _id; }
  std::string_view Name() const noexcept { return _name; }
  const HNSWInfo& Info() const noexcept { return _info; }

  void Search(HNSWSearchContext& ctx) const;
  void RangeSearch(HNSWRangeSearchContext& ctx) const;

 private:
  field_id _id;
  std::string _name;
  mutable faiss::HNSW _hnsw;  // mutable because faiss::HNSW::search isn't const
  HNSWInfo _info;
  const ColumnReader& _vector_column;
};

}  // namespace columnstore
}  // namespace irs
