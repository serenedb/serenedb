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

#include <vector>

#include "basics/memory.hpp"
#include "basics/noncopyable.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/string.hpp"

namespace duckdb {

class DatabaseInstance;
}

namespace irs {

struct TrackingDirectory;

struct DocRemap {
  explicit DocRemap(IResourceManager& rm) noexcept : id_map{{rm}} {}

  const DocumentMask* mask = nullptr;
  doc_id_t base_id = doc_limits::invalid();
  ManagedVector<doc_id_t> id_map;

  bool IsMasked(doc_id_t src) const noexcept {
    return mask != nullptr && mask->contains(src);
  }

  doc_id_t Remap(doc_id_t src) const noexcept {
    if (!id_map.empty()) {
      SDB_ASSERT(src < id_map.size());
      return id_map[src];
    }
    return base_id + (src - doc_limits::min());
  }
};

class MergeWriter : public util::Noncopyable {
 public:
  using FlushProgress = std::function<bool()>;

  struct ReaderCtx {
    ReaderCtx(const SubReader* reader, IResourceManager& rm) noexcept;
    ReaderCtx(const SubReader& reader, IResourceManager& rm) noexcept
      : ReaderCtx{&reader, rm} {}

    const SubReader* reader;
    DocRemap remap;
  };

  explicit MergeWriter(Directory& dir,
                       const SegmentWriterOptions& options) noexcept
    : _dir{dir},
      _readers{{options.resource_manager}},
      _scorer{options.scorer},
      _db{[db = options.db] -> duckdb::DatabaseInstance& {
        SDB_ASSERT(db);
        return *db;
      }()},
      _field_options{options.field_options} {}
  MergeWriter(MergeWriter&&) = default;
  MergeWriter& operator=(MergeWriter&&) = delete;

  template<typename Iterator>
  void Reset(Iterator begin, Iterator end) {
    _readers.reserve(_readers.size() + std::distance(begin, end));
    while (begin != end) {
      _readers.emplace_back(*begin++, _readers.get_allocator().Manager());
    }
  }

  // Flush all added readers into a single segment.
  bool Flush(SegmentMeta& segment, const FlushProgress& progress = {});

  const ReaderCtx& operator[](size_t i) const noexcept {
    SDB_ASSERT(i < _readers.size());
    return _readers[i];
  }

 private:
  Directory& _dir;
  ManagedVector<ReaderCtx> _readers;
  ScorerPtr _scorer;
  duckdb::DatabaseInstance& _db;
  // Non-owning; the caller pins it for the whole merge.
  const IndexFieldOptions* _field_options = nullptr;
};

static_assert(std::is_nothrow_move_constructible_v<MergeWriter>);

}  // namespace irs
