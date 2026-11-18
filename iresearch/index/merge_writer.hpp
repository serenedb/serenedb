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

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/segment_reader_impl.hpp"
#include "iresearch/index/segment_writer_options.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/noncopyable.hpp"
#include "iresearch/utils/string.hpp"

namespace duckdb {

class DatabaseInstance;
}

namespace irs {

struct TrackingDirectory;

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
      _db{[db = options.db] -> duckdb::DatabaseInstance& {
        SDB_ASSERT(db);
        return *db;
      }()},
      _field_options{options.field_options},
      _hnsw_bases{options.hnsw_bases} {}
  MergeWriter(MergeWriter&&) = default;
  MergeWriter& operator=(MergeWriter&&) = delete;

  template<typename Iterator>
  void Reset(Iterator begin, Iterator end) {
    _readers.reserve(_readers.size() + std::distance(begin, end));
    while (begin != end) {
      _readers.emplace_back(*begin++, _readers.get_allocator().Manager());
    }
  }

  std::shared_ptr<const SegmentReaderImpl> FlushPiece(SegmentMeta& segment,
                                                      uint64_t id);

  const ReaderCtx& operator[](size_t i) const noexcept {
    SDB_ASSERT(i < _readers.size());
    return _readers[i];
  }

 private:
  Directory& _dir;
  ManagedVector<ReaderCtx> _readers;
  duckdb::DatabaseInstance& _db;
  // Non-owning; the caller pins it for the whole merge.
  const IndexFieldOptions* _field_options = nullptr;
  HnswBases* _hnsw_bases = nullptr;
};

static_assert(std::is_nothrow_move_constructible_v<MergeWriter>);

}  // namespace irs
