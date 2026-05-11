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
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct TrackingDirectory;
class Comparer;

class MergeWriter : public util::Noncopyable {
 public:
  using FlushProgress = std::function<bool()>;

  struct ReaderCtx {
    ReaderCtx(const SubReader* reader, IResourceManager& rm) noexcept;
    ReaderCtx(const SubReader& reader, IResourceManager& rm) noexcept
      : ReaderCtx{&reader, rm} {}

    const SubReader* reader;                    // segment reader
    ManagedVector<doc_id_t> doc_id_map;         // FIXME use bitpacking vector
    std::function<doc_id_t(doc_id_t)> doc_map;  // mapping function
  };

  MergeWriter(IResourceManager& resource_manager) noexcept;

  explicit MergeWriter(Directory& dir,
                       const SegmentWriterOptions& options) noexcept
    : _dir{dir},
      _readers{{options.resource_manager}},
      _column_info{&options.column_info},
      _scorer{options.scorer},
      _comparator{options.comparator},
      _scorers_features{options.scorers_features} {
    SDB_ASSERT(_column_info);
  }
  MergeWriter(MergeWriter&&) = default;
  MergeWriter& operator=(MergeWriter&&) = delete;

  explicit operator bool() const noexcept;

  template<typename Iterator>
  void Reset(Iterator begin, Iterator end) {
    _readers.reserve(_readers.size() + std::distance(begin, end));
    while (begin != end) {
      _readers.emplace_back(*begin++, _readers.get_allocator().Manager());
    }
  }

  // Flush all of the added readers into a single segment.
  // `segment` the segment that was flushed.
  // `progress` report flush progress (abort if 'progress' returns false).
  // Return merge successful.
  bool Flush(SegmentMeta& segment, const FlushProgress& progress = {});

  const ReaderCtx& operator[](size_t i) const noexcept {
    SDB_ASSERT(i < _readers.size());
    return _readers[i];
  }

 private:
  bool FlushSorted(TrackingDirectory& dir, SegmentMeta& segment,
                   const FlushProgress& progress);

  bool FlushUnsorted(TrackingDirectory& dir, SegmentMeta& segment,
                     const FlushProgress& progress);

  Directory& _dir;
  ManagedVector<ReaderCtx> _readers;
  const ColumnInfoProvider* _column_info{};
  ScorerPtr _scorer;
  const Comparer* const _comparator{};
  IndexFeatures _scorers_features{};
};

static_assert(std::is_nothrow_move_constructible_v<MergeWriter>);

}  // namespace irs
