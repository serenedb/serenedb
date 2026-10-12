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
#include <yaclib/async/future.hpp>

#include "iresearch/formats/ann_build_env.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/merge/merge_profile.hpp"
#include "iresearch/index/segment_writer_options.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/noncopyable.hpp"
#include "iresearch/utils/string.hpp"

namespace duckdb {

class DatabaseInstance;
}

namespace irs {

class MergeExecutor;
struct TrackingDirectory;

class MergeWriter : public util::Noncopyable {
 public:
  using FlushProgress = std::function<bool()>;

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
  auto Flush(SegmentMeta& segment, const FlushProgress& progress = {},
             const AnnBuildEnv* env = nullptr,
             MergeExecutor* executor = nullptr) -> yaclib::Future<bool>;

  const MergeDocMap& DocMap() const noexcept { return _readers; }

  MergeDocMap TakeDocMap() && noexcept { return std::move(_readers); }

  const MergeProfile& Profile() const noexcept { return _profile; }

 private:
  Directory& _dir;
  MergeDocMap _readers;
  ScorerPtr _scorer;
  duckdb::DatabaseInstance& _db;
  // Non-owning; the caller pins it for the whole merge.
  const IndexFieldOptions* _field_options = nullptr;
  MergeProfile _profile;
};

static_assert(std::is_nothrow_move_constructible_v<MergeWriter>);

}  // namespace irs
