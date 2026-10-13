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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/merge/merge_profile.hpp"
#include "iresearch/index/merge/postings_unit.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace duckdb {

class DatabaseInstance;

}  // namespace duckdb
namespace irs {

class AnnBuildEnv;
struct Directory;
class IndexFieldOptions;
struct SegmentMeta;

enum class MergeUnitKind : uint8_t {
  Norms,
  Column,
  ColumnTrain,
  ColumnCopy,
  ColumnsFooter,
  Ann,
  Postings,
  Index,
};

struct MergeUnit {
  MergeUnitKind kind;
  field_id id = field_limits::invalid();
  uint64_t cost = 0;
  uint32_t part = 0;
};

inline constexpr uint64_t kMergeDocsMax =
  doc_limits::eof() - doc_limits::min() - (uint64_t{1} << 16);

class MergePlan {
 public:
  MergePlan(Directory& dir, MergeDocMap& doc_map, SegmentMeta& segment,
            ScorerPtr scorer, duckdb::DatabaseInstance& db,
            const IndexFieldOptions* field_options,
            const MergeProgress& progress, const AnnBuildEnv* env);
  ~MergePlan();

  MergePlan(const MergePlan&) = delete;
  MergePlan& operator=(const MergePlan&) = delete;

  bool Prepare(size_t threads = 1);

  std::span<const MergeUnit> Units() const noexcept;

  std::span<const size_t> Dependencies(size_t unit) const noexcept;

  bool RunUnit(size_t unit);

  const MergeProfile& Profile() const noexcept;

  void SetWallNanos(uint64_t nanos) noexcept;

 private:
  struct State;
  std::unique_ptr<State> _state;
};

class MergeExecutor {
 public:
  virtual bool Run(MergePlan& plan) = 0;
  virtual size_t Threads() const noexcept { return 1; }

 protected:
  ~MergeExecutor() = default;
};

bool RunSerially(MergePlan& plan);

}  // namespace irs
