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

#include <cstdint>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/index_reader.hpp>
#include <optional>
#include <span>
#include <vector>

namespace duckdb {

class TaskScheduler;

}  // namespace duckdb
namespace sdb::search {

struct ColumnCopyCheck {
  irs::field_id column = irs::field_limits::invalid();
  uint64_t rows = 0;
  uint64_t bytes = 0;
  std::optional<uint64_t> reencoded_bytes{};
  bool codec_changed = false;

  bool Saves() const noexcept;
  bool Reencode() const noexcept { return codec_changed || Saves(); }
};

struct SegmentCopyCheck {
  uint64_t bytes = 0;
  uint64_t saved_bytes = 0;
  bool codec_changed = false;

  bool Rewrite() const noexcept;
  uint64_t SavedPercent() const noexcept;
};

std::vector<ColumnCopyCheck> CheckColumns(
  const irs::SubReader& segment, const irs::IndexFieldOptions& options);

SegmentCopyCheck CheckSegment(const irs::SubReader& segment,
                              const irs::IndexFieldOptions& options);

class CopyChecker {
 public:
  CopyChecker(const irs::IndexFieldOptions& options,
              duckdb::TaskScheduler& scheduler) noexcept
    : _options{&options}, _scheduler{&scheduler} {}

  const irs::IndexFieldOptions& Options() const noexcept { return *_options; }

  void Warm(std::span<const irs::SubReader* const> segments) const;

  SegmentCopyCheck Check(const irs::SubReader& segment) const {
    return CheckSegment(segment, *_options);
  }

 private:
  const irs::IndexFieldOptions* _options;
  duckdb::TaskScheduler* _scheduler;
};

}  // namespace sdb::search
