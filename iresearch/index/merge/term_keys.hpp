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

#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <duckdb/common/sorting/presorted_merge.hpp>
#include <limits>
#include <map>

#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/merge/merge_progress.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class TermKeys final : public duckdb::PresortedMergeInput {
 public:
  static constexpr uint64_t kNoAnchor = std::numeric_limits<uint64_t>::max();

  struct TermAnchor {
    uint64_t row = kNoAnchor;
    bytes_view term;
  };

  explicit TermKeys(const TermReader& reader) noexcept : _reader{reader} {}

  duckdb::idx_t Count() const final { return _reader.size(); }
  duckdb::unique_ptr<duckdb::PresortedMergeReader> CreateReader() const final;

  const TermReader& Reader() const noexcept { return _reader; }

  TermAnchor Anchor(uint64_t row) const;
  void Record(uint64_t row, bytes_view term) const;
  void AddAnchor(uint64_t row) const;

  void SeekBefore(SeekTermIterator& it, uint64_t row) const;

 private:
  const TermReader& _reader;
  mutable absl::Mutex _mutex;
  mutable std::map<uint64_t, bstring> _anchors;
};

bool NextTermRuns(const duckdb::PresortedMerge& merge,
                  duckdb::PresortedMergeState& state,
                  duckdb::vector<duckdb::PresortedRun>& runs,
                  const MergeProgress& progress, field_id field);

}  // namespace irs
