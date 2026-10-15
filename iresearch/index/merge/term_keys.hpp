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
#include <duckdb/common/sorting/presorted_merge.hpp>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "iresearch/utils/string.hpp"

namespace irs {

struct TermReader;

class TermKeys final : public duckdb::PresortedMergeInput {
 public:
  static constexpr uint64_t kAnchorStep = 256;

  explicit TermKeys(const TermReader& reader) noexcept : _reader{reader} {}

  duckdb::idx_t Count() const final;
  duckdb::unique_ptr<duckdb::PresortedMergeReader> CreateReader() const final;

  const TermReader& Reader() const noexcept { return _reader; }

  std::optional<std::pair<uint64_t, bstring>> Anchor(uint64_t row) const;
  void Record(uint64_t row, bytes_view term) const;

 private:
  const TermReader& _reader;
  mutable std::mutex _mutex;
  mutable std::vector<bstring> _anchors;
};

}  // namespace irs
