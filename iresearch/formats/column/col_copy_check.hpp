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

#include <absl/container/flat_hash_map.h>

#include <cstddef>
#include <cstdint>
#include <duckdb/common/enums/compression_type.hpp>
#include <mutex>
#include <optional>
#include <utility>

#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"

namespace irs {

class ColReader;
class ColumnReader;

inline constexpr size_t kReencodeSampleRowGroups = 2;

struct ReencodeEstimate {
  uint64_t stored_bytes = 0;
  uint64_t reencoded_bytes = 0;
  bool codec_changed = false;

  friend bool operator==(const ReencodeEstimate&,
                         const ReencodeEstimate&) = default;
};

struct ReencodeKey {
  bool skip_validity = false;
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
  ColCodecParams codec_params;
  uint32_t row_group_size = 0;
  size_t samples = 0;

  friend bool operator==(const ReencodeKey&, const ReencodeKey&) = default;
};

class ReencodeCache {
 public:
  std::optional<ReencodeEstimate> Find(field_id column,
                                       const ReencodeKey& key) const;
  void Store(field_id column, const ReencodeKey& key,
             const ReencodeEstimate& estimate);

 private:
  mutable std::mutex _mutex;
  absl::flat_hash_map<field_id, std::pair<ReencodeKey, ReencodeEstimate>>
    _entries;
};

uint64_t StoredBytes(const ColumnReader& column);

std::optional<ReencodeEstimate> EstimateReencode(
  const ColReader& reader, const ColumnReader& column,
  const IndexFieldOptions& options, size_t samples = kReencodeSampleRowGroups);

}  // namespace irs
