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
#include <duckdb/common/enums/compression_type.hpp>

#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs {

struct ReencodeEstimate {
  uint64_t stored_bytes = 0;
  uint64_t reencoded_bytes = 0;
  bool codec_changed = false;
};

struct ReencodeKey {
  bool skip_validity = false;
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
  ColCodecParams codec_params;
  uint32_t row_group_size = 0;
  uint64_t dictionary = 0;

  bool operator==(const ReencodeKey&) const = default;
};

class ReencodeCache {
 public:
  template<typename Trial>
  ReencodeEstimate Estimate(field_id column, const ReencodeKey& key,
                            Trial&& trial) {
    if (ReencodeEstimate cached; Find(column, key, cached)) {
      return cached;
    }
    const ReencodeEstimate estimate = trial();
    Store(column, key, estimate);
    return estimate;
  }

 private:
  struct Entry {
    ReencodeKey key;
    ReencodeEstimate estimate;
  };

  bool Find(field_id column, const ReencodeKey& key,
            ReencodeEstimate& estimate) const;
  void Store(field_id column, const ReencodeKey& key,
             const ReencodeEstimate& estimate);

  mutable absl::Mutex _mutex;
  irs::containers::FlatHashMap<field_id, Entry> _entries;
};

}  // namespace irs
