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

#include <absl/hash/hash.h>

#include <array>
#include <bit>
#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/text/dedup_scratch.hpp"
#include "iresearch/formats/column/codecs/text/segment_rows.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/utils/containers/flat_hash_set.hpp"
#include "iresearch/utils/default_init_allocator.hpp"

namespace irs::codecs::text {

class Values final : public State {
 public:
  static constexpr StateId kId = StateId::TextValues;

  explicit Values(ColumnDataCheckpointData& data);
  Values(const Values&) = delete;
  Values& operator=(const Values&) = delete;

  uint64_t Rows() const noexcept { return codes.size(); }

  Features GetFeatures() const noexcept {
    const auto valid =
      static_cast<float>(std::max<uint64_t>(Rows() - null_count, 1));
    return {
      static_cast<float>(raw) / valid,
      static_cast<float>(distinct.size()) / valid,
      0.F,
    };
  }

  bool Repeats() const noexcept {
    const uint64_t null_values = null_count != 0 ? 1 : 0;
    return (distinct.size() + null_values) * kMinRepeat <= Rows();
  }

  uint64_t null_count = 0;
  uint64_t raw = 0;
  std::vector<const duckdb::string_t*> distinct;
  ScratchVector<uint32_t> codes;

 private:
  static constexpr uint64_t kMinRepeat = 2;

  using Distinct = std::vector<const duckdb::string_t*>;

  void Add(const duckdb::Vector& input);

  struct Hash {
    using is_transparent = void;

    size_t operator()(const duckdb::string_t& value) const noexcept {
      if (value.IsInlined()) {
        return absl::HashOf(std::bit_cast<std::array<uint64_t, 2>>(value));
      }
      return absl::HashOf(std::string_view{value.GetData(), value.GetSize()});
    }

    size_t operator()(uint32_t code) const noexcept {
      return (*this)(*(*distinct)[code - 1]);
    }

    const Distinct* distinct;
  };

  struct Eq {
    using is_transparent = void;

    bool operator()(uint32_t lhs, uint32_t rhs) const noexcept;

    bool operator()(uint32_t code,
                    const duckdb::string_t& value) const noexcept {
      return *(*distinct)[code - 1] == value;
    }

    const Distinct* distinct;
  };

  containers::FlatHashSet<uint32_t, Hash, Eq> _codes{0, Hash{&distinct},
                                                     Eq{&distinct}};
  uint32_t _last_code = 0;
  const duckdb::string_t* _last = nullptr;
};

}  // namespace irs::codecs::text
