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
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>

#include "iresearch/formats/column/codecs/numeric_layout.hpp"
#include "iresearch/index/column_info.hpp"

namespace irs::codecs {

bool NumericApplies(duckdb::PhysicalType physical) noexcept;

struct NumericChoice {
  NumericTransform transform = NumericTransform::Raw;
  NumericLeaf leaf = NumericLeaf::None;
  uint8_t level = 0;
  bool shuffled = false;

  friend bool operator==(const NumericChoice&, const NumericChoice&) = default;
};

struct NumericSegment {
  duckdb::BaseStatistics stats;
  uint64_t rows = 0;
  NumericChoice choice;
  std::string bytes;
};

struct NumericTuning {
  std::optional<NumericChoice> pick;
  bool calibrated = false;
  uint32_t gap = 1;
  uint32_t since = 0;
  double bytes_per_row = 0;

  bool Due() noexcept { return !calibrated || ++since >= gap; }
};

std::optional<NumericSegment> EncodeCodes(std::span<const uint32_t> codes,
                                          uint64_t rival_bytes,
                                          NumericTuning& tuning);

class NumericSealer {
 public:
  static std::unique_ptr<NumericSealer> Make(const duckdb::LogicalType& type,
                                             uint64_t rows);

  virtual ~NumericSealer() = default;

  virtual void Add(const duckdb::Vector& input) = 0;

  virtual std::optional<NumericSegment> Seal(const ColCodecParams& params,
                                             uint64_t rival_bytes,
                                             NumericTuning& tuning,
                                             bool due) = 0;
};

}  // namespace irs::codecs
