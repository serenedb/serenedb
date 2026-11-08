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
#include <span>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/fsst.hpp"
#include "iresearch/formats/column/codecs/segment_rows.hpp"
#include "iresearch/formats/column/codecs/string_tuning.hpp"

namespace irs::codecs::text {

class FsstState {
 public:
  static constexpr ByteCodec kCodec = ByteCodec::Fsst;

  FsstState(const duckdb::LogicalType& type, DedupScratch& dedup,
            EstimatorState& estimators, uint32_t target)
    : _rows{type, dedup, estimators.codes},
      _history{estimators.history[static_cast<size_t>(kCodec)]},
      _target{target} {}

  void Begin(const Config& config, uint64_t first_row, size_t distinct,
             Segment& out) {
    _rows.Begin(config, first_row, distinct);
    _out = &out;
    out.bytes.Truncate(0);
  }

  IRS_FORCE_INLINE void AddValue(const duckdb::string_t* value) {
    _rows.AddValue(value, [](const duckdb::string_t&, uint32_t) {});
  }

  IRS_FORCE_INLINE void AddCode(
    uint32_t code, std::span<const duckdb::string_t* const> values) {
    _rows.AddCode(code, values, [](const duckdb::string_t&, uint32_t) {});
  }

  IRS_FORCE_INLINE bool Full(uint32_t target) {
    return _rows.EstimateDue(target) &&
           _rows.Estimate() +
               EstimateEntries(
                 _rows.View(),
                 _history[static_cast<size_t>(_rows.SegmentShape())]) >=
             target;
  }

  void Finish();

 private:
  SegmentRows _rows;
  SortScratch _sort;
  RatioHistory* _history;
  uint32_t _target;
  FsstEncoder _codec;
  Segment* _out = nullptr;
  std::vector<FrameMeta> _frames;
  std::vector<uint32_t> _lcp_stream;
};

}  // namespace irs::codecs::text
