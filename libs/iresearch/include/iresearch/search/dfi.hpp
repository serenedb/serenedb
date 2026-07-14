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

#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/scorer.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {

// Divergence From Independence (DFI) similarity.
//
// DFISimilarity:
//   expected = (ttf_t + 1) * dl / (ttf_field + 1)
//   if tf <= expected: score = 0
//   else:              score = boost * log2(measure(tf, expected) + 1)
//
// Parameter-free and non-parametric; three independence measures select
// the measure(tf, expected) kernel:
//   Standardized:  (tf - expected) / sqrt(expected)
//   Saturated:     (tf - expected) / expected
//   ChiSquared:    (tf - expected)^2 / expected
//
// Reference: http://dx.doi.org/10.1007/s10791-013-9225-4
enum class DFIMeasure : uint8_t {
  Standardized,
  Saturated,
  ChiSquared,
};

// Per (field, term) stats: precomputed (ttf_t + 1) / (ttf_field + 1),
// which is multiplied by dl at scoring time to get the expected count.
struct DFIStats {
  score_t ratio;
};

class DFI final : public irs::ScorerBase<DFI, DFIStats> {
 public:
  static constexpr std::string_view type_name() noexcept { return "dfi"; }

  static constexpr DFIMeasure MEASURE() noexcept {
    return DFIMeasure::Standardized;
  }

  struct Options {
    using Owner = DFI;
    DFIMeasure measure = MEASURE();
    bool operator==(const Options&) const = default;
  };

  static std::unique_ptr<DFI> Make(const Options& opts) {
    if (opts.measure > DFIMeasure::ChiSquared) {
      THROW_SQL_ERROR(ERR_MSG("dfi: invalid measure"));
    }
    return std::make_unique<DFI>(opts.measure);
  }

  explicit DFI(DFIMeasure measure = MEASURE()) noexcept : _measure{measure} {}

  void collect(byte_type* stats_buf, const irs::FieldCollector* field,
               const irs::TermCollector* term) const final;

  IndexFeatures GetIndexFeatures() const noexcept final {
    return IndexFeatures::Freq | IndexFeatures::Norm;
  }

  ScoreFunction PrepareScorer(const ScoreContext& ctx) const final;

  bool equals(const Scorer& other) const noexcept final;

  DFIMeasure measure() const noexcept { return _measure; }

 private:
  DFIMeasure _measure;
};

}  // namespace irs
