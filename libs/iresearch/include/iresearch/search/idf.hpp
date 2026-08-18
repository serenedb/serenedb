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

#include "iresearch/search/scorer.hpp"

namespace irs {

struct IDFStats {
  score_t value;
};

class IDF final : public irs::ScorerBase<IDF, IDFStats> {
 public:
  static constexpr std::string_view type_name() noexcept { return "idf"; }

  struct Options {
    using Owner = IDF;
    bool operator==(const Options&) const = default;
  };

  static std::unique_ptr<IDF> Make(const Options& /*opts*/) {
    return std::make_unique<IDF>();
  }

  void collect(byte_type* stats_buf, const irs::FieldCollector* field,
               const irs::TermCollector* term) const final;

  IndexFeatures GetIndexFeatures() const noexcept final {
    return IndexFeatures::None;
  }

  ScoreFunction PrepareScorer(const ScoreContext& ctx) const final;
};

}  // namespace irs
