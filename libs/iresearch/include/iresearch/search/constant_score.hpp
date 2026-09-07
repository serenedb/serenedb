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

#include <string>

#include "iresearch/search/scorer.hpp"

namespace irs {

class ConstantScore final : public irs::ScorerBase<ConstantScore, void> {
 public:
  static constexpr std::string_view type_name() noexcept { return "constant"; }

  static constexpr score_t VALUE() noexcept { return 1.f; }

  struct Options {
    using Owner = ConstantScore;
    score_t value = VALUE();
    bool operator==(const Options&) const = default;
  };

  static std::unique_ptr<ConstantScore> Make(const Options& opts) {
    return std::make_unique<ConstantScore>(opts.value);
  }

  constexpr explicit ConstantScore(score_t value = VALUE()) noexcept
    : _value{value} {}

  ScoreFunction PrepareScorer(const ScoreContext& ctx) const final;

  IndexFeatures GetIndexFeatures() const noexcept final {
    return IndexFeatures::None;
  }

  bool ScoresPerDoc() const noexcept final { return false; }

  bool equals(const Scorer& other) const noexcept final;

  std::string ToString() const final;

 private:
  score_t _value;
};

const ConstantScore& DefaultConstScore() noexcept;

inline bool NeedsTermStats(const Scorer& scorer) {
  return scorer.stats_size() != 0 ||
         scorer.GetIndexFeatures() != IndexFeatures::None;
}

inline const Scorer* ResolveScorer(const Scorer* own, const Scorer* parent) {
  if (own == nullptr) {
    return parent;
  }
  if (parent != nullptr && own == &DefaultConstScore() &&
      !NeedsTermStats(*parent)) {
    return parent;
  }
  return own;
}

}  // namespace irs
