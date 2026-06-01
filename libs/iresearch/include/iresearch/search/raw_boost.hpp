////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {

struct RawBoost final : ScorerBase<RawBoost, void> {
  static constexpr std::string_view type_name() noexcept { return "raw_boost"; }

  struct Options {
    using Owner = RawBoost;
    bool operator==(const Options&) const = default;
  };

  static std::unique_ptr<RawBoost> Make(const Options& /*opts*/) {
    return std::make_unique<RawBoost>();
  }

  ScoreFunction PrepareScorer(const ScoreContext& ctx) const final;

  IndexFeatures GetIndexFeatures() const noexcept final {
    return IndexFeatures::None;
  }
};

}  // namespace irs
