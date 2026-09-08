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

class Unscored final : public irs::ScorerBase<Unscored, void> {
 public:
  static constexpr std::string_view type_name() noexcept { return "unscored"; }

  struct Options {
    using Owner = Unscored;
    bool operator==(const Options&) const = default;
  };

  static std::unique_ptr<Unscored> Make(const Options&) {
    return std::make_unique<Unscored>();
  }

  static const Unscored& Instance() noexcept {
    static const Unscored kInstance;
    return kInstance;
  }

  IndexFeatures GetIndexFeatures() const noexcept final {
    return IndexFeatures::None;
  }

  bool ScoresPerDoc() const noexcept final { return false; }

  ScoreFunction PrepareScorer(const ScoreContext&) const final {
    return ScoreFunction::Default();
  }

  std::string ToString() const final { return "unscored"; }
};

inline bool IsUnscored(const Scorer& scorer) noexcept {
  return scorer.type() == irs::Type<Unscored>::id();
}

}  // namespace irs
