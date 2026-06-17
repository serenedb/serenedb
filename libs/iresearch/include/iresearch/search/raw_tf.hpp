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

#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {

// RawTF similarity
//
// score(doc, term) = boost * freq(doc, term)
//
// No IDF, no length normalization. Useful as a debugging / teaching
// baseline and as a building block for custom scoring pipelines.
class RawTF final : public irs::ScorerBase<RawTF, void> {
 public:
  static constexpr std::string_view type_name() noexcept { return "raw_tf"; }

  struct Options {
    using Owner = RawTF;
    bool operator==(const Options&) const = default;
  };

  static std::unique_ptr<RawTF> Make(const Options& /*opts*/) {
    return std::make_unique<RawTF>();
  }

  RawTF() noexcept = default;

  IndexFeatures GetIndexFeatures() const noexcept final {
    return IndexFeatures::Freq;
  }

  ScoreFunction PrepareScorer(const ScoreContext& ctx) const final;
};

}  // namespace irs
