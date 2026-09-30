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

#include <memory>
#include <span>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/search/filters/phrase_filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {
namespace analysis {

class ShingleTokenizer;

}  // namespace analysis

class PhraseTokenSourceFactory;

struct ShinglePhrasePlan {
  enum class Kind : uint8_t {
    None,
    Term,
    Phrase,
  };

  Kind kind = Kind::None;
  bstring term;
  ByPhraseOptions phrase;
};

ByPhraseOptions MakeTokenPhrase(std::span<const bytes_view> tokens,
                                std::span<const PosAttr::value_t> positions);

ShinglePhrasePlan PlanShinglePhrase(
  const analysis::ShingleTokenizer& tokenizer,
  std::span<const bytes_view> tokens,
  std::span<const PosAttr::value_t> positions, bool positional,
  std::shared_ptr<const PhraseTokenSourceFactory> source);

}  // namespace irs
