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

#include <unicode/locid.h>

#include <iresearch/analysis/tokenizer_config.hpp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "test_resources.hpp"

namespace tests {

struct TextChain {
  std::string locale = "en_US.UTF-8";
  irs::Case convert = irs::Case::None;
  bool stemming = true;
  bool accent = true;
  std::vector<std::string> stopwords;
};

inline irs::analysis::TokenizerConfig TextChainConfig(TextChain chain) {
  using namespace irs::analysis;
  const auto locale = icu::Locale::createFromName(chain.locale.c_str());
  PipelineTokenizer::Options opts;
  const auto add = [&](TokenizerConfig cfg) {
    opts.children.push_back(std::make_unique<TokenizerConfig>(std::move(cfg)));
  };
  add({TextTokenizer::Options{.convert = chain.convert}});
  if (!chain.accent) {
    add({NormalizingTokenizer::Options{.locale = locale, .accent = false}});
  }
  if (!chain.stopwords.empty()) {
    add({StopwordsTokenizer::Options{.mask = std::move(chain.stopwords)}});
  }
  if (chain.stemming) {
    add({StemmingTokenizer::Options{.locale = locale}});
  }
  return {std::move(opts)};
}

inline irs::analysis::Tokenizer::ptr MakeTextChain(TextChain chain) {
  return irs::analysis::CreateTokenizer(TextChainConfig(std::move(chain)),
                                        Cache());
}

}  // namespace tests
