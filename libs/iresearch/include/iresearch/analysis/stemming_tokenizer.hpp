////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <unicode/locid.h>

#include "iresearch/analysis/stem_cache.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/icu_locale_serde.hpp"
#include "iresearch/utils/snowball_stemmer.hpp"
#include "tokenizer.hpp"

namespace irs {
namespace analysis {

// an tokenizer capable of stemming the text, treated as a single token,
// for supported languages
// expects UTF-8 encoded input
class StemmingTokenizer final : public TypedTokenizer<StemmingTokenizer>,
                                private util::Noncopyable {
 public:
  struct Options {
    using Owner = StemmingTokenizer;
    icu::Locale locale = irs::MakeBogusLocale();
  };
  static ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept { return "stem"; }

  explicit StemmingTokenizer(Options options);

  TokenTraits Traits() const noexcept final {
    return {
      .unique = true,
      .offsets = true,
    };
  }

  template<TokenLayout Layout>
  bool DoFill(const duckdb::string_t& value, TokenSink& sink);

  duckdb::string_t Stem(const duckdb::string_t& data);

 private:
  Options _options;
  stemmer_ptr _stemmer;
  StemCache _cache;
};

extern template class TypedTokenizer<StemmingTokenizer>;

}  // namespace analysis
}  // namespace irs
