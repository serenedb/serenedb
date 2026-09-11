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

#include <limits>

#include "iresearch/analysis/process_tokens.hpp"
#include "iresearch/analysis/text/dict/stem_cache.hpp"
#include "iresearch/utils/icu_locale_serde.hpp"
#include "iresearch/utils/snowball_stemmer.hpp"
#include "tokenizer.hpp"

namespace irs {
namespace analysis {

// an tokenizer capable of stemming the text, treated as a single token,
// for supported languages
// expects UTF-8 encoded input
class StemmingTokenizer final : public TypedTokenizer<StemmingTokenizer>,
                                public TypedTokenStage<StemmingTokenizer>,
                                private util::Noncopyable {
 public:
  struct Options {
    using Owner = StemmingTokenizer;
    icu::Locale locale = irs::MakeBogusLocale();
  };
  static ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept { return "stem"; }

  explicit StemmingTokenizer(const Options& options);

  TokenTraits Traits() const noexcept final {
    return {
      .unique = true,
      .offsets = true,
    };
  }

  template<TokenLayout Layout, typename Sink>
  IRS_FORCE_INLINE bool DoFill(const duckdb::string_t& value, Sink& sink);

  size_t MemoryUsage() const noexcept final { return _cache.MemoryBytes(); }

 private:
  static bool FitsStemmer(size_t size) noexcept {
    return size <= static_cast<size_t>(std::numeric_limits<int32_t>::max());
  }

  stemmer_ptr _stemmer;
  dict::StemCache _cache;
};

extern template class TypedTokenizer<StemmingTokenizer>;
extern template class TypedTokenStage<StemmingTokenizer>;

}  // namespace analysis
}  // namespace irs
