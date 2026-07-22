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

#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/icu_locale_serde.hpp"
#include "tokenizer.hpp"

namespace irs {
namespace analysis {

////////////////////////////////////////////////////////////////////////////////
/// @class normalizing_token_stream
/// @brief an tokenizer capable of normalizing the text, treated as a single
///        token, i.e. case conversion and accent removal
/// @note expects UTF-8 encoded input
////////////////////////////////////////////////////////////////////////////////
class NormalizingTokenizer final : public TypedTokenizer<NormalizingTokenizer>,
                                   private util::Noncopyable {
 public:
  struct Options {
    using Owner = NormalizingTokenizer;
    icu::Locale locale = irs::MakeBogusLocale();
    Case case_convert{Case::None};  // no extra normalization
    bool accent{true};              // no extra normalization
  };
  static ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept { return "norm"; }

  explicit NormalizingTokenizer(Options options);

  void ForceUnicodePath(bool force) noexcept { _force_unicode = force; }

  TokenTraits Traits() const noexcept final {
    return {.unique = true};
  }

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink);

  bool AsciiFastEligible(std::string_view value) const noexcept;
  bool AsciiRewrite(std::string_view value, std::string& out) const;

 private:
  bool Normalize(std::string_view data);
  template<TokenLayout Layout>
  void AsciiEmit(TokenEmitter& sink, std::string_view value);

  struct StateT;
  struct StateDeleterT {
    void operator()(StateT*) const noexcept;
  };

  std::unique_ptr<StateT, StateDeleterT> _state;
  uint32_t _input_size = 0;
  bool _ascii_case_safe = false;
  bool _force_unicode = false;
};

extern template class TypedTokenizer<NormalizingTokenizer>;

}  // namespace analysis
}  // namespace irs
