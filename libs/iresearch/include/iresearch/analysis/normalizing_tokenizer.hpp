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
#include <unicode/normalizer2.h>
#include <unicode/translit.h>

#include <memory>
#include <string>
#include <tuple>

#include "basics/noncopyable.hpp"
#include "iresearch/analysis/process_tokens.hpp"
#include "iresearch/utils/icu_locale_serde.hpp"
#include "tokenizer.hpp"

namespace irs {
namespace analysis {

enum class NormForm : uint8_t {
  Nfc,
  Nfkc,
};

class NormalizingTokenizer final : public TypedTokenizer<NormalizingTokenizer>,
                                   public TypedTokenStage<NormalizingTokenizer>,
                                   private util::Noncopyable {
 public:
  struct Options {
    using Owner = NormalizingTokenizer;
    icu::Locale locale = irs::MakeBogusLocale();
    Case case_convert{Case::None};
    bool accent{true};
    NormForm form{NormForm::Nfc};
  };
  static ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept { return "norm"; }

  explicit NormalizingTokenizer(Options options);

  TokenTraits Traits() const noexcept final {
    return {
      .unique = true,
      .offsets = true,
    };
  }

  std::tuple<Case, bool, bool> PrepareBatch(BlockTraits traits);

  size_t MemoryUsage() const noexcept final {
    return _norm_buf.capacity() + _strip_buf.capacity() +
           static_cast<size_t>(_udata.getCapacity() + _token.getCapacity()) *
             sizeof(char16_t);
  }

  template<TokenLayout Layout, Case C, bool Accent, bool KnownAscii,
           typename Sink>
  bool DoFill(const duckdb::string_t& value, Sink& sink);

  BlockTraits WantedBlockTraits() const noexcept final {
    return {.ascii = _case_path != CasePath::Icu};
  }

 private:
  enum class CasePath : uint8_t {
    Fast,
    IcuNonAscii,
    Icu,
  };

  template<TokenLayout Layout, Case C, bool Accent, typename Sink>
  bool UnicodeEmit(const duckdb::string_t& raw, Sink& sink);
  template<TokenLayout Layout, Case C, bool Accent, NormForm F, typename Sink>
  bool FastUnicodeEmit(const duckdb::string_t& raw, Sink& sink);

  Options _options;
  icu::UnicodeString _udata;
  icu::UnicodeString _token;
  const icu::Normalizer2* _normalizer{};
  std::unique_ptr<icu::Transliterator> _transliterator;
  std::string _norm_buf;
  std::string _strip_buf;
  CasePath _case_path = CasePath::Fast;
};

extern template class TypedTokenizer<NormalizingTokenizer>;
extern template class TypedTokenStage<NormalizingTokenizer>;

}  // namespace analysis
}  // namespace irs
