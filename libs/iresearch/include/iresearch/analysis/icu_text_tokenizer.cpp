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

#include "icu_text_tokenizer.hpp"

#include <unicode/brkiter.h>
#include <unicode/ubrk.h>
#include <unicode/utext.h>

#include <memory>

#include "basics/misc.hpp"
#include "iresearch/analysis/text/segment/fill.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

using Options = IcuTextTokenizer::Options;
using Accept = Options::Accept;
using Convert = segment::Convert;

template<Options::Separate S>
class IcuTextAnalyzerImpl final : public TypedTokenizer<IcuTextAnalyzerImpl<S>>,
                                  public IcuTextTokenizer {
 public:
  explicit IcuTextAnalyzerImpl(const Options& opts) noexcept
    : _accept{opts.accept}, _locale{opts.locale} {}

  BlockTraits WantedBlockTraits() const noexcept final {
    if constexpr (S == Options::Separate::Word) {
      return {.ascii = true};
    } else {
      return {.ascii = _accept != Accept::Any};
    }
  }

  auto PrepareBatch(BlockTraits traits) {
    if (!_break) {
      auto err = UErrorCode::U_ZERO_ERROR;
      if constexpr (S == Options::Separate::Word) {
        _break.reset(icu::BreakIterator::createWordInstance(_locale, err));
      } else {
        _break.reset(icu::BreakIterator::createSentenceInstance(_locale, err));
      }
      if (!U_SUCCESS(err) || !_break) {
        THROW_SQL_ERROR(ERR_MSG("icu_text: failed to create a break iterator"));
      }
    }
    return std::tuple{_accept, traits.ascii};
  }

  TokenTraits Traits() const noexcept final {
    return {.offsets = true, .stable = true};
  }

  template<TokenLayout Layout, Accept A, bool KnownAscii>
  bool DoFill(const duckdb::string_t& raw, TokenSink& sink) {
    if constexpr (S == Options::Separate::Word && KnownAscii) {
      segment::WordFillValue<Layout, Convert::None, A, true>(sink, raw);
      return true;
    } else {
      return FillValue<Layout, A, KnownAscii>(sink, raw);
    }
  }

 private:
  template<TokenLayout Layout, Accept A, bool KnownAscii>
  bool FillValue(TokenSink& sink, const duckdb::string_t& value) {
    const char* data = value.GetData();
    const uint32_t n = value.GetSize();
    auto status = UErrorCode::U_ZERO_ERROR;
    UText ut = UTEXT_INITIALIZER;
    utext_openUTF8(&ut, data, static_cast<int64_t>(n), &status);
    if (!U_SUCCESS(status)) [[unlikely]] {
      return false;
    }
    Finally close_ut = [&]() noexcept { utext_close(&ut); };
    _break->setText(&ut, status);
    if (!U_SUCCESS(status)) [[unlikely]] {
      return false;
    }
    for (auto start = _break->first(), end = _break->next();
         end != icu::BreakIterator::DONE; start = end, end = _break->next()) {
      const auto begin = static_cast<uint32_t>(start);
      const auto stop = static_cast<uint32_t>(end);
      if constexpr (S == Options::Separate::Sentence) {
        segment::EmitTrimmedSegment<Layout, Convert::None, A, KnownAscii>(
          sink, data, n, begin, stop);
      } else {
        if constexpr (A == Accept::AlphaNumeric || A == Accept::Alpha) {
          if (_break->getRuleStatus() == UWordBreak::UBRK_WORD_NONE) {
            continue;
          }
        }
        segment::EmitAccepted<Layout, Convert::None, A, KnownAscii>(
          sink, data, n, begin, stop);
      }
    }
    return true;
  }

  Accept _accept;
  icu::Locale _locale;
  std::unique_ptr<icu::BreakIterator> _break;
};

}  // namespace
}  // namespace irs::analysis
namespace irs {

template<analysis::IcuTextTokenizer::Options::Separate S>
struct Type<analysis::IcuTextAnalyzerImpl<S>>
  : Type<analysis::IcuTextTokenizer> {};

}  // namespace irs
namespace irs::analysis {

Tokenizer::ptr IcuTextTokenizer::Make(Options options) {
  if (options.locale.isBogus()) {
    THROW_SQL_ERROR(ERR_MSG("icu_text: locale is required"));
  }
  using Separate = Options::Separate;
  switch (options.separate) {
    case Separate::Word:
      return std::make_unique<IcuTextAnalyzerImpl<Separate::Word>>(options);
    case Separate::Sentence:
      return std::make_unique<IcuTextAnalyzerImpl<Separate::Sentence>>(options);
  }
}

}  // namespace irs::analysis
