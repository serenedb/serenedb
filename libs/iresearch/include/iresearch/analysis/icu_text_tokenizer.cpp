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

#include <simdutf.h>
#include <unicode/brkiter.h>
#include <unicode/ubrk.h>
#include <unicode/uloc.h>
#include <unicode/unistr.h>
#include <unicode/utext.h>

#include <memory>
#include <string_view>
#include <vector>

#include "iresearch/analysis/text/segment/fill.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

using Options = IcuTextTokenizer::Options;
using Accept = Options::Accept;
using Convert = segment::Convert;

std::unique_ptr<icu::BreakIterator> MakeBreakIterator(
  Options::Separate separate, const icu::Locale& locale) {
  auto err = UErrorCode::U_ZERO_ERROR;
  std::unique_ptr<icu::BreakIterator> it{
    separate == Options::Separate::Word
      ? icu::BreakIterator::createWordInstance(locale, err)
      : icu::BreakIterator::createSentenceInstance(locale, err)};
  if (!U_SUCCESS(err) || !it) {
    THROW_SQL_ERROR(
      ERR_MSG("icu_text: failed to create a break iterator for "
              "locale '",
              locale.getName(), "': ", u_errorName(err)));
  }
  return it;
}

bool RulesAreTailored(const icu::BreakIterator& it) {
  auto err = UErrorCode::U_ZERO_ERROR;
  const char* actual = it.getLocaleID(ULOC_ACTUAL_LOCALE, err);
  if (!U_SUCCESS(err) || actual == nullptr) {
    return true;
  }
  const std::string_view name{actual};
  return !name.empty() && name != "root";
}

template<Options::Separate S>
class IcuTextAnalyzerImpl final : public TypedTokenizer<IcuTextAnalyzerImpl<S>>,
                                  public IcuTextTokenizer {
 public:
  explicit IcuTextAnalyzerImpl(const Options& opts)
    : _accept{opts.accept},
      _break{MakeBreakIterator(S, opts.locale)},
      _tailored{RulesAreTailored(*_break)} {}

  ~IcuTextAnalyzerImpl() final { utext_close(&_ut); }

  BlockTraits WantedBlockTraits() const noexcept final {
    if constexpr (S == Options::Separate::Word) {
      return {.ascii = !_tailored};
    } else {
      return {.ascii = _accept != Accept::Any};
    }
  }

  auto PrepareBatch(BlockTraits traits) const noexcept {
    return std::tuple{_accept, traits.ascii};
  }

  TokenTraits Traits() const noexcept final {
    return {.offsets = true, .stable = true};
  }

  size_t MemoryUsage() const noexcept final {
    return _u16.capacity() * sizeof(char16_t);
  }

  template<TokenLayout Layout, Accept A, bool KnownAscii>
  bool DoFill(const duckdb::string_t& raw, TokenSink& sink) {
    if constexpr (S == Options::Separate::Word && KnownAscii) {
      segment::WordFillValue<Layout, Convert::None, A, true>(sink, raw);
      return true;
    } else {
      return FillStaged<Layout, A, KnownAscii>(sink, raw);
    }
  }

 private:
  template<TokenLayout Layout, Accept A, bool KnownAscii, typename ToByte>
  IRS_FORCE_INLINE void EmitBoundaries(TokenSink& sink, const char* data,
                                       uint32_t n, ToByte to_byte) {
    uint32_t begin = 0;
    _break->first();
    for (auto end = _break->next(); end != icu::BreakIterator::DONE;
         end = _break->next()) {
      const uint32_t stop = to_byte(end);
      if constexpr (S == Options::Separate::Sentence) {
        segment::EmitTrimmedSegment<Layout, Convert::None, A, KnownAscii>(
          sink, data, n, begin, stop);
      } else {
        if constexpr (A == Accept::AlphaNumeric || A == Accept::Alpha) {
          if (_break->getRuleStatus() == UWordBreak::UBRK_WORD_NONE) {
            begin = stop;
            continue;
          }
        }
        segment::EmitAccepted<Layout, Convert::None, A, KnownAscii>(
          sink, data, n, begin, stop);
      }
      begin = stop;
    }
  }

  template<TokenLayout Layout, Accept A, bool KnownAscii>
  bool FillUtf8(TokenSink& sink, const duckdb::string_t& value) {
    const char* data = value.GetData();
    const uint32_t n = value.GetSize();
    auto status = UErrorCode::U_ZERO_ERROR;
    utext_openUTF8(&_ut, data, static_cast<int64_t>(n), &status);
    if (!U_SUCCESS(status)) [[unlikely]] {
      return false;
    }
    _break->setText(&_ut, status);
    if (!U_SUCCESS(status)) [[unlikely]] {
      return false;
    }
    EmitBoundaries<Layout, A, KnownAscii>(
      sink, data, n,
      [](int32_t pos) IRS_FORCE_INLINE { return static_cast<uint32_t>(pos); });
    return true;
  }

  template<TokenLayout Layout, Accept A, bool KnownAscii>
  bool FillStaged(TokenSink& sink, const duckdb::string_t& value) {
    const char* data = value.GetData();
    const uint32_t n = value.GetSize();
    if (n == 0) {
      return true;
    }
    if (_u16.size() < n) {
      _u16.resize(n);
    }
    const size_t len = simdutf::convert_utf8_to_utf16(data, n, _u16.data());
    if (len == 0) [[unlikely]] {
      return FillUtf8<Layout, A, KnownAscii>(sink, value);
    }
    _text.setTo(false, _u16.data(), static_cast<int32_t>(len));
    _break->setText(_text);
    const auto* p = reinterpret_cast<const uint8_t*>(data);
    uint32_t byte = 0;
    int32_t unit = 0;
    EmitBoundaries<Layout, A, KnownAscii>(
      sink, data, n, [&](int32_t pos) IRS_FORCE_INLINE {
        while (unit < pos) {
          const uint8_t lead = p[byte];
          const uint32_t size = lead < 0x80   ? 1
                                : lead < 0xE0 ? 2
                                : lead < 0xF0 ? 3
                                              : 4;
          unit += size == 4 ? 2 : 1;
          byte += size;
        }
        return byte;
      });
    return true;
  }

  Accept _accept;
  std::unique_ptr<icu::BreakIterator> _break;
  bool _tailored;
  UText _ut = UTEXT_INITIALIZER;
  std::vector<char16_t> _u16;
  icu::UnicodeString _text;
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
