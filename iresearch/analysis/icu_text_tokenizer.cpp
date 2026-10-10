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

#include <string_view>
#include <text_break_iterator.hpp>
#include <type_traits>

#include "iresearch/analysis/text/segment/fill.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::analysis {
namespace {

using Options = IcuTextTokenizer::Options;
using Accept = Options::Accept;

template<Options::Separate S>
class IcuTextAnalyzerImpl final : public TypedTokenizer<IcuTextAnalyzerImpl<S>>,
                                  public IcuTextTokenizer {
 public:
  explicit IcuTextAnalyzerImpl(const Options& opts)
    : _accept{opts.accept},
      _break{S == Options::Separate::Word ? duckdb::text::BreakKind::WORD
                                          : duckdb::text::BreakKind::SENTENCE,
             opts.locale, duckdb::text::BreakUnits::UTF16},
      _scan_ascii{!_break.IsTailored()} {}

  BlockTraits WantedBlockTraits() const noexcept final {
    if constexpr (S == Options::Separate::Word) {
      return {.ascii = _scan_ascii};
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

  size_t MemoryUsage() const noexcept final { return 0; }

  bool Scan(const duckdb::string_t& value, TokenSink& sink,
            ScanProgress& progress, BlockTraits known) final {
    return ScanWithProgress(*this, value, sink, progress, known);
  }

  template<TokenLayout Layout, Accept A, bool KnownAscii,
           typename Progress = classify::NoProgress>
  bool DoFill(const duckdb::string_t& raw, TokenSink& sink,
              Progress progress = {}) {
    if constexpr (S == Options::Separate::Word && KnownAscii) {
      segment::WordFillValue<Layout, Case::None, A, true>(sink, raw, progress);
      return true;
    } else {
      return FillValue<Layout, A, KnownAscii>(sink, raw, progress);
    }
  }

 private:
  template<TokenLayout Layout, Accept A, bool KnownAscii, typename Progress>
  bool FillValue(TokenSink& sink, const duckdb::string_t& value,
                 Progress progress) {
    const char* data = value.GetData();
    const uint32_t n = value.GetSize();
    if (n == 0) {
      return true;
    }
    _break.SetText(data, n);

    [[maybe_unused]] const auto report = BindProgress(progress, sink);
    [[maybe_unused]] uint32_t reported = 0;
    uint32_t begin = 0;
    for (auto end = _break.Next(); end != duckdb::text::BreakIterator::DONE;
         end = _break.Next()) {
      const auto stop = static_cast<uint32_t>(end);
      if constexpr (S == Options::Separate::Sentence) {
        segment::EmitTrimmedSegment<Layout, Case::None, A, KnownAscii>(
          sink, data, n, begin, stop);
      } else if constexpr (A == Accept::AlphaNumeric || A == Accept::Alpha) {
        if (_break.GetRuleStatus() != duckdb::text::WORD_NONE) {
          segment::EmitAccepted<Layout, Case::None, A, KnownAscii>(
            sink, data, n, begin, stop);
        }
      } else {
        segment::EmitAccepted<Layout, Case::None, A, KnownAscii>(sink, data, n,
                                                                 begin, stop);
      }
      begin = stop;
      if constexpr (!std::is_same_v<Progress, classify::NoProgress>) {
        if (stop - reported >= classify::kBytesPerProgress) {
          reported = stop;
          if (!report()) {
            return true;
          }
        }
      }
    }
    return true;
  }

  Accept _accept;
  duckdb::text::BreakIterator _break;
  bool _scan_ascii;
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
  if (options.locale.IsBogus()) {
    THROW_SQL_ERROR(ERR_MSG("split_text_icu: locale is required"));
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
