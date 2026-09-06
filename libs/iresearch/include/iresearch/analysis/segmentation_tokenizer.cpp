////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include "segmentation_tokenizer.hpp"

#include <memory>

#include "iresearch/analysis/text/segment/fill.hpp"
#include "iresearch/analysis/token_batch.hpp"

namespace irs::analysis {
namespace {

using Options = SegmentationTokenizer::Options;

template<Options::Separate S>
class UnicodeAnalyzerImpl final : public TypedTokenizer<UnicodeAnalyzerImpl<S>>,
                                  public SegmentationTokenizer {
 public:
  explicit UnicodeAnalyzerImpl(const Options& opts) noexcept
    : _convert{opts.convert}, _accept{opts.accept} {}

  BlockTraits WantedBlockTraits() const noexcept final {
    if constexpr (S == Options::Separate::Word ||
                  S == Options::Separate::None) {
      return {.ascii = true};
    } else {
      return {.ascii = _convert != Case::None ||
                       _accept != Options::Accept::Any};
    }
  }

  std::tuple<Case, Options::Accept, bool> PrepareBatch(
    BlockTraits traits) const noexcept {
    return {_convert, _accept, traits.ascii};
  }

  TokenTraits Traits() const noexcept final {
    return {.offsets = true, .stable = _convert == Case::None};
  }

  template<TokenLayout Layout, Case C, Options::Accept A,
           bool KnownAscii>
  bool DoFill(duckdb::string_t raw, TokenSink& sink) {
    if constexpr (S == Options::Separate::Sentence) {
      segment::SentenceFillValue<Layout, C, A, KnownAscii>(sink, raw);
    } else if constexpr (S == Options::Separate::Line ||
                         S == Options::Separate::Paragraph) {
      segment::LineFillValue<Layout, C, A, S == Options::Separate::Paragraph,
                             KnownAscii>(sink, raw);
    } else if constexpr (S == Options::Separate::Word) {
      segment::WordFillValue<Layout, C, A, KnownAscii>(sink, raw);
    } else {
      segment::WholeFillValue<Layout, C, A, KnownAscii>(sink, raw);
    }
    return true;
  }

 private:
  Case _convert;
  Options::Accept _accept;
};

}  // namespace
}  // namespace irs::analysis
namespace irs {

template<analysis::SegmentationTokenizer::Options::Separate S>
struct Type<analysis::UnicodeAnalyzerImpl<S>>
  : Type<analysis::SegmentationTokenizer> {};

}  // namespace irs
namespace irs::analysis {

Tokenizer::ptr SegmentationTokenizer::Make(Options options) {
  using Separate = Options::Separate;
  switch (options.separate) {
    case Separate::None:
      return std::make_unique<UnicodeAnalyzerImpl<Separate::None>>(options);
    case Separate::Word:
      return std::make_unique<UnicodeAnalyzerImpl<Separate::Word>>(options);
    case Separate::Sentence:
      return std::make_unique<UnicodeAnalyzerImpl<Separate::Sentence>>(options);
    case Separate::Line:
      return std::make_unique<UnicodeAnalyzerImpl<Separate::Line>>(options);
    case Separate::Paragraph:
      return std::make_unique<UnicodeAnalyzerImpl<Separate::Paragraph>>(
        options);
  }
}

}  // namespace irs::analysis
