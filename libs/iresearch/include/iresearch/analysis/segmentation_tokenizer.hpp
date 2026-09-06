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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "basics/shared.hpp"
#include "iresearch/analysis/text/segment/options.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

class SegmentationTokenizer : private util::Noncopyable {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "segmentation";
  }

  struct Options {
    using Owner = SegmentationTokenizer;

    enum class Separate : uint8_t {
      None = 0,
      Word,
      Sentence,
      Line,
      Paragraph,
    };
    using Accept = segment::Accept;
    Separate separate = Separate::Word;
    Accept accept = Accept::AlphaNumeric;
    Case convert = Case::Lower;
  };

  static Tokenizer::ptr Make(Options opts);

  virtual ~SegmentationTokenizer() = default;
};

}  // namespace irs::analysis
namespace magic_enum {

// The one reflection of Accept, beside the enum: every TU that reflects it
// must see the same names, or the linker mixes the per-TU tables (ODR). The
// names are the user surface ("alpha" accepts letters and digits); Alpha is
// internal and hidden from parsing.
template<>
constexpr customize::customize_t
customize::enum_name<irs::analysis::SegmentationTokenizer::Options::Accept>(
  irs::analysis::SegmentationTokenizer::Options::Accept value) noexcept {
  using Accept = irs::analysis::SegmentationTokenizer::Options::Accept;
  switch (value) {
    case Accept::Any:
      return "all";
    case Accept::Graphic:
      return "graphic";
    case Accept::AlphaNumeric:
      return "alpha";
    case Accept::Alpha:
      return invalid_tag;
  }
  return invalid_tag;
}

}  // namespace magic_enum
