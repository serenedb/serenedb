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

#include "analyzer.hpp"
#include "basics/shared.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "token_attributes.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

class SegmentationTokenizer : public TypedAnalyzer<SegmentationTokenizer>,
                              private util::Noncopyable {
 public:
  static constexpr size_t kMaxStringSizeToOptimizeAccept = 64;

  static constexpr std::string_view type_name() noexcept {
    return "segmentation";
  }

  struct Options {
    using Owner = SegmentationTokenizer;

    // Separate input to tokens based on rules for
    enum class Separate : uint8_t {
      // TODO(mbkkt) other options?
      None = 0,
      // This feels useless
      // Graphema,
      Word,
      // These can make sense with access to term dictionary/widlcard analyzer
      // Sentence,
      // Paragraph,
      // Line,
    };
    // Accept tokens which contains
    enum class Accept : uint8_t {
      Any = 0,       // anything
      Graphic,       // at least one not whitespace
      AlphaNumeric,  // at least one letter or numeric
      Alpha,         // at least one letter
      // TODO(mbkkt) other options?
      // Also we can implement "contains only" logic
      // For an example, AsciiOnly
    };
    // Convert tokens to some case
    enum class Convert : uint8_t {
      None = 0,
      Lower,
      Upper,
    };
    // TODO(mbkkt) speedup implementation and decrease it size via
    // 1. removing special cases in convert to specific languages
    //    because for locale everyone use icu
    // 2. removing breaking in opposite direction: prev_word_break, etc
    // 3. remove utf8 <-> utf16 encoding
    //    separate, accept and convert works only for utf32
    //    so we needed only utf8/utf16 <-> utf32
    // 4. unify kGeneralCategoryTable and word_prop_map
    // 5. ascii token -> ascii accept and convert
    // 6. ascii input -> ascii next
    // 7. maybe make check in convert before convert
    // 8. remove not used functions/classes, especially tables!

    // lowercase tokens, match default values in text analyzer
    Separate separate = Separate::Word;
    Accept accept = Accept::AlphaNumeric;
    Convert convert = Convert::Lower;
  };

  static Analyzer::ptr Make(Options opts);

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

 protected:
  using Attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;

  Attributes _attrs;
  // buffer for value if value cannot be referenced directly
  std::string _term_buf;
};

}  // namespace irs::analysis
