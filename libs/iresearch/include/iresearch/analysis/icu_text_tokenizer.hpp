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

#pragma once

#include <unicode/locid.h>

#include "basics/shared.hpp"
#include "iresearch/analysis/text/segment/options.hpp"
#include "iresearch/utils/icu_locale_serde.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

class IcuTextTokenizer : private util::Noncopyable {
 public:
  static constexpr std::string_view type_name() noexcept { return "icu_text"; }

  struct Options {
    using Owner = IcuTextTokenizer;

    enum class Separate : uint8_t {
      Word = 0,
      Sentence,
    };
    using Accept = segment::Accept;

    Separate separate = Separate::Word;
    Accept accept = Accept::AlphaNumeric;
    icu::Locale locale = irs::MakeBogusLocale();
  };

  static Tokenizer::ptr Make(Options opts);

  virtual ~IcuTextTokenizer() = default;
};

}  // namespace irs::analysis
