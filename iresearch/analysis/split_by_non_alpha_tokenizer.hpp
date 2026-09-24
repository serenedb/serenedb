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

#include "iresearch/utils/serializer.hpp"
#include "iresearch/utils/shared.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

class SplitByNonAlphaTokenizer final
  : public TypedTokenizer<SplitByNonAlphaTokenizer>,
    private util::Noncopyable {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "split_by_non_alpha";
  }

  struct Options {
    using Owner = SplitByNonAlphaTokenizer;
    enum class Chars : uint8_t {
      Ascii = 0,
      AsciiBytes,
      Alnum,
      Letters,
      Whitespace,
    };
    Case case_convert{Case::None};
    Chars chars{Chars::Ascii};
  };
  static ptr Make(Options opts);

  explicit SplitByNonAlphaTokenizer(Options opts) noexcept : _options{opts} {}

  BlockTraits WantedBlockTraits() const noexcept final {
    using enum Options::Chars;
    return {.ascii =
              _options.chars == Alnum || _options.chars == Letters ||
              (_options.chars != Ascii && _options.case_convert != Case::None)};
  }

  auto PrepareBatch(BlockTraits traits) const {
    return std::tuple{_options.case_convert, _options.chars, traits.ascii};
  }

  TokenTraits Traits() const noexcept final {
    return {.offsets = true,
            .stable = _options.case_convert == Case::None,
            .keeps_ascii = true};
  }

  template<TokenLayout Layout, Case C, Options::Chars W, bool KnownAscii>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  Options _options;
};

template<typename Context>
void SerdeWrite(Context ctx, const SplitByNonAlphaTokenizer::Options& o) {
  irs::utils::WriteTupleOrObject(ctx, std::tie(o.case_convert, o.chars));
}

template<typename Context>
void SerdeRead(Context ctx, SplitByNonAlphaTokenizer::Options& o) {
  auto refs = std::tie(o.case_convert, o.chars);
  irs::utils::ReadTupleOrObject(ctx, refs);
}

}  // namespace irs::analysis
namespace magic_enum {

template<>
constexpr customize::customize_t
customize::enum_name<irs::analysis::SplitByNonAlphaTokenizer::Options::Chars>(
  irs::analysis::SplitByNonAlphaTokenizer::Options::Chars value) noexcept {
  if (value ==
      irs::analysis::SplitByNonAlphaTokenizer::Options::Chars::AsciiBytes) {
    return "ascii_bytes";
  }
  return default_tag;
}

}  // namespace magic_enum
