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

#include <magic_enum/magic_enum.hpp>
#include <string>
#include <string_view>
#include <text_casing.hpp>
#include <text_normalizer.hpp>
#include <tuple>
#include <vector>

#include "iresearch/analysis/process_tokens.hpp"
#include "iresearch/utils/locale_serde.hpp"
#include "iresearch/utils/noncopyable.hpp"
#include "tokenizer.hpp"

namespace irs {
namespace analysis {

enum class NormForm : uint8_t {
  Nfc,
  Nfkc,
  Nfd,
  Nfkd,
  NfkcCf,
};

class NormalizingTokenizer final : public TypedTokenizer<NormalizingTokenizer>,
                                   public TypedTokenStage<NormalizingTokenizer>,
                                   private util::Noncopyable {
 public:
  struct Options {
    using Owner = NormalizingTokenizer;
    duckdb::text::Locale locale;
    Case case_convert{Case::None};
    bool accent{true};
    NormForm form{NormForm::Nfc};
    bool fold{false};
  };
  static ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept {
    return "normalize_tokens";
  }

  explicit NormalizingTokenizer(Options options);

  TokenTraits Traits() const noexcept final {
    return {
      .unique = true,
      .offsets = true,
      .keeps_ascii = _case_path != CasePath::Icu,
    };
  }

  std::tuple<Case, bool, bool> PrepareBatch(BlockTraits traits);

  size_t MemoryUsage() const noexcept final {
    return _norm_buf.capacity() + _strip_buf.capacity() +
           (_chars.capacity() + _mapped.capacity()) * sizeof(uint32_t);
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
  template<TokenLayout Layout, Case C, bool Accent, NormForm F, typename Sink>
  bool DecomposedEmit(const duckdb::string_t& raw, Sink& sink);
  template<Case C>
  size_t CaseBound(size_t size) const noexcept;
  template<Case C>
  size_t ConvertCase(std::string_view bytes, byte_type* out) const noexcept;
  template<Case C, bool Accent>
  void NormalizeCaseStrip();

  Options _options;
  std::vector<uint32_t> _chars;
  std::vector<uint32_t> _mapped;
  std::string _norm_buf;
  std::string _strip_buf;
  duckdb::text::NormalizationForm _form;
  duckdb::text::NormalizationForm _renormalize_form;
  duckdb::text::NormalizationForm _strip_form;
  bool _strip_composes{true};
  duckdb::text::CaseLocale _case_locale;
  duckdb::text::CaseFolding _folding{duckdb::text::CaseFolding::DEFAULT};
  CasePath _case_path = CasePath::Fast;
};

}  // namespace analysis
}  // namespace irs
namespace magic_enum {

template<>
constexpr customize::customize_t customize::enum_name<irs::analysis::NormForm>(
  irs::analysis::NormForm value) noexcept {
  using NormForm = irs::analysis::NormForm;
  switch (value) {
    case NormForm::Nfc:
      return "nfc";
    case NormForm::Nfkc:
      return "nfkc";
    case NormForm::Nfd:
      return "nfd";
    case NormForm::Nfkd:
      return "nfkd";
    case NormForm::NfkcCf:
      return "nfkc_cf";
  }
  return invalid_tag;
}

}  // namespace magic_enum
