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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <unicode/locid.h>
#include <unicode/ucol.h>

#include <memory>
#include <tuple>
#include <vector>

#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/icu_locale_serde.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

// an tokenizer capable of converting UTF-8 encoded input into a sortable
// token as per specified locale
// expects UTF-8 encoded input
class CollationTokenizer final : public TypedTokenizer<CollationTokenizer>,
                                 private util::Noncopyable {
 public:
  struct Options {
    using Owner = CollationTokenizer;
    icu::Locale locale = irs::MakeBogusLocale();
    bool force_utf8 = true;
  };
  static ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept { return "collation"; }

  explicit CollationTokenizer(Options options);

  TokenTraits Traits() const noexcept final {
    return {
      .output = duckdb::LogicalTypeId::BLOB,
      .unique = true,
      .offsets = true,
    };
  }

  std::tuple<> PrepareBatch();

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  struct CollatorDeleter {
    void operator()(UCollator* p) const noexcept { ucol_close(p); }
  };

  Options _options;
  std::unique_ptr<UCollator, CollatorDeleter> _collator;
  std::vector<char16_t> _u16_buf;
};

extern template class TypedTokenizer<CollationTokenizer>;

}  // namespace irs::analysis
