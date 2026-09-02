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

#include "basics/serializer.h"
#include "iresearch/analysis/process_tokens.hpp"
#include "iresearch/utils/icu_locale_serde.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

class CollationTokenizer final : public TypedTokenizer<CollationTokenizer>,
                                 public TypedTokenStage<CollationTokenizer>,
                                 private util::Noncopyable {
 public:
  struct Options {
    using Owner = CollationTokenizer;
    icu::Locale locale = irs::MakeBogusLocale();
  };
  static ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept { return "collation"; }

  explicit CollationTokenizer(const Options& options);

  TokenTraits Traits() const noexcept final {
    return {
      .output = duckdb::LogicalTypeId::BLOB,
      .unique = true,
      .offsets = true,
    };
  }

  BlockTraits WantedBlockTraits() const noexcept final {
    return {.ascii = true};
  }

  std::tuple<bool> PrepareBatch(BlockTraits traits) const noexcept {
    return {traits.ascii};
  }

  size_t MemoryUsage() const noexcept final {
    return _u16_buf.capacity() * sizeof(char16_t);
  }

  template<TokenLayout Layout, bool Ascii, typename Sink>
  bool DoFill(duckdb::string_t value, Sink& sink);

 private:
  struct CollatorDeleter {
    void operator()(UCollator* p) const noexcept { ucol_close(p); }
  };

  std::unique_ptr<UCollator, CollatorDeleter> _collator;
  std::vector<char16_t> _u16_buf;
};

extern template class TypedTokenizer<CollationTokenizer>;
extern template class TypedTokenStage<CollationTokenizer>;

template<typename Context>
void SerdeWrite(Context ctx, const CollationTokenizer::Options& o) {
  const bool legacy_force_utf8 = true;
  sdb::basics::WriteTupleOrObject(ctx, std::tie(o.locale, legacy_force_utf8));
}

template<typename Context>
void SerdeRead(Context ctx, CollationTokenizer::Options& o) {
  bool legacy_force_utf8 = true;
  auto refs = std::tie(o.locale, legacy_force_utf8);
  sdb::basics::ReadTupleOrObject(ctx, refs);
}

}  // namespace irs::analysis
