////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "stemming_tokenizer.hpp"

#include <libstemmer.h>

#include <string_view>

#include "iresearch/analysis/token_batch.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

StemmingTokenizer::StemmingTokenizer(const Options& options) {
  if (options.locale.isBogus()) {
    THROW_SQL_ERROR(ERR_MSG("stem: invalid locale"));
  }
  _stemmer = make_stemmer_ptr(options.locale.getLanguage(), nullptr);
}

Tokenizer::ptr StemmingTokenizer::Make(Options opts) {
  return std::make_unique<StemmingTokenizer>(std::move(opts));
}

static_assert(std::string().capacity() >= kTermViewSlack);

template<TokenLayout Layout, typename Sink>
IRS_FORCE_INLINE void EmitStem(const std::string& stem, Sink& sink) {
  sink.template Emit<Layout>(stem.data(), static_cast<uint32_t>(stem.size()),
                             stem.data() + kTermViewSlack);
}

template<TokenLayout Layout, typename Sink>
bool StemmingTokenizer::DoFill(const duckdb::string_t& raw, Sink& sink) {
  if (const auto* stem = _cache.Find(raw)) [[likely]] {
    EmitStem<Layout>(*stem, sink);
    return true;
  }
  if (!_stemmer || !FitsStemmer(raw.GetSize())) {
    sink.template Emit<Layout>(raw);
    return true;
  }
  const auto stemmed =
    dict::StemUncached(_stemmer.get(), {raw.GetData(), raw.GetSize()});
  if (!stemmed) {
    sink.template Emit<Layout>(raw);
    return true;
  }
  EmitStem<Layout>(_cache.Insert(raw, *stemmed), sink);
  return true;
}

template class TypedTokenizer<StemmingTokenizer>;
template class TypedTokenStage<StemmingTokenizer>;

}  // namespace irs::analysis
