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

StemmingTokenizer::StemmingTokenizer(Options options)
  : _options{std::move(options)} {
  if (_options.locale.isBogus()) {
    THROW_SQL_ERROR(ERR_MSG("stem: invalid locale"));
  }
  // defaults to utf-8; an unsupported language leaves the stemmer null and
  // the tokenizer passes values through unstemmed
  _stemmer = make_stemmer_ptr(_options.locale.getLanguage(), nullptr);
}

Tokenizer::ptr StemmingTokenizer::Make(Options opts) {
  return std::make_unique<StemmingTokenizer>(std::move(opts));
}

duckdb::string_t StemmingTokenizer::Stem(const duckdb::string_t& data) {
  const std::string_view key{data.GetData(), data.GetSize()};
  if (_stemmer && key.size() <= static_cast<uint32_t>(
                                  std::numeric_limits<int32_t>::max())) {
    if (const auto stemmed = _cache.Stem(_stemmer.get(), key)) {
      return MakeTermView(stemmed->data(),
                          static_cast<uint32_t>(stemmed->size()));
    }
  }
  // use the value of the unstemmed token
  return data;
}

template<TokenLayout Layout>
bool StemmingTokenizer::DoFill(const duckdb::string_t& raw, TokenSink& sink) {
  const auto raw_size = static_cast<uint32_t>(raw.GetSize());
  if (_stemmer &&
      raw_size > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    return false;
  }
  const auto stemmed = Stem(raw);
  const char* const data = stemmed.GetData();
  const uint32_t size = stemmed.GetSize();
  if (size <= duckdb::string_t::INLINE_LENGTH || data == raw.GetData()) {
    sink.Emit<Layout>(stemmed);
  } else {
    sink.Emit<Layout>(
      size,
      [&](byte_type* mem) IRS_FORCE_INLINE {
        std::memcpy(mem, data, size);
        return size;
      });
  }
  return true;
}

template class TypedTokenizer<StemmingTokenizer>;

}  // namespace irs::analysis
