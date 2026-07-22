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

#include "iresearch/analysis/batch/token_batch.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

StemmingTokenizer::StemmingTokenizer(Options options)
  : _options{std::move(options)} {}

Tokenizer::ptr StemmingTokenizer::Make(Options opts) {
  if (opts.locale.isBogus()) {
    THROW_SQL_ERROR(ERR_MSG("stem: invalid locale"));
  }
  return std::make_unique<StemmingTokenizer>(std::move(opts));
}

bytes_view StemmingTokenizer::Stem(std::string_view data) {
  if (!_stemmer) {
    // defaults to utf-8
    _stemmer = make_stemmer_ptr(_options.locale.getLanguage(), nullptr);
  }
  _input_size = static_cast<uint32_t>(data.size());
  if (_stemmer && data.size() <= static_cast<uint32_t>(
                                   std::numeric_limits<int32_t>::max())) {
    const bool cacheable = data.size() <= kMaxCachedKey;
    if (cacheable) {
      if (const auto it = _cache.find(data); it != _cache.end()) {
        return ViewCast<byte_type>(std::string_view{it->second});
      }
    }
    static_assert(sizeof(sb_symbol) == sizeof(char));
    const auto* value = reinterpret_cast<const sb_symbol*>(data.data());
    value =
      sb_stemmer_stem(_stemmer.get(), value, static_cast<int>(data.size()));
    if (value) {
      static_assert(sizeof(byte_type) == sizeof(sb_symbol));
      const std::string_view stemmed{
        reinterpret_cast<const char*>(value),
        static_cast<size_t>(sb_stemmer_length(_stemmer.get()))};
      if (cacheable) {
        if (_cache.size() >= kMaxCacheEntries) [[unlikely]] {
          _cache.clear();
        }
        _cache.emplace(data, stemmed);
      }
      return ViewCast<byte_type>(stemmed);
    }
  }
  // use the value of the unstemmed token
  static_assert(sizeof(byte_type) == sizeof(char));
  return ViewCast<byte_type>(data);
}

template<TokenLayout Layout>
bool StemmingTokenizer::DoFill(std::string_view value, TokenEmitter& sink) {
  if (value.size() >
      static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    if (!_stemmer) {
      _stemmer = make_stemmer_ptr(_options.locale.getLanguage(), nullptr);
    }
    if (_stemmer) {
      sink.buf.unique = false;
      return false;
    }
  }
  const auto stemmed = Stem(value);
  sink.EmitInterned<Layout>(stemmed, 0, _input_size);
  return true;
}

template class TypedTokenizer<StemmingTokenizer>;

}  // namespace irs::analysis
