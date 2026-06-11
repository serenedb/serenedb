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

#include "basics/exceptions.h"

namespace irs::analysis {

StemmingTokenizer::StemmingTokenizer(Options options)
  : _options{std::move(options)} {}

Analyzer::ptr StemmingTokenizer::Make(Options opts) {
  if (opts.locale.isBogus()) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER, "stem: invalid locale");
  }
  return std::make_unique<StemmingTokenizer>(std::move(opts));
}

bool StemmingTokenizer::next() {
  if (_term_eof) {
    return false;
  }

  _term_eof = true;

  return true;
}

bool StemmingTokenizer::reset(std::string_view data) {
  if (!_stemmer) {
    // defaults to utf-8
    _stemmer = make_stemmer_ptr(_options.locale.getLanguage(), nullptr);
  }

  auto& term = std::get<TermAttr>(_attrs);

  term.value = {};  // reset

  auto& offset = std::get<OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = static_cast<uint32_t>(data.size());

  _term_eof = false;

  // find the token stem
  std::string_view utf8_data{data};

  if (_stemmer) {
    if (utf8_data.size() >
        static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
      return false;
    }

    static_assert(sizeof(sb_symbol) == sizeof(char));
    const auto* value = reinterpret_cast<const sb_symbol*>(utf8_data.data());

    value = sb_stemmer_stem(_stemmer.get(), value,
                            static_cast<int>(utf8_data.size()));

    if (value) {
      static_assert(sizeof(byte_type) == sizeof(sb_symbol));
      term.value = bytes_view(reinterpret_cast<const byte_type*>(value),
                              sb_stemmer_length(_stemmer.get()));

      return true;
    }
  }

  // use the value of the unstemmed token
  static_assert(sizeof(byte_type) == sizeof(char));
  term.value = ViewCast<byte_type>(utf8_data);

  return true;
}

}  // namespace irs::analysis
