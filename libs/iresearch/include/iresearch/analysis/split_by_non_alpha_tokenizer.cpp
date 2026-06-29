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

#include "split_by_non_alpha_tokenizer.hpp"

#include <absl/strings/ascii.h>

namespace irs::analysis {

Analyzer::ptr SplitByNonAlphaTokenizer::Make(Options opts) {
  return std::make_unique<SplitByNonAlphaTokenizer>(opts.to_lower);
}

bool SplitByNonAlphaTokenizer::reset(std::string_view data) {
  _data = data;
  _pos = 0;
  return true;
}

bool SplitByNonAlphaTokenizer::next() {
  const char* const base = _data.data();
  const size_t size = _data.size();

  // Skip separators: any byte that is not an ASCII letter or digit. Non-ASCII
  // bytes (>= 0x80) are not alphanumeric, so they act as separators too, which
  // matches a `[^A-Za-z0-9]+` split.
  while (_pos < size &&
         !absl::ascii_isalnum(static_cast<unsigned char>(base[_pos]))) {
    ++_pos;
  }
  if (_pos >= size) {
    return false;
  }

  const size_t start = _pos;
  while (_pos < size &&
         absl::ascii_isalnum(static_cast<unsigned char>(base[_pos]))) {
    ++_pos;
  }
  const size_t end = _pos;

  auto& offset = std::get<OffsAttr>(_attrs);
  auto& term = std::get<TermAttr>(_attrs);
  auto& inc = std::get<IncAttr>(_attrs);

  offset.start = static_cast<uint32_t>(start);
  offset.end = static_cast<uint32_t>(end);
  inc.value = 1;

  const std::string_view token{base + start, end - start};
  if (_to_lower) {
    _term_buf.resize(token.size());
    absl::ascii_internal::AsciiStrToLower(_term_buf.data(), token.data(),
                                          token.size());
    term.value = ViewCast<byte_type>(std::string_view{_term_buf});
  } else {
    term.value = ViewCast<byte_type>(token);
  }
  return true;
}

}  // namespace irs::analysis
