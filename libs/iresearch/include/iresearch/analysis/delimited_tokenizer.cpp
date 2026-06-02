////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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

#include "delimited_tokenizer.hpp"

#include <string_view>

namespace irs::analysis {
namespace {

bytes_view EvalTerm(bstring& buf, bytes_view data) {
  if (data.empty() || '"' != data[0]) {
    return data;  // not a quoted term (even if quotes inside
  }

  buf.clear();

  bool escaped = false;
  size_t start = 1;

  for (size_t i = 1, count = data.size(); i < count; ++i) {
    if ('"' == data[i]) {
      if (escaped && start == i) {  // an escaped quote
        escaped = false;

        continue;
      }

      if (escaped) {
        break;  // mismatched quote
      }

      buf.append(&data[start], i - start);
      escaped = true;
      start = i + 1;
    }
  }

  return start != 1 && start == data.size()
           ? bytes_view(buf)
           : data;  // return identity for mismatched quotes
}

size_t FindDelimiter(bytes_view data, bytes_view delim) {
  if (IsNull(delim)) {
    return data.size();
  }

  bool quoted = false;

  for (size_t i = 0, count = data.size(); i < count; ++i) {
    if (quoted) {
      if ('"' == data[i]) {
        quoted = false;
      }

      continue;
    }

    if (data.size() - i < delim.size()) {
      break;  // no more delimiters in data
    }

    if (0 == memcmp(data.data() + i, delim.data(), delim.size()) &&
        (i || delim.size())) {  // do not match empty delim at data start
      return i;  // delimiter match takes precedence over '"' match
    }

    if ('"' == data[i]) {
      quoted = true;
    }
  }

  return data.size();
}

}  // namespace

DelimitedTokenizer::DelimitedTokenizer(std::string_view delimiter)
  : _delim(ViewCast<byte_type>(delimiter)) {
  if (!IsNull(_delim)) {
    _delim_buf = _delim;  // keep a local copy of the delimiter
    _delim = _delim_buf;  // update the delimter to point at the local copy
  }
}

Analyzer::ptr DelimitedTokenizer::Make(Options opts) {
  return std::make_unique<DelimitedTokenizer>(opts.delimiter);
}

bool DelimitedTokenizer::next() {
  if (IsNull(_data)) {
    return false;
  }

  auto& offset = std::get<OffsAttr>(_attrs);

  auto size = FindDelimiter(_data, _delim);
  auto next = std::max<size_t>(1, size + _delim.size());
  auto start = offset.end + static_cast<uint32_t>(_delim.size());
  // value is allowed to overflow, will only produce invalid result
  auto end = start + size;

  if (std::numeric_limits<uint32_t>::max() < end) {
    return false;  // cannot fit the next token into offset calculation
  }

  auto& term = std::get<TermAttr>(_attrs);

  offset.start = start;
  offset.end = static_cast<uint32_t>(end);
  term.value = IsNull(_delim)
                 ? bytes_view{_data.data(), size}
                 : EvalTerm(_term_buf, bytes_view(_data.data(), size));
  _data = size >= _data.size()
            ? bytes_view{}
            : bytes_view{_data.data() + next, _data.size() - next};

  return true;
}

bool DelimitedTokenizer::reset(std::string_view data) {
  _data = ViewCast<byte_type>(data);

  auto& offset = std::get<OffsAttr>(_attrs);
  offset.start = 0;
  // counterpart to computation in next() above
  offset.end = 0 - static_cast<uint32_t>(_delim.size());

  return true;
}

}  // namespace irs::analysis
