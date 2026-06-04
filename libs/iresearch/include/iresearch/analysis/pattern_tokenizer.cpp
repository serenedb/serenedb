////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "pattern_tokenizer.hpp"

#include <re2/re2.h>

#include <string_view>

#include "basics/exceptions.h"

namespace irs::analysis {

PatternTokenizer::PatternTokenizer(std::string_view pattern, int group)
  : _pattern(pattern, re2::RE2::Quiet),
    _group(group),
    _num_groups(_pattern.NumberOfCapturingGroups()) {
  _matches.resize(std::max(1, _num_groups + 1));
}
PatternTokenizer::~PatternTokenizer() = default;

Analyzer::ptr PatternTokenizer::Make(Options opts) {
  if (opts.pattern.empty()) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER, "pattern: empty pattern");
  }
  re2::RE2 re(opts.pattern, re2::RE2::Quiet);
  if (!re.ok()) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER, "pattern: invalid regex");
  }
  return std::make_unique<PatternTokenizer>(opts.pattern, opts.group);
}

bool PatternTokenizer::reset(std::string_view data) {
  _data = data;
  _current_pos = 0;
  _exhausted = false;
  return true;
}

bool PatternTokenizer::next() {
  if (_exhausted || _data.empty()) {
    return false;
  }

  auto& offset_attr = std::get<OffsAttr>(_attrs);
  auto& term_attr = std::get<TermAttr>(_attrs);
  auto& inc_attr = std::get<IncAttr>(_attrs);

  const char* const data_base = _data.data();
  const size_t data_len = _data.size();

  while (_current_pos <= data_len) {
    re2::StringPiece input(data_base + _current_pos, data_len - _current_pos);

    if (!_pattern.Match(input, 0, input.size(), re2::RE2::UNANCHORED,
                        _matches.data(), _matches.size())) {
      if (_group < 0 && _current_pos < data_len) {
        const size_t start = _current_pos;
        const size_t end = data_len;

        offset_attr.start = static_cast<uint32_t>(start);
        offset_attr.end = static_cast<uint32_t>(end);
        term_attr.value =
          ViewCast<byte_type>(std::string_view(data_base + start, end - start));
        inc_attr.value = 1;

        _exhausted = true;
        return true;
      }

      _exhausted = true;
      return false;
    }

    const auto& match = _matches[0];

    size_t match_start = _current_pos + (match.data() - input.data());
    size_t match_end = match_start + match.length();

    if (_group >= 0) {
      if (_group <= _num_groups) {
        const auto& g = _matches[_group];

        if (!g.empty()) {
          const size_t start = _current_pos + (g.data() - input.data());
          const size_t end = start + g.length();

          offset_attr.start = static_cast<uint32_t>(start);
          offset_attr.end = static_cast<uint32_t>(end);
          term_attr.value = ViewCast<byte_type>(g);
          inc_attr.value = 1;

          _current_pos = match_end;
          return true;
        }
      }

      _current_pos = (match.length() == 0) ? _current_pos + 1 : match_end;
      continue;
    }

    if (match_start > _current_pos) {
      size_t start = _current_pos;
      size_t end = match_start;

      offset_attr.start = static_cast<uint32_t>(start);
      offset_attr.end = static_cast<uint32_t>(end);
      term_attr.value =
        ViewCast<byte_type>(std::string_view(data_base + start, end - start));
      inc_attr.value = 1;

      _current_pos = match_end;
      return true;
    }

    _current_pos = (match.length() == 0) ? _current_pos + 1 : match_end;
  }

  return false;
}

}  // namespace irs::analysis
