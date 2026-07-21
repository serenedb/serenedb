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

#include "path_hierarchy_tokenizer.hpp"

#include <absl/strings/str_cat.h>

#include <string_view>

#include "iresearch/analysis/batch/term_view.hpp"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

PathHierarchyTokenizer::PathHierarchyTokenizer(Options&& options) noexcept
  : _options{std::move(options)}, _term_eof{true} {}

PathHierarchyTokenizer::~PathHierarchyTokenizer() = default;

// Template parameters:
//   SingleChar: true if delimiter.size() == 1 (fast path)
//   NoReplacement: true if delimiter == replacement (zero-copy path)
template<bool SingleChar, bool NoReplacement>
class ForwardPathHierarchyTokenizer final
  : public TypedTokenizer<
      ForwardPathHierarchyTokenizer<SingleChar, NoReplacement>>,
    public PathHierarchyTokenizer {
 public:
  explicit ForwardPathHierarchyTokenizer(Options&& options) noexcept
    : PathHierarchyTokenizer(std::move(options)) {}

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink) {
    BeginFillValue(value);
    bytes_view term;
    uint32_t start;
    uint32_t end;
    while (NextCore(term, start, end)) {
      if constexpr (NoReplacement) {
        sink.Emit<Layout>(MakeTermView(term), start, end);
      } else {
        sink.EmitInterned<Layout>(term, start, end);
      }
    }
    return true;
  }

 private:
  void BeginFillValue(std::string_view data);
  bool NextCore(bytes_view& term, uint32_t& start, uint32_t& end);
  // delimiter search
  size_t FindNextDelimiter(size_t from_position) const noexcept {
    if constexpr (SingleChar) {
      return _data.find(_options.delimiter[0], from_position);
    } else {
      return _data.find(_options.delimiter, from_position);
    }
  }

  // check if substring at position matches delimiter
  bool IsDelimiterAt(size_t position) const noexcept {
    if constexpr (SingleChar) {
      return position < _data.size() &&
             _data[position] == _options.delimiter[0];
    } else {
      return position + _delimiter_size <= _data.size() &&
             _data.substr(position, _delimiter_size) == _options.delimiter;
    }
  }

  std::string _buffer;  // buffer for NoReplacement = false mode
  size_t _prefix_start_in_input =
    0;  // left edge of prefix; every token's offset.start
  size_t _prefix_end_in_input =
    0;  // input merged into _buffer ends before this index
  size_t _delimiter_search_from = 0;  // find(next delimiter) starts here
  size_t _delimiter_size = 0;         // delimiter length
};

template<bool SingleChar, bool NoReplacement>
void ForwardPathHierarchyTokenizer<SingleChar, NoReplacement>::BeginFillValue(
  std::string_view data) {
  _data = data;
  _term_eof = data.empty();

  if (_term_eof) {
    return;
  }

  _delimiter_size = _options.delimiter.size();
  SDB_ASSERT(_delimiter_size > 0);

  _prefix_start_in_input = 0;

  // skip: walk left-to-right, advancing prefix start to skip leading tokens
  if (_options.skip > 0) {
    size_t skip_step_idx = 0;
    size_t scan_from = 0;
    // leading delimiter counts as one skip step: (/a/b/c equal a/b/c)
    // without +1, skip would line up wrong on paths that start with a delimiter
    size_t delimiter_steps_to_skip =
      _options.skip + (FindNextDelimiter(0) == 0);

    while (skip_step_idx < delimiter_steps_to_skip) {
      size_t next_delimiter_position = FindNextDelimiter(scan_from);
      if (next_delimiter_position == std::string_view::npos) {
        _term_eof = true;
        return;
      }

      _prefix_start_in_input = next_delimiter_position;
      scan_from = next_delimiter_position + _delimiter_size;
      ++skip_step_idx;
    }
  }

  _prefix_end_in_input = _prefix_start_in_input;
  _delimiter_search_from = _prefix_start_in_input;

  // for leading delimiter: /a/b/c
  // bump search cursor past it so the first segment token isn't empty
  if (IsDelimiterAt(_delimiter_search_from)) {
    _delimiter_search_from += _delimiter_size;
  }

  if constexpr (!NoReplacement) {
    // buffered path: delimiter != replacement
    _buffer.clear();
    _buffer.reserve(_options.buffer_size);
  }
}

template<bool SingleChar, bool NoReplacement>
bool ForwardPathHierarchyTokenizer<SingleChar, NoReplacement>::NextCore(
  bytes_view& term, uint32_t& start, uint32_t& end) {
  if (_term_eof) {
    return false;
  }

  // find next delimiter from search position
  size_t token_end_position = _data.size();
  size_t next_delimiter_position = FindNextDelimiter(_delimiter_search_from);

  if (next_delimiter_position != std::string_view::npos) {
    token_end_position = next_delimiter_position;
    _delimiter_search_from = next_delimiter_position + _delimiter_size;
  } else {
    _term_eof = true;
  }

  SDB_ASSERT(_prefix_start_in_input <= token_end_position);
  SDB_ASSERT(token_end_position <= _data.size());

  if constexpr (NoReplacement) {
    term = ViewCast<byte_type>(_data.substr(
      _prefix_start_in_input, token_end_position - _prefix_start_in_input));
  } else {
    SDB_ASSERT(_prefix_end_in_input <= token_end_position);

    // when first character isn't delimiter: a/b/c
    // if cursor is on delimiter, emit replacement then tail; else append input
    if (IsDelimiterAt(_prefix_end_in_input)) {
      _buffer.append(_options.replacement);

      size_t segment_len = _prefix_end_in_input + _delimiter_size;
      _buffer.append(_data.data() + segment_len,
                     token_end_position - segment_len);
    } else {
      _buffer.append(_data.data() + _prefix_end_in_input,
                     token_end_position - _prefix_end_in_input);
    }

    _prefix_end_in_input = token_end_position;
    term = ViewCast<byte_type>(std::string_view(_buffer));
  }

  start = static_cast<uint32_t>(_prefix_start_in_input);
  end = static_cast<uint32_t>(token_end_position);
  return true;
}

// Template parameters:
//   SingleChar: true if delimiter.size() == 1 (fast path)
//   NoReplacement: true if delimiter == replacement (zero-copy path)
template<bool SingleChar, bool NoReplacement>
class ReversePathHierarchyTokenizer final
  : public TypedTokenizer<
      ReversePathHierarchyTokenizer<SingleChar, NoReplacement>>,
    public PathHierarchyTokenizer {
 public:
  explicit ReversePathHierarchyTokenizer(Options&& options) noexcept
    : PathHierarchyTokenizer(std::move(options)) {}

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink) {
    BeginFillValue(value);
    bytes_view term;
    uint32_t start;
    uint32_t end;
    while (NextCore(term, start, end)) {
      if constexpr (NoReplacement) {
        sink.Emit<Layout>(MakeTermView(term), start, end);
      } else {
        sink.EmitInterned<Layout>(term, start, end);
      }
    }
    return true;
  }

 private:
  void BeginFillValue(std::string_view data);
  bool NextCore(bytes_view& term, uint32_t& start, uint32_t& end);
  // delimiter search
  size_t FindNextDelimiter(size_t from_position) const noexcept {
    if constexpr (SingleChar) {
      return _data.find(_options.delimiter[0], from_position);
    } else {
      return _data.find(_options.delimiter, from_position);
    }
  }

  // find previous delimiter (right-to-left search)
  size_t FindPreviousDelimiter(size_t search_end) const noexcept {
    if (search_end <= _delimiter_size) {
      return std::string_view::npos;
    }

    // search from one position before the end minus delimiter size to skip past
    // the current delimiter boundary and find the previous one
    size_t actual_search_end = search_end - _delimiter_size - 1;
    if constexpr (SingleChar) {
      return _data.rfind(_options.delimiter[0], actual_search_end);
    } else {
      return _data.rfind(_options.delimiter, actual_search_end);
    }
  }

  std::string _buffer;                 // buffer for NoReplacement = false mode
  size_t _suffix_start_in_buffer = 0;  // current token's left edge in _buffer
  size_t _suffix_start_in_input =
    0;  // current token's left edge in input (for offset.start)
  size_t _suffix_window_end =
    0;  // path ends here after skip-from-right (past last byte)
  size_t _delimiter_size = 0;  // delimiter length
};

template<bool SingleChar, bool NoReplacement>
void ReversePathHierarchyTokenizer<SingleChar, NoReplacement>::BeginFillValue(
  std::string_view data) {
  _data = data;
  _term_eof = data.empty();

  if (_term_eof) {
    return;
  }

  _delimiter_size = _options.delimiter.size();
  SDB_ASSERT(_delimiter_size > 0);

  // skip: walk right-to-left, dropping trailing segments
  size_t trimmed_window_end = data.size();
  for (size_t skip_idx = 0; skip_idx < _options.skip; ++skip_idx) {
    if (trimmed_window_end <= _delimiter_size) {
      _term_eof = true;
      return;
    }

    size_t rfind_delimiter_position = FindPreviousDelimiter(trimmed_window_end);
    if (rfind_delimiter_position == std::string_view::npos) {
      _term_eof = true;
      return;
    }
    trimmed_window_end = rfind_delimiter_position + _delimiter_size;
  }
  _suffix_window_end = trimmed_window_end;
  _suffix_start_in_input = 0;

  if constexpr (NoReplacement) {
    if (_suffix_window_end == 0) {
      _term_eof = true;
    }
    return;
  }

  // buffered path: delimiter != replacement
  // pre-build full buffer with all delimiters replaced (left-to-right)
  _buffer.clear();
  _buffer.reserve(_options.buffer_size);
  _suffix_start_in_buffer = 0;

  // scan left-to-right through window, joining segments with replacement
  size_t scan_from = 0;
  while (scan_from < _suffix_window_end) {
    size_t next_delimiter_position = FindNextDelimiter(scan_from);
    if (next_delimiter_position == std::string_view::npos) {
      SDB_ASSERT(scan_from <= _suffix_window_end);
      _buffer.append(data.data() + scan_from, _suffix_window_end - scan_from);
      break;
    }
    SDB_ASSERT(next_delimiter_position + _delimiter_size <= _suffix_window_end);

    _buffer.append(data.data() + scan_from,
                   next_delimiter_position - scan_from);
    _buffer.append(_options.replacement);

    scan_from = next_delimiter_position + _delimiter_size;
  }

  if (_buffer.size() == 0) {
    _term_eof = true;
  }
}

template<bool SingleChar, bool NoReplacement>
bool ReversePathHierarchyTokenizer<SingleChar, NoReplacement>::NextCore(
  bytes_view& term, uint32_t& start, uint32_t& end) {
  if (_term_eof) {
    return false;
  }

  SDB_ASSERT(_suffix_window_end <= _data.size());
  SDB_ASSERT(_suffix_start_in_input <= _suffix_window_end);

  if constexpr (NoReplacement) {
    term = ViewCast<byte_type>(_data.substr(
      _suffix_start_in_input, _suffix_window_end - _suffix_start_in_input));
  } else {
    SDB_ASSERT(_suffix_start_in_buffer <= _buffer.size());
    term = ViewCast<byte_type>(
      std::string_view(_buffer).substr(_suffix_start_in_buffer));
  }

  start = static_cast<uint32_t>(_suffix_start_in_input);
  end = static_cast<uint32_t>(_suffix_window_end);

  size_t next_delimiter_position = FindNextDelimiter(_suffix_start_in_input);

  if (next_delimiter_position == std::string_view::npos ||
      next_delimiter_position >= _suffix_window_end) {
    _term_eof = true;
  } else {
    SDB_ASSERT(next_delimiter_position >= _suffix_start_in_input);
    SDB_ASSERT(next_delimiter_position + _delimiter_size <= _suffix_window_end);

    size_t segment_len = next_delimiter_position - _suffix_start_in_input;
    _suffix_start_in_input = next_delimiter_position + _delimiter_size;

    if constexpr (!NoReplacement) {
      _suffix_start_in_buffer += segment_len + _options.replacement.size();
    }

    if (_suffix_start_in_input >= _suffix_window_end) {
      _term_eof = true;
    }
  }

  return true;
}

}  // namespace irs::analysis
namespace irs {

template<bool SingleChar, bool NoReplacement>
struct Type<analysis::ForwardPathHierarchyTokenizer<SingleChar, NoReplacement>>
  : Type<analysis::PathHierarchyTokenizer> {};

template<bool SingleChar, bool NoReplacement>
struct Type<analysis::ReversePathHierarchyTokenizer<SingleChar, NoReplacement>>
  : Type<analysis::PathHierarchyTokenizer> {};

}  // namespace irs
namespace irs::analysis {
namespace {

template<template<bool, bool> class Tok>
Tokenizer::ptr MakePathHierarchy(PathHierarchyTokenizer::Options&& opts,
                                 bool single_char, bool no_replacement) {
  const auto pick = [&]<bool SingleChar>() -> Tokenizer::ptr {
    if (no_replacement) {
      return std::make_unique<Tok<SingleChar, true>>(std::move(opts));
    }
    return std::make_unique<Tok<SingleChar, false>>(std::move(opts));
  };
  return single_char ? pick.template operator()<true>()
                     : pick.template operator()<false>();
}

}  // namespace

Tokenizer::ptr PathHierarchyTokenizer::Make(Options opts) {
  if (opts.delimiter.empty()) {
    THROW_SQL_ERROR(ERR_MSG("path_hierarchy: empty delimiter"));
  }

  if (opts.replacement.empty()) {
    opts.replacement = opts.delimiter;
  }

  const bool single_char = (opts.delimiter.size() == 1);
  const bool no_replacement = (opts.delimiter == opts.replacement);

  return opts.reverse ? MakePathHierarchy<ReversePathHierarchyTokenizer>(
                          std::move(opts), single_char, no_replacement)
                      : MakePathHierarchy<ForwardPathHierarchyTokenizer>(
                          std::move(opts), single_char, no_replacement);
}

}  // namespace irs::analysis
