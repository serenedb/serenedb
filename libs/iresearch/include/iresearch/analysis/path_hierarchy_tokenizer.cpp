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

#include <cstring>
#include <string_view>

#include "iresearch/analysis/term_view.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

PathHierarchyTokenizer::PathHierarchyTokenizer(Options&& options) noexcept
  : _options{std::move(options)} {}

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
  TokenTraits Traits() const noexcept final { return {.offsets = true}; }

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  // delimiter search
  size_t FindNextDelimiter(std::string_view data,
                           size_t from_position) const noexcept {
    if constexpr (SingleChar) {
      return data.find(_options.delimiter[0], from_position);
    } else {
      return data.find(_options.delimiter, from_position);
    }
  }

  // check if substring at position matches delimiter
  bool IsDelimiterAt(std::string_view data, size_t position) const noexcept {
    if constexpr (SingleChar) {
      return position < data.size() && data[position] == _options.delimiter[0];
    } else {
      const size_t delimiter_size = _options.delimiter.size();
      return position + delimiter_size <= data.size() &&
             data.substr(position, delimiter_size) == _options.delimiter;
    }
  }

  // copy data[from, to) into out, delimiters replaced
  void CopyReplaced(std::string_view data, size_t from, size_t to,
                    byte_type* out) const noexcept {
    auto* dst = reinterpret_cast<char*>(out);
    size_t pos = from;
    while (pos < to) {
      const size_t next = FindNextDelimiter(data, pos);
      if (next >= to) {
        std::memcpy(dst, data.data() + pos, to - pos);
        return;
      }
      std::memcpy(dst, data.data() + pos, next - pos);
      dst += next - pos;
      std::memcpy(dst, _options.replacement.data(),
                  _options.replacement.size());
      dst += _options.replacement.size();
      pos = next + _options.delimiter.size();
    }
  }
};

template<bool SingleChar, bool NoReplacement>
template<TokenLayout Layout>
bool ForwardPathHierarchyTokenizer<SingleChar, NoReplacement>::DoFill(
  duckdb::string_t raw, TokenSink& sink) {
  const std::string_view value{raw.GetData(), raw.GetSize()};
  if (value.empty()) {
    return true;
  }

  const size_t delimiter_size = _options.delimiter.size();
  SDB_ASSERT(delimiter_size > 0);

  // left edge of prefix; every token's offset.start
  size_t prefix_start_in_input = 0;

  // skip: walk left-to-right, advancing prefix start to skip leading tokens
  if (_options.skip > 0) {
    size_t skip_step_idx = 0;
    size_t scan_from = 0;
    // leading delimiter counts as one skip step: (/a/b/c equal a/b/c)
    // without +1, skip would line up wrong on paths that start with a delimiter
    size_t delimiter_steps_to_skip =
      _options.skip + (FindNextDelimiter(value, 0) == 0);

    while (skip_step_idx < delimiter_steps_to_skip) {
      size_t next_delimiter_position = FindNextDelimiter(value, scan_from);
      if (next_delimiter_position == std::string_view::npos) {
        return true;
      }

      prefix_start_in_input = next_delimiter_position;
      scan_from = next_delimiter_position + delimiter_size;
      ++skip_step_idx;
    }
  }

  // find(next delimiter) starts here
  size_t delimiter_search_from = prefix_start_in_input;
  // delimiters inside the current token's input range
  size_t ndelims = 0;

  // for leading delimiter: /a/b/c
  // bump search cursor past it so the first segment token isn't empty
  if (IsDelimiterAt(value, delimiter_search_from)) {
    delimiter_search_from += delimiter_size;
    ndelims = 1;
  }

  const size_t replacement_size = _options.replacement.size();

  if constexpr (NoReplacement) {
    bool more = true;
    while (more) {
      size_t token_end_position = value.size();
      const size_t next_delimiter_position =
        FindNextDelimiter(value, delimiter_search_from);

      more = next_delimiter_position != std::string_view::npos;
      if (more) {
        token_end_position = next_delimiter_position;
        delimiter_search_from = next_delimiter_position + delimiter_size;
      }

      SDB_ASSERT(prefix_start_in_input <= token_end_position);
      SDB_ASSERT(token_end_position <= value.size());

      sink.Emit<Layout>(
        MakeTermView(
          value.data() + prefix_start_in_input,
          static_cast<uint32_t>(token_end_position - prefix_start_in_input),
          value.data() + value.size()),
        Offs{static_cast<uint32_t>(prefix_start_in_input),
             static_cast<uint32_t>(token_end_position)});
    }
  } else {
    // every token is a prefix of the fully converted path: the sink stages
    // one block per wave (restage rebuilds the committed prefix), each gen
    // call scans to the next delimiter and appends only its token's delta;
    // the emit count is exact -- one per delimiter past the cursor, plus the
    // full-path token
    size_t tail_ndelims = 0;
    for (size_t pos = FindNextDelimiter(value, delimiter_search_from);
         pos != std::string_view::npos;
         pos = FindNextDelimiter(value, pos + delimiter_size)) {
      ++tail_ndelims;
    }
    const size_t total_ndelims = tail_ndelims + ndelims;
    const size_t total_converted = (value.size() - prefix_start_in_input) -
                                   total_ndelims * delimiter_size +
                                   total_ndelims * replacement_size;
    size_t built_size = 0;
    size_t built_input_end = prefix_start_in_input;
    sink.EmitK<Layout>(
      tail_ndelims + 1, total_converted,
      [&](byte_type* mem, size_t) IRS_FORCE_INLINE {
        CopyReplaced(value, prefix_start_in_input, built_input_end, mem);
      },
      [&](size_t, byte_type* mem) IRS_FORCE_INLINE {
        size_t token_end_position = value.size();
        const size_t next_delimiter_position =
          FindNextDelimiter(value, delimiter_search_from);
        const bool more =
          next_delimiter_position != std::string_view::npos;
        if (more) {
          token_end_position = next_delimiter_position;
          delimiter_search_from = next_delimiter_position + delimiter_size;
        }
        SDB_ASSERT(prefix_start_in_input <= token_end_position);
        SDB_ASSERT(token_end_position <= value.size());
        const size_t converted_size =
          (token_end_position - prefix_start_in_input) -
          ndelims * delimiter_size + ndelims * replacement_size;
        CopyReplaced(value, built_input_end, token_end_position,
                     mem + built_size);
        built_size = converted_size;
        built_input_end = token_end_position;
        ndelims += more;
        return EmitKSlot{0, static_cast<uint32_t>(converted_size),
                         Offs{static_cast<uint32_t>(prefix_start_in_input),
                              static_cast<uint32_t>(token_end_position)}};
      });
  }
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
  TokenTraits Traits() const noexcept final { return {.offsets = true}; }

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  // delimiter search
  size_t FindNextDelimiter(std::string_view data,
                           size_t from_position) const noexcept {
    if constexpr (SingleChar) {
      return data.find(_options.delimiter[0], from_position);
    } else {
      return data.find(_options.delimiter, from_position);
    }
  }

  // find previous delimiter (right-to-left search)
  size_t FindPreviousDelimiter(std::string_view data,
                               size_t search_end) const noexcept {
    const size_t delimiter_size = _options.delimiter.size();
    if (search_end <= delimiter_size) {
      return std::string_view::npos;
    }

    // search from one position before the end minus delimiter size to skip
    // past the current delimiter boundary and find the previous one
    size_t actual_search_end = search_end - delimiter_size - 1;
    if constexpr (SingleChar) {
      return data.rfind(_options.delimiter[0], actual_search_end);
    } else {
      return data.rfind(_options.delimiter, actual_search_end);
    }
  }

  // copy data[from, to) into out, delimiters replaced
  void CopyReplaced(std::string_view data, size_t from, size_t to,
                    byte_type* out) const noexcept {
    auto* dst = reinterpret_cast<char*>(out);
    size_t pos = from;
    while (pos < to) {
      const size_t next = FindNextDelimiter(data, pos);
      if (next >= to) {
        std::memcpy(dst, data.data() + pos, to - pos);
        return;
      }
      std::memcpy(dst, data.data() + pos, next - pos);
      dst += next - pos;
      std::memcpy(dst, _options.replacement.data(),
                  _options.replacement.size());
      dst += _options.replacement.size();
      pos = next + _options.delimiter.size();
    }
  }
};

template<bool SingleChar, bool NoReplacement>
template<TokenLayout Layout>
bool ReversePathHierarchyTokenizer<SingleChar, NoReplacement>::DoFill(
  duckdb::string_t raw, TokenSink& sink) {
  const std::string_view value{raw.GetData(), raw.GetSize()};
  if (value.empty()) {
    return true;
  }

  const size_t delimiter_size = _options.delimiter.size();
  SDB_ASSERT(delimiter_size > 0);

  // skip: walk right-to-left, dropping trailing segments
  // path ends here after skip-from-right (past last byte)
  size_t suffix_window_end = value.size();
  for (size_t skip_idx = 0; skip_idx < _options.skip; ++skip_idx) {
    if (suffix_window_end <= delimiter_size) {
      return true;
    }

    size_t rfind_delimiter_position =
      FindPreviousDelimiter(value, suffix_window_end);
    if (rfind_delimiter_position == std::string_view::npos) {
      return true;
    }
    suffix_window_end = rfind_delimiter_position + delimiter_size;
  }

  // current token's left edge in input (for offset.start)
  size_t suffix_start_in_input = 0;
  const size_t replacement_size = _options.replacement.size();

  if constexpr (NoReplacement) {
    bool more = true;
    while (more) {
      SDB_ASSERT(suffix_window_end <= value.size());
      SDB_ASSERT(suffix_start_in_input <= suffix_window_end);

      sink.Emit<Layout>(
        MakeTermView(
          value.data() + suffix_start_in_input,
          static_cast<uint32_t>(suffix_window_end - suffix_start_in_input),
          value.data() + value.size()),
        Offs{static_cast<uint32_t>(suffix_start_in_input),
             static_cast<uint32_t>(suffix_window_end)});

      const size_t next_delimiter_position =
        FindNextDelimiter(value, suffix_start_in_input);
      if (next_delimiter_position == std::string_view::npos ||
          next_delimiter_position >= suffix_window_end) {
        more = false;
      } else {
        suffix_start_in_input = next_delimiter_position + delimiter_size;
        if (suffix_start_in_input >= suffix_window_end) {
          more = false;
        }
      }
    }
  } else {
    // every token is a suffix of the current staged window: the sink stages
    // the window per wave, gens emit its converted suffixes and advance the
    // left edge; a delimiter ending exactly at the window end yields no
    // extra (empty) token
    size_t ndelims = 0;
    size_t last_delimiter_position = std::string_view::npos;
    for (size_t pos = FindNextDelimiter(value, 0); pos < suffix_window_end;
         pos = FindNextDelimiter(value, pos + delimiter_size)) {
      SDB_ASSERT(pos + delimiter_size <= suffix_window_end);
      last_delimiter_position = pos;
      ++ndelims;
    }
    const bool trailing =
      ndelims != 0 &&
      last_delimiter_position + delimiter_size == suffix_window_end;
    size_t staged_converted = 0;
    sink.EmitK<Layout>(
      ndelims + 1 - trailing,
      (suffix_window_end - suffix_start_in_input) -
        ndelims * delimiter_size + ndelims * replacement_size,
      [&](byte_type* mem, size_t) IRS_FORCE_INLINE {
        staged_converted = (suffix_window_end - suffix_start_in_input) -
                           ndelims * delimiter_size +
                           ndelims * replacement_size;
        CopyReplaced(value, suffix_start_in_input, suffix_window_end, mem);
      },
      [&](size_t, byte_type*) IRS_FORCE_INLINE {
        SDB_ASSERT(suffix_start_in_input <= suffix_window_end);
        const size_t converted_size =
          (suffix_window_end - suffix_start_in_input) -
          ndelims * delimiter_size + ndelims * replacement_size;
        const Offs offs{static_cast<uint32_t>(suffix_start_in_input),
                        static_cast<uint32_t>(suffix_window_end)};
        const auto begin =
          static_cast<uint32_t>(staged_converted - converted_size);
        const size_t next_delimiter_position =
          FindNextDelimiter(value, suffix_start_in_input);
        if (next_delimiter_position != std::string_view::npos &&
            next_delimiter_position < suffix_window_end) {
          SDB_ASSERT(next_delimiter_position + delimiter_size <=
                     suffix_window_end);
          suffix_start_in_input = next_delimiter_position + delimiter_size;
          --ndelims;
        }
        return EmitKSlot{
          begin, static_cast<uint32_t>(begin + converted_size), offs};
      });
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
