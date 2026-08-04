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

namespace {

template<bool SingleChar>
size_t FindNextDelim(std::string_view data, size_t from,
                     std::string_view delim) noexcept {
  if constexpr (SingleChar) {
    return data.find(delim[0], from);
  } else {
    return data.find(delim, from);
  }
}

// find previous delimiter strictly before the one ending at `search_end`
// (right-to-left search)
template<bool SingleChar>
size_t FindPrevDelim(std::string_view data, size_t search_end,
                     std::string_view delim) noexcept {
  if (search_end <= delim.size()) {
    return std::string_view::npos;
  }
  const size_t actual_search_end = search_end - delim.size() - 1;
  if constexpr (SingleChar) {
    return data.rfind(delim[0], actual_search_end);
  } else {
    return data.rfind(delim, actual_search_end);
  }
}

template<bool SingleChar>
bool IsDelimAt(std::string_view data, size_t pos,
               std::string_view delim) noexcept {
  if constexpr (SingleChar) {
    return pos < data.size() && data[pos] == delim[0];
  } else {
    return pos + delim.size() <= data.size() &&
           data.substr(pos, delim.size()) == delim;
  }
}

// copy data[from, to) into out, delimiters replaced
template<bool SingleChar>
void CopyReplaced(std::string_view data, size_t from, size_t to,
                  std::string_view delim, std::string_view replacement,
                  byte_type* out) noexcept {
  auto* dst = reinterpret_cast<char*>(out);
  size_t pos = from;
  while (pos < to) {
    const size_t next = FindNextDelim<SingleChar>(data, pos, delim);
    if (next >= to || next + delim.size() > to) {
      std::memcpy(dst, data.data() + pos, to - pos);
      return;
    }
    std::memcpy(dst, data.data() + pos, next - pos);
    dst += next - pos;
    std::memcpy(dst, replacement.data(), replacement.size());
    dst += replacement.size();
    pos = next + delim.size();
  }
}

template<TokenLayout Layout, bool SingleChar>
void EmitPrefixViews(TokenSink& sink, std::string_view value,
                     size_t prefix_start_in_input, size_t delimiter_search_from,
                     std::string_view delimiter) {
  bool more = true;
  while (more) {
    size_t token_end_position = value.size();
    const size_t next_delimiter_position =
      FindNextDelim<SingleChar>(value, delimiter_search_from, delimiter);

    more = next_delimiter_position != std::string_view::npos;
    if (more) {
      token_end_position = next_delimiter_position;
      delimiter_search_from = next_delimiter_position + delimiter.size();
    }

    SDB_ASSERT(prefix_start_in_input <= token_end_position);
    SDB_ASSERT(token_end_position <= value.size());

    sink.Emit<Layout>(MakeTermView(value.data() + prefix_start_in_input,
                                   static_cast<uint32_t>(token_end_position -
                                                         prefix_start_in_input),
                                   value.data() + value.size()),
                      Offs{static_cast<uint32_t>(prefix_start_in_input),
                           static_cast<uint32_t>(token_end_position)});
  }
}

// every token is a prefix of the fully converted path: the sink stages
// one block per wave (restage rebuilds the committed prefix), each gen
// call scans to the next delimiter and appends only its token's delta;
// the emit count is exact -- one per delimiter past the cursor, plus the
// full-path token
template<TokenLayout Layout, bool SingleChar>
void EmitReplacedPrefixes(TokenSink& sink, std::string_view value,
                          size_t prefix_start_in_input,
                          size_t delimiter_search_from, size_t ndelims,
                          std::string_view delimiter,
                          std::string_view replacement) {
  const size_t delimiter_size = delimiter.size();
  const size_t replacement_size = replacement.size();
  size_t tail_ndelims = 0;
  for (size_t pos =
         FindNextDelim<SingleChar>(value, delimiter_search_from, delimiter);
       pos != std::string_view::npos;
       pos =
         FindNextDelim<SingleChar>(value, pos + delimiter_size, delimiter)) {
    ++tail_ndelims;
  }
  const size_t total_ndelims = tail_ndelims + ndelims;
  const size_t total_converted = (value.size() - prefix_start_in_input) -
                                 total_ndelims * delimiter_size +
                                 total_ndelims * replacement_size;
  size_t built_size = 0;
  size_t built_input_end = prefix_start_in_input;
  bool delta_starts_with_delim = ndelims == 1;
  sink.EmitK<Layout>(
    tail_ndelims + 1, total_converted,
    [&](byte_type* mem, size_t) IRS_FORCE_INLINE {
      CopyReplaced<SingleChar>(value, prefix_start_in_input, built_input_end,
                               delimiter, replacement, mem);
    },
    [&](size_t, byte_type* mem) IRS_FORCE_INLINE {
      size_t token_end_position = value.size();
      const size_t next_delimiter_position =
        FindNextDelim<SingleChar>(value, delimiter_search_from, delimiter);
      const bool more = next_delimiter_position != std::string_view::npos;
      if (more) {
        token_end_position = next_delimiter_position;
        delimiter_search_from = next_delimiter_position + delimiter_size;
      }
      SDB_ASSERT(prefix_start_in_input <= token_end_position);
      SDB_ASSERT(token_end_position <= value.size());
      const size_t converted_size =
        (token_end_position - prefix_start_in_input) -
        ndelims * delimiter_size + ndelims * replacement_size;
      auto* dst = mem + built_size;
      size_t src = built_input_end;
      if (delta_starts_with_delim) {
        SDB_ASSERT(IsDelimAt<SingleChar>(value, src, delimiter));
        std::memcpy(dst, replacement.data(), replacement_size);
        dst += replacement_size;
        src += delimiter_size;
      }
      std::memcpy(dst, value.data() + src, token_end_position - src);
      built_size = converted_size;
      built_input_end = token_end_position;
      ndelims += more;
      delta_starts_with_delim = more;
      return EmitKSlot{0, static_cast<uint32_t>(converted_size),
                       Offs{static_cast<uint32_t>(prefix_start_in_input),
                            static_cast<uint32_t>(token_end_position)}};
    });
}

template<TokenLayout Layout, bool SingleChar>
void EmitSuffixViews(TokenSink& sink, std::string_view value,
                     size_t suffix_window_end, std::string_view delimiter) {
  size_t suffix_start_in_input = 0;
  bool more = true;
  while (more) {
    SDB_ASSERT(suffix_window_end <= value.size());
    SDB_ASSERT(suffix_start_in_input <= suffix_window_end);

    sink.Emit<Layout>(MakeTermView(value.data() + suffix_start_in_input,
                                   static_cast<uint32_t>(suffix_window_end -
                                                         suffix_start_in_input),
                                   value.data() + value.size()),
                      Offs{static_cast<uint32_t>(suffix_start_in_input),
                           static_cast<uint32_t>(suffix_window_end)});

    const size_t next_delimiter_position =
      FindNextDelim<SingleChar>(value, suffix_start_in_input, delimiter);
    if (next_delimiter_position == std::string_view::npos ||
        next_delimiter_position >= suffix_window_end) {
      more = false;
    } else {
      suffix_start_in_input = next_delimiter_position + delimiter.size();
      if (suffix_start_in_input >= suffix_window_end) {
        more = false;
      }
    }
  }
}

// every token is a suffix of the current staged window: the sink stages
// the window per wave, gens emit its converted suffixes and advance the
// left edge; a delimiter ending exactly at the window end yields no
// extra (empty) token
template<TokenLayout Layout, bool SingleChar>
void EmitReplacedSuffixes(TokenSink& sink, std::string_view value,
                          size_t suffix_window_end, std::string_view delimiter,
                          std::string_view replacement) {
  const size_t delimiter_size = delimiter.size();
  const size_t replacement_size = replacement.size();
  size_t suffix_start_in_input = 0;
  size_t ndelims = 0;
  size_t last_delimiter_position = std::string_view::npos;
  for (size_t pos = FindNextDelim<SingleChar>(value, 0, delimiter);
       pos < suffix_window_end && pos + delimiter_size <= suffix_window_end;
       pos =
         FindNextDelim<SingleChar>(value, pos + delimiter_size, delimiter)) {
    last_delimiter_position = pos;
    ++ndelims;
  }
  const bool trailing =
    ndelims != 0 &&
    last_delimiter_position + delimiter_size == suffix_window_end;
  size_t staged_converted = 0;
  sink.EmitK<Layout>(
    ndelims + 1 - trailing,
    (suffix_window_end - suffix_start_in_input) - ndelims * delimiter_size +
      ndelims * replacement_size,
    [&](byte_type* mem, size_t) IRS_FORCE_INLINE {
      staged_converted = (suffix_window_end - suffix_start_in_input) -
                         ndelims * delimiter_size + ndelims * replacement_size;
      CopyReplaced<SingleChar>(value, suffix_start_in_input, suffix_window_end,
                               delimiter, replacement, mem);
    },
    [&](size_t, byte_type*) IRS_FORCE_INLINE {
      SDB_ASSERT(suffix_start_in_input <= suffix_window_end);
      const size_t converted_size =
        (suffix_window_end - suffix_start_in_input) - ndelims * delimiter_size +
        ndelims * replacement_size;
      const Offs offs{static_cast<uint32_t>(suffix_start_in_input),
                      static_cast<uint32_t>(suffix_window_end)};
      const auto begin =
        static_cast<uint32_t>(staged_converted - converted_size);
      const size_t next_delimiter_position =
        FindNextDelim<SingleChar>(value, suffix_start_in_input, delimiter);
      if (next_delimiter_position != std::string_view::npos &&
          next_delimiter_position + delimiter_size <= suffix_window_end) {
        suffix_start_in_input = next_delimiter_position + delimiter_size;
        --ndelims;
      }
      return EmitKSlot{begin, static_cast<uint32_t>(begin + converted_size),
                       offs};
    });
}

}  // namespace

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
};

template<bool SingleChar, bool NoReplacement>
template<TokenLayout Layout>
bool ForwardPathHierarchyTokenizer<SingleChar, NoReplacement>::DoFill(
  duckdb::string_t raw, TokenSink& sink) {
  const std::string_view value{raw.GetData(), raw.GetSize()};
  if (value.empty()) {
    return true;
  }

  const std::string_view delimiter{_options.delimiter};
  const std::string_view replacement{_options.replacement};
  const size_t delimiter_size = delimiter.size();
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
      _options.skip + (FindNextDelim<SingleChar>(value, 0, delimiter) == 0);

    while (skip_step_idx < delimiter_steps_to_skip) {
      size_t next_delimiter_position =
        FindNextDelim<SingleChar>(value, scan_from, delimiter);
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
  if (IsDelimAt<SingleChar>(value, delimiter_search_from, delimiter)) {
    delimiter_search_from += delimiter_size;
    ndelims = 1;
  }

  if constexpr (NoReplacement) {
    EmitPrefixViews<Layout, SingleChar>(sink, value, prefix_start_in_input,
                                        delimiter_search_from, delimiter);
  } else {
    EmitReplacedPrefixes<Layout, SingleChar>(sink, value, prefix_start_in_input,
                                             delimiter_search_from, ndelims,
                                             delimiter, replacement);
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
};

template<bool SingleChar, bool NoReplacement>
template<TokenLayout Layout>
bool ReversePathHierarchyTokenizer<SingleChar, NoReplacement>::DoFill(
  duckdb::string_t raw, TokenSink& sink) {
  const std::string_view value{raw.GetData(), raw.GetSize()};
  if (value.empty()) {
    return true;
  }

  const std::string_view delimiter{_options.delimiter};
  const std::string_view replacement{_options.replacement};
  const size_t delimiter_size = delimiter.size();
  SDB_ASSERT(delimiter_size > 0);

  // skip: walk right-to-left, dropping trailing segments
  // path ends here after skip-from-right (past last byte)
  size_t suffix_window_end = value.size();
  for (size_t skip_idx = 0; skip_idx < _options.skip; ++skip_idx) {
    if (suffix_window_end <= delimiter_size) {
      return true;
    }

    size_t rfind_delimiter_position =
      FindPrevDelim<SingleChar>(value, suffix_window_end, delimiter);
    if (rfind_delimiter_position == std::string_view::npos) {
      return true;
    }
    suffix_window_end = rfind_delimiter_position + delimiter_size;
  }

  if constexpr (NoReplacement) {
    EmitSuffixViews<Layout, SingleChar>(sink, value, suffix_window_end,
                                        delimiter);
  } else {
    EmitReplacedSuffixes<Layout, SingleChar>(sink, value, suffix_window_end,
                                             delimiter, replacement);
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
