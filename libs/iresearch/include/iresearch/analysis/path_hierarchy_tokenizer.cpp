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

#include <cstring>
#include <string_view>

#include "iresearch/analysis/tokenizer.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

PathHierarchyTokenizer::PathHierarchyTokenizer(Options&& options) noexcept
  : _options{std::move(options)} {}

enum class Direction : uint8_t {
  Forward,
  Reverse,
};

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
                     size_t prefix_start, size_t search_from,
                     std::string_view delimiter) {
  const auto emit = [&](size_t token_end_position) {
    SDB_ASSERT(prefix_start <= token_end_position);
    SDB_ASSERT(token_end_position <= value.size());
    sink.EmitSlice<Layout>(value.data(), value.data() + value.size(),
                           Offs{static_cast<uint32_t>(prefix_start),
                                static_cast<uint32_t>(token_end_position)});
  };
  for (size_t pos = FindNextDelim<SingleChar>(value, search_from, delimiter);
       pos != std::string_view::npos;
       pos =
         FindNextDelim<SingleChar>(value, pos + delimiter.size(), delimiter)) {
    emit(pos);
  }
  emit(value.size());
}

template<TokenLayout Layout, bool SingleChar>
void EmitReplacedPrefixes(TokenSink& sink, std::string_view value,
                          size_t prefix_start, size_t search_from,
                          size_t ndelims, std::string_view delimiter,
                          std::string_view replacement) {
  const size_t delimiter_size = delimiter.size();
  const size_t replacement_size = replacement.size();
  size_t tail_ndelims = 0;
  for (size_t pos = FindNextDelim<SingleChar>(value, search_from, delimiter);
       pos != std::string_view::npos;
       pos =
         FindNextDelim<SingleChar>(value, pos + delimiter_size, delimiter)) {
    ++tail_ndelims;
  }
  const size_t total_ndelims = tail_ndelims + ndelims;
  const size_t total_converted = (value.size() - prefix_start) -
                                 total_ndelims * delimiter_size +
                                 total_ndelims * replacement_size;
  size_t built_size = 0;
  size_t built_input_end = prefix_start;
  bool delta_starts_with_delim = ndelims == 1;
  sink.EmitK<Layout>(
    tail_ndelims + 1, total_converted,
    [&](byte_type* mem) IRS_FORCE_INLINE {
      CopyReplaced<SingleChar>(value, prefix_start, built_input_end, delimiter,
                               replacement, mem);
    },
    [&](size_t, byte_type* mem) IRS_FORCE_INLINE {
      size_t token_end_position = value.size();
      const size_t next_delim =
        FindNextDelim<SingleChar>(value, search_from, delimiter);
      const bool more = next_delim != std::string_view::npos;
      if (more) {
        token_end_position = next_delim;
        search_from = next_delim + delimiter_size;
      }
      SDB_ASSERT(prefix_start <= token_end_position);
      SDB_ASSERT(token_end_position <= value.size());
      const size_t converted_size = (token_end_position - prefix_start) -
                                    ndelims * delimiter_size +
                                    ndelims * replacement_size;
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
      return EmitKSlotOffs{0, static_cast<uint32_t>(converted_size),
                           Offs{static_cast<uint32_t>(prefix_start),
                                static_cast<uint32_t>(token_end_position)}};
    });
}

template<TokenLayout Layout, bool SingleChar>
void EmitSuffixViews(TokenSink& sink, std::string_view value, size_t window_end,
                     std::string_view delimiter) {
  SDB_ASSERT(window_end <= value.size());
  size_t suffix_start = 0;
  for (;;) {
    SDB_ASSERT(suffix_start <= window_end);

    sink.EmitSlice<Layout>(value.data(), value.data() + value.size(),
                           Offs{static_cast<uint32_t>(suffix_start),
                                static_cast<uint32_t>(window_end)});

    const size_t next_delim =
      FindNextDelim<SingleChar>(value, suffix_start, delimiter);
    if (next_delim == std::string_view::npos || next_delim >= window_end) {
      return;
    }
    suffix_start = next_delim + delimiter.size();
    if (suffix_start >= window_end) {
      return;
    }
  }
}

template<TokenLayout Layout, bool SingleChar>
void EmitReplacedSuffixes(TokenSink& sink, std::string_view value,
                          size_t window_end, std::string_view delimiter,
                          std::string_view replacement) {
  const size_t delimiter_size = delimiter.size();
  const size_t replacement_size = replacement.size();
  size_t suffix_start = 0;
  size_t ndelims = 0;
  size_t last_delim = std::string_view::npos;
  for (size_t pos = FindNextDelim<SingleChar>(value, 0, delimiter);
       pos < window_end && pos + delimiter_size <= window_end;
       pos =
         FindNextDelim<SingleChar>(value, pos + delimiter_size, delimiter)) {
    last_delim = pos;
    ++ndelims;
  }
  const bool trailing =
    ndelims != 0 && last_delim + delimiter_size == window_end;
  const auto converted_len = [&]() IRS_FORCE_INLINE {
    return (window_end - suffix_start) - ndelims * delimiter_size +
           ndelims * replacement_size;
  };
  size_t staged_converted = 0;
  sink.EmitK<Layout>(
    ndelims + 1 - trailing, converted_len(),
    [&](byte_type* mem) IRS_FORCE_INLINE {
      staged_converted = converted_len();
      CopyReplaced<SingleChar>(value, suffix_start, window_end, delimiter,
                               replacement, mem);
    },
    [&](size_t, byte_type*) IRS_FORCE_INLINE {
      SDB_ASSERT(suffix_start <= window_end);
      const size_t converted_size = converted_len();
      const Offs offs{static_cast<uint32_t>(suffix_start),
                      static_cast<uint32_t>(window_end)};
      const auto begin =
        static_cast<uint32_t>(staged_converted - converted_size);
      const size_t next_delim =
        FindNextDelim<SingleChar>(value, suffix_start, delimiter);
      if (next_delim != std::string_view::npos &&
          next_delim + delimiter_size <= window_end) {
        suffix_start = next_delim + delimiter_size;
        --ndelims;
      }
      return EmitKSlotOffs{begin, static_cast<uint32_t>(begin + converted_size),
                           offs};
    });
}

}  // namespace

template<Direction D, bool SingleChar, bool NoReplacement>
class PathHierarchyTokenizerImpl final
  : public TypedTokenizer<
      PathHierarchyTokenizerImpl<D, SingleChar, NoReplacement>>,
    public PathHierarchyTokenizer {
 public:
  explicit PathHierarchyTokenizerImpl(Options&& options) noexcept
    : PathHierarchyTokenizer(std::move(options)) {}
  TokenTraits Traits() const noexcept final {
    return {.offsets = true, .stable = NoReplacement};
  }

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  template<TokenLayout Layout>
  IRS_FORCE_INLINE void ForwardFill(std::string_view value,
                                    std::string_view delimiter,
                                    TokenSink& sink);

  template<TokenLayout Layout>
  IRS_FORCE_INLINE void ReverseFill(std::string_view value,
                                    std::string_view delimiter,
                                    TokenSink& sink);
};

template<Direction D, bool SingleChar, bool NoReplacement>
template<TokenLayout Layout>
bool PathHierarchyTokenizerImpl<D, SingleChar, NoReplacement>::DoFill(
  duckdb::string_t raw, TokenSink& sink) {
  const std::string_view value{raw.GetData(), raw.GetSize()};
  if (value.empty()) {
    return true;
  }

  const std::string_view delimiter{_options.delimiter};
  SDB_ASSERT(!delimiter.empty());

  if constexpr (D == Direction::Forward) {
    ForwardFill<Layout>(value, delimiter, sink);
  } else {
    ReverseFill<Layout>(value, delimiter, sink);
  }
  return true;
}

template<Direction D, bool SingleChar, bool NoReplacement>
template<TokenLayout Layout>
IRS_FORCE_INLINE void
PathHierarchyTokenizerImpl<D, SingleChar, NoReplacement>::ForwardFill(
  std::string_view value, std::string_view delimiter, TokenSink& sink) {
  size_t prefix_start = 0;

  if (_options.skip > 0) {
    size_t scan_from = 0;
    const size_t skip_steps =
      _options.skip + IsDelimAt<SingleChar>(value, 0, delimiter);
    for (size_t i = 0; i < skip_steps; ++i) {
      const size_t next_delim =
        FindNextDelim<SingleChar>(value, scan_from, delimiter);
      if (next_delim == std::string_view::npos) {
        return;
      }
      prefix_start = next_delim;
      scan_from = next_delim + delimiter.size();
    }
  }

  size_t search_from = prefix_start;
  size_t ndelims = 0;

  if (IsDelimAt<SingleChar>(value, search_from, delimiter)) {
    search_from += delimiter.size();
    ndelims = 1;
  }

  if constexpr (NoReplacement) {
    EmitPrefixViews<Layout, SingleChar>(sink, value, prefix_start, search_from,
                                        delimiter);
  } else {
    const std::string_view replacement{_options.replacement};
    EmitReplacedPrefixes<Layout, SingleChar>(
      sink, value, prefix_start, search_from, ndelims, delimiter, replacement);
  }
}

template<Direction D, bool SingleChar, bool NoReplacement>
template<TokenLayout Layout>
IRS_FORCE_INLINE void
PathHierarchyTokenizerImpl<D, SingleChar, NoReplacement>::ReverseFill(
  std::string_view value, std::string_view delimiter, TokenSink& sink) {
  size_t window_end = value.size();
  for (size_t skip_idx = 0; skip_idx < _options.skip; ++skip_idx) {
    size_t prev_delim = FindPrevDelim<SingleChar>(value, window_end, delimiter);
    if (prev_delim == std::string_view::npos) {
      return;
    }
    window_end = prev_delim + delimiter.size();
  }

  if constexpr (NoReplacement) {
    EmitSuffixViews<Layout, SingleChar>(sink, value, window_end, delimiter);
  } else {
    const std::string_view replacement{_options.replacement};
    EmitReplacedSuffixes<Layout, SingleChar>(sink, value, window_end, delimiter,
                                             replacement);
  }
}

}  // namespace irs::analysis
namespace irs {

template<analysis::Direction D, bool SingleChar, bool NoReplacement>
struct Type<analysis::PathHierarchyTokenizerImpl<D, SingleChar, NoReplacement>>
  : Type<analysis::PathHierarchyTokenizer> {};

}  // namespace irs
namespace irs::analysis {
namespace {

template<Direction D>
Tokenizer::ptr MakePathHierarchy(PathHierarchyTokenizer::Options&& opts,
                                 bool single_char, bool no_replacement) {
  const auto pick = [&]<bool SingleChar>() -> Tokenizer::ptr {
    if (no_replacement) {
      return std::make_unique<PathHierarchyTokenizerImpl<D, SingleChar, true>>(
        std::move(opts));
    }
    return std::make_unique<PathHierarchyTokenizerImpl<D, SingleChar, false>>(
      std::move(opts));
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

  return opts.reverse ? MakePathHierarchy<Direction::Reverse>(
                          std::move(opts), single_char, no_replacement)
                      : MakePathHierarchy<Direction::Forward>(
                          std::move(opts), single_char, no_replacement);
}

}  // namespace irs::analysis
