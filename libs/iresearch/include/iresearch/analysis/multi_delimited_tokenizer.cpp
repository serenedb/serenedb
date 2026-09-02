////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include "multi_delimited_tokenizer.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <functional>
#include <string_view>

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

struct NoDelimFinder {
  template<typename OnDelim>
  IRS_FORCE_INLINE void ForEachDelim(bytes_view, OnDelim&&) const noexcept {}
};

struct OneCharFinder {
  byte_type delim;

  template<typename OnDelim>
  IRS_FORCE_INLINE void ForEachDelim(bytes_view data,
                                     OnDelim&& on_delim) const {
    classify::DrainClassified(
      data.data(), data.size(), true,
      [&](const byte_type* block)
        IRS_FORCE_INLINE { return classify::ClassifyEqBlock(block, delim); },
      [&](byte_type c) IRS_FORCE_INLINE { return c == delim; },
      [&](size_t pos) IRS_FORCE_INLINE { on_delim(pos, size_t{1}); });
  }
};

struct ManyCharsFinder {
  static constexpr size_t kMaxBlockDelims = 8;

  explicit ManyCharsFinder(const std::vector<bstring>& delimiters) {
    for (const auto& delim : delimiters) {
      SDB_ASSERT(delim.size() == 1);
      bytes.Add(delim[0]);
      if (ndelims < kMaxBlockDelims) {
        delims[ndelims] = delim[0];
      }
      ++ndelims;
    }
  }

  template<typename OnDelim>
  IRS_FORCE_INLINE void ForEachDelim(bytes_view data,
                                     OnDelim&& on_delim) const {
    classify::DrainClassified(
      data.data(), data.size(), ndelims <= kMaxBlockDelims,
      [&](const byte_type* block) IRS_FORCE_INLINE {
        return classify::ClassifyAnyEqBlock(block, {delims.data(), ndelims});
      },
      [&](byte_type c) IRS_FORCE_INLINE { return bytes.Contains(c); },
      [&](size_t pos) IRS_FORCE_INLINE { on_delim(pos, size_t{1}); });
  }

  classify::ByteSet bytes;
  std::array<byte_type, kMaxBlockDelims> delims{};
  size_t ndelims = 0;
};

inline constexpr size_t kHorspoolNeedleThreshold = 8;

struct OneStringFinder {
  bstring delim;

  explicit OneStringFinder(bstring&& delimiter) : delim{std::move(delimiter)} {}

  template<typename OnDelim>
  IRS_FORCE_INLINE void ForEachDelim(bytes_view data,
                                     OnDelim&& on_delim) const {
    const bytes_view needle{delim};
    for (size_t pos = data.find(needle); pos != bytes_view::npos;
         pos = data.find(needle, pos + needle.size())) {
      on_delim(pos, needle.size());
    }
  }
};

struct OneLongStringFinder {
  bstring delim;
  std::boyer_moore_horspool_searcher<bstring::const_iterator> searcher;

  explicit OneLongStringFinder(bstring&& delimiter)
    : delim{std::move(delimiter)}, searcher{delim.begin(), delim.end()} {}

  template<typename OnDelim>
  IRS_FORCE_INLINE void ForEachDelim(bytes_view data,
                                     OnDelim&& on_delim) const {
    for (auto it = std::search(data.begin(), data.end(), searcher);
         it != data.end();
         it = std::search(it + delim.size(), data.end(), searcher)) {
      on_delim(static_cast<size_t>(it - data.begin()), delim.size());
    }
  }
};

IRS_FORCE_INLINE bool BytesEqual(const byte_type* a, const byte_type* b,
                                 size_t n) {
  for (size_t i = 0; i < n; ++i) {
    if (a[i] != b[i]) {
      return false;
    }
  }
  return true;
}

struct MultiStringFinder {
  static constexpr size_t kMaxBlockFirsts = 8;
  static constexpr size_t kPrefix = sizeof(uint64_t);

  explicit MultiStringFinder(std::vector<bstring>&& delimiters) {
    for (auto& d : delimiters) {
      SDB_ASSERT(!d.empty());
      if (!first.Contains(d.front())) {
        first.Add(d.front());
        if (nfirsts < kMaxBlockFirsts) {
          firsts[nfirsts] = d.front();
        }
        ++nfirsts;
      }
      const size_t head = std::min(d.size(), kPrefix);
      std::array<byte_type, kPrefix> ones{};
      std::fill_n(ones.begin(), head, byte_type{0xFF});
      uint64_t prefix = 0;
      uint64_t mask = 0;
      std::memcpy(&prefix, d.data(), head);
      std::memcpy(&mask, ones.data(), kPrefix);
      prefixes.push_back(prefix);
      masks.push_back(mask);
      sizes.push_back(static_cast<uint32_t>(d.size()));
      delims.push_back(std::move(d));
    }
  }

  IRS_FORCE_INLINE size_t MatchAt(const byte_type* tail, size_t n) const {
    if (n < kPrefix) [[unlikely]] {
      for (size_t j = 0; j < delims.size(); ++j) {
        if (sizes[j] <= n && BytesEqual(tail, delims[j].data(), sizes[j])) {
          return sizes[j];
        }
      }
      return 0;
    }
    uint64_t t8;
    std::memcpy(&t8, tail, kPrefix);
    for (size_t j = 0; j < delims.size(); ++j) {
      if (((t8 ^ prefixes[j]) & masks[j]) != 0) {
        continue;
      }
      const size_t size = sizes[j];
      if (size <= kPrefix) {
        return size;
      }
      if (size <= n && BytesEqual(tail + kPrefix, delims[j].data() + kPrefix,
                                  size - kPrefix)) {
        return size;
      }
    }
    return 0;
  }

  template<typename OnDelim>
  IRS_FORCE_INLINE void ForEachDelim(bytes_view data,
                                     OnDelim&& on_delim) const {
    const auto* p = data.data();
    const size_t size = data.size();
    size_t pos = 0;
    if (nfirsts <= kMaxBlockFirsts && size >= classify::kClassifyBlock) {
      for (;;) {
        const size_t base = std::min(pos, size - classify::kClassifyBlock);
        auto mask =
          classify::ClassifyAnyEqBlock(p + base, {firsts.data(), nfirsts}) &
          (~uint32_t{0} << (pos - base));
        size_t next = base + classify::kClassifyBlock;
        while (mask != 0) {
          const size_t at = base + std::countr_zero(mask);
          const size_t skip = MatchAt(p + at, size - at);
          if (skip == 0) {
            mask &= mask - 1;
            continue;
          }
          on_delim(at, skip);
          const size_t end = at + skip;
          if (end >= next) {
            next = end;
            break;
          }
          mask &= ~uint32_t{0} << (end - base);
        }
        if (next >= size) {
          return;
        }
        pos = next;
      }
    }
    while (pos < size) {
      const size_t skip =
        first.Contains(p[pos]) ? MatchAt(p + pos, size - pos) : 0;
      if (skip == 0) {
        ++pos;
        continue;
      }
      on_delim(pos, skip);
      pos += skip;
    }
  }

  std::vector<uint64_t> prefixes;
  std::vector<uint64_t> masks;
  std::vector<uint32_t> sizes;
  std::vector<bstring> delims;
  classify::ByteSet first;
  std::array<byte_type, kMaxBlockFirsts> firsts{};
  size_t nfirsts = 0;
};

template<typename Finder>
class MultiDelimitedTokenizerImpl final
  : public TypedTokenizer<MultiDelimitedTokenizerImpl<Finder>>,
    public MultiDelimitedTokenizer {
 public:
  template<typename... Args>
  explicit MultiDelimitedTokenizerImpl(Args&&... args)
    : _finder{std::forward<Args>(args)...} {}

  TokenTraits Traits() const noexcept final {
    return {
      .offsets = true,
      .stable = true,
    };
  }

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t raw, TokenSink& sink) {
    const auto* p = reinterpret_cast<const byte_type*>(raw.GetData());
    const size_t size = raw.GetSize();
    const auto* const bound = p + size;
    size_t tok_begin = 0;
    const auto emit = [&](size_t begin, size_t end) IRS_FORCE_INLINE {
      if (begin == end) {
        return;
      }
      sink.EmitSlice<Layout>(
        p, bound,
        Offs{static_cast<uint32_t>(begin), static_cast<uint32_t>(end)});
    };
    _finder.ForEachDelim(bytes_view{p, size},
                         [&](size_t pos, size_t len) IRS_FORCE_INLINE {
                           emit(tok_begin, pos);
                           tok_begin = pos + len;
                         });
    emit(tok_begin, size);
    return true;
  }

 private:
  [[no_unique_address]] Finder _finder;
};

}  // namespace irs::analysis
namespace irs {

template<typename Finder>
struct Type<analysis::MultiDelimitedTokenizerImpl<Finder>>
  : Type<analysis::MultiDelimitedTokenizer> {};

}  // namespace irs
namespace irs::analysis {
namespace {

Tokenizer::ptr MakeImpl(std::vector<bstring>&& delimiters) {
  const bool single_character_case = absl::c_all_of(
    delimiters, [](const auto& delim) { return delim.size() == 1; });
  if (single_character_case) {
    switch (delimiters.size()) {
      case 0:
        return std::make_unique<MultiDelimitedTokenizerImpl<NoDelimFinder>>();
      case 1:
        return std::make_unique<MultiDelimitedTokenizerImpl<OneCharFinder>>(
          delimiters[0][0]);
      default:
        return std::make_unique<MultiDelimitedTokenizerImpl<ManyCharsFinder>>(
          delimiters);
    }
  }
  if (delimiters.size() == 1) {
    if (delimiters[0].size() > kHorspoolNeedleThreshold) {
      return std::make_unique<MultiDelimitedTokenizerImpl<OneLongStringFinder>>(
        std::move(delimiters[0]));
    }
    return std::make_unique<MultiDelimitedTokenizerImpl<OneStringFinder>>(
      std::move(delimiters[0]));
  }
  return std::make_unique<MultiDelimitedTokenizerImpl<MultiStringFinder>>(
    std::move(delimiters));
}

}  // namespace

Tokenizer::ptr MultiDelimitedTokenizer::Make(
  MultiDelimitedTokenizer::Options opts) {
  for (size_t i = 0; i < opts.delimiters.size(); ++i) {
    const bytes_view view{opts.delimiters[i]};
    if (view.empty()) {
      THROW_SQL_ERROR(ERR_MSG("multi_delimited: empty delimiter"));
    }
    for (size_t j = 0; j < i; ++j) {
      const bytes_view known{opts.delimiters[j]};
      if (view.starts_with(known) || known.starts_with(view)) {
        THROW_SQL_ERROR(
          ERR_MSG("multi_delimited: delimiters must not be prefixes of one "
                  "another"));
      }
    }
  }
  return MakeImpl(std::move(opts.delimiters));
}

}  // namespace irs::analysis
