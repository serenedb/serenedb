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
#include <functional>
#include <string_view>

#include "iresearch/analysis/classify.hpp"
#include "iresearch/analysis/term_view.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

// Block-drained splitter for single-byte delimiter sets: one classification
// mask per 32-byte block, every set bit consumed, empty tokens skipped,
// zero-copy token views.
template<TokenLayout Layout, typename Finder>
void DrainSingleCharValue(TokenSink& sink, duckdb::string_t raw,
                          const Finder& finder) {
  const auto* p = reinterpret_cast<const byte_type*>(raw.GetData());
  const size_t size = raw.GetSize();
  size_t tok_begin = 0;

  const auto emit = [&](size_t begin, size_t end) {
    if (begin == end) {
      return;
    }
    sink.Emit<Layout>(
      MakeTermView(p + begin, static_cast<uint32_t>(end - begin), p + size),
      Offs{static_cast<uint32_t>(begin), static_cast<uint32_t>(end)});
  };

  size_t offset = 0;
  while (size - offset >= classify::kClassifyBlock) {
    VisitSetBits(finder.ClassifyBlock(p + offset), [&](uint32_t bit) {
      const size_t pos = offset + bit;
      emit(tok_begin, pos);
      tok_begin = pos + 1;
    });
    offset += classify::kClassifyBlock;
  }
  for (; offset < size; ++offset) {
    if (finder.IsDelimByte(p[offset])) {
      emit(tok_begin, offset);
      tok_begin = offset + 1;
    }
  }
  emit(tok_begin, size);
}

// Delimiter-search policies for the single flat tokenizer below. Each returns
// {next delimiter position, bytes to skip past it}; kBlockScan finders also
// expose the block-classification trio consumed by DrainSingleCharValue.

struct NoDelimFinder {
  static constexpr bool kBlockScan = false;

  auto FindNextDelim(bytes_view data) const {
    return std::make_pair(data.end(), size_t{0});
  }
};

struct OneCharFinder {
  static constexpr bool kBlockScan = true;

  byte_type delim;

  auto FindNextDelim(bytes_view data) const {
    auto next = data.end();
    if (auto pos = data.find(delim); pos != bstring::npos) {
      next = data.begin() + pos;
    }
    return std::make_pair(next, size_t{1});
  }

  bool CanBlockScan() const { return true; }
  bool IsDelimByte(byte_type c) const { return c == delim; }
  uint32_t ClassifyBlock(const byte_type* block) const {
    return ClassifyEqBlock(block, delim);
  }
};

struct ManyCharsFinder {
  static constexpr bool kBlockScan = true;
  static constexpr size_t kMaxBlockDelims = 8;

  explicit ManyCharsFinder(const std::vector<bstring>& delimiters) {
    for (const auto& delim : delimiters) {
      SDB_ASSERT(delim.size() == 1);
      if (delim[0] > SCHAR_MAX) {
        // The table path never matches high bytes; keep that behavior and
        // disable the block path so both agree.
        high_byte_delim = true;
        continue;
      }
      bytes[delim[0]] = true;
      if (ndelims < kMaxBlockDelims) {
        delims[ndelims] = delim[0];
      }
      ++ndelims;
    }
  }

  auto FindNextDelim(bytes_view data) const {
    auto next =
      absl::c_find_if(data, [&](auto c) { return c <= SCHAR_MAX && bytes[c]; });
    return std::make_pair(next, size_t{1});
  }

  bool CanBlockScan() const {
    return !high_byte_delim && ndelims <= kMaxBlockDelims;
  }
  bool IsDelimByte(byte_type c) const { return c <= SCHAR_MAX && bytes[c]; }
  uint32_t ClassifyBlock(const byte_type* block) const {
    return ClassifyAnyEqBlock(block, {delims.data(), ndelims});
  }

  // TODO(mbkkt) maybe use a bitset instead?
  std::array<bool, SCHAR_MAX + 1> bytes{};
  std::array<byte_type, kMaxBlockDelims> delims{};
  size_t ndelims = 0;
  bool high_byte_delim = false;
};

// Needle-length picks the one-string algorithm (the only construction-time
// signal; first-byte density in the data is what really separates the
// regimes, pinned by the multi_delimiter_str* arms): memchr-backed find
// dominates short needles (2B: 2.4x vs boyer-moore) and rare-first-byte long
// needles (15B: -26%), boyer-moore dominates long needles whose first byte
// is common in the data (10B markup: 2.4x) with a bounded worst case where
// find's blowup is not.
inline constexpr size_t kBmNeedleThreshold = 8;

struct OneStringFinder {
  static constexpr bool kBlockScan = false;

  bstring delim;

  explicit OneStringFinder(bstring&& delimiter) : delim{std::move(delimiter)} {}

  auto FindNextDelim(bytes_view data) const {
    auto next = data.end();
    if (auto pos = data.find(delim); pos != bstring::npos) {
      next = data.begin() + pos;
    }
    return std::make_pair(next, delim.size());
  }
};

struct OneLongStringFinder {
  static constexpr bool kBlockScan = false;

  bstring delim;
  std::boyer_moore_horspool_searcher<bstring::const_iterator> searcher;

  explicit OneLongStringFinder(bstring&& delimiter)
    : delim{std::move(delimiter)}, searcher{delim.begin(), delim.end()} {}

  auto FindNextDelim(bytes_view data) const {
    auto next = std::search(data.begin(), data.end(), searcher);
    return std::make_pair(next, delim.size());
  }
};

// The prefix-free rule (enforced by Make) means at most one delimiter can
// match at any start position, so leftmost-first search needs no automaton:
// scan for the next byte in the first-byte set, then verify the candidates
// sharing it.
struct MultiStringFinder {
  static constexpr bool kBlockScan = false;

  explicit MultiStringFinder(std::vector<bstring>&& delimiters) {
    for (auto& d : delimiters) {
      SDB_ASSERT(!d.empty());
      first[d.front()] = true;
      if (d.size() >= sizeof(uint32_t)) {
        uint32_t p;
        std::memcpy(&p, d.data(), sizeof p);
        long_prefix4.push_back(p);
        long_delims.push_back(std::move(d));
      } else {
        short_delims.push_back(std::move(d));
      }
    }
  }

  // Prefix-free (Make) means at most one delimiter matches at any start, so
  // candidate order is free. Short delimiters take a bare starts_with; long
  // ones are prefiltered by one u32 load register-compared against each
  // delimiter's first four bytes -- a mismatching candidate family dies in a
  // cycle instead of a memcmp (the tags_hard arm is that regime), while
  // short-only sets never pay for the machinery (the mixed8 arm is that
  // one). The prefilter only filters; starts_with decides, so zero-padded
  // partial loads near the value end stay correct.
  auto FindNextDelim(bytes_view data) const {
    for (size_t pos = 0; pos < data.size(); ++pos) {
      if (!first[data[pos]]) {
        continue;
      }
      const auto tail = data.substr(pos);
      for (const auto& d : short_delims) {
        if (tail.starts_with(d)) {
          return std::make_pair(data.begin() + pos, d.size());
        }
      }
      if (!long_delims.empty()) {
        uint32_t t4 = 0;
        std::memcpy(&t4, tail.data(), std::min<size_t>(tail.size(), sizeof t4));
        for (size_t j = 0; j < long_delims.size(); ++j) {
          if (t4 == long_prefix4[j] && tail.starts_with(long_delims[j])) {
            return std::make_pair(data.begin() + pos, long_delims[j].size());
          }
        }
      }
    }
    return std::make_pair(data.end(), size_t{0});
  }

  std::vector<bstring> short_delims;
  std::vector<bstring> long_delims;
  std::vector<uint32_t> long_prefix4;
  std::array<bool, 256> first{};
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
    };
  }

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t raw, TokenSink& sink) {
    if constexpr (Finder::kBlockScan) {
      if (_finder.CanBlockScan()) {
        DrainSingleCharValue<Layout>(sink, raw, _finder);
        return true;
      }
    }
    bytes_view data{reinterpret_cast<const byte_type*>(raw.GetData()),
                    raw.GetSize()};
    const byte_type* const start = data.data();
    const byte_type* const bound = start + data.size();
    while (data.begin() != data.end()) {
      auto [next, skip] = _finder.FindNextDelim(data);

      if (next == data.begin()) {
        SDB_ASSERT(skip <= data.size());
        data = bytes_view(data.data() + skip, data.size() - skip);
        continue;
      }

      const auto size =
        static_cast<uint32_t>(std::distance(data.begin(), next));
      const auto off = static_cast<uint32_t>(std::distance(start, data.data()));
      sink.Emit<Layout>(MakeTermView(data.data(), size, bound),
                        Offs{off, off + size});

      if (next == data.end()) {
        data = {};
      } else {
        data =
          bytes_view(&(*next) + skip, std::distance(next, data.end()) - skip);
      }
    }
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
    if (delimiters[0].size() > kBmNeedleThreshold) {
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
