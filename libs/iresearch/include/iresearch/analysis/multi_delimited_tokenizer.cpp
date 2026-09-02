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

template<TokenLayout Layout, typename Finder>
void DrainSingleCharValue(TokenSink& sink, duckdb::string_t raw,
                          const Finder& finder) {
  const auto* p = reinterpret_cast<const byte_type*>(raw.GetData());
  const size_t size = raw.GetSize();
  size_t tok_begin = 0;

  const auto* const bound = p + size;
  const auto emit = [&](size_t begin, size_t end) IRS_FORCE_INLINE {
    if (begin == end) {
      return;
    }
    sink.EmitSlice<Layout>(
      p, bound, Offs{static_cast<uint32_t>(begin), static_cast<uint32_t>(end)});
  };

  classify::DrainClassified(
    p, size, true,
    [&](const byte_type* block)
      IRS_FORCE_INLINE { return finder.ClassifyBlock(block); },
    [&](byte_type c) IRS_FORCE_INLINE { return finder.IsDelimByte(c); },
    [&](size_t pos) IRS_FORCE_INLINE {
      emit(tok_begin, pos);
      tok_begin = pos + 1;
    });
  emit(tok_begin, size);
}

struct NoDelimFinder {
  auto FindNextDelim(bytes_view data) const {
    return std::make_pair(data.end(), size_t{0});
  }
};

struct OneCharFinder {
  byte_type delim;

  bool IsDelimByte(byte_type c) const { return c == delim; }
  uint32_t ClassifyBlock(const byte_type* block) const {
    return classify::ClassifyEqBlock(block, delim);
  }
};

struct ManyCharsFinder {
  static constexpr size_t kMaxBlockDelims = 8;

  explicit ManyCharsFinder(const std::vector<bstring>& delimiters) {
    for (const auto& delim : delimiters) {
      SDB_ASSERT(delim.size() == 1);
      bytes[delim[0]] = true;
      if (ndelims < kMaxBlockDelims) {
        delims[ndelims] = delim[0];
      }
      ++ndelims;
    }
  }

  auto FindNextDelim(bytes_view data) const {
    auto next = absl::c_find_if(data, [&](auto c) { return bytes[c]; });
    return std::make_pair(next, size_t{1});
  }

  bool CanBlockScan() const { return ndelims <= kMaxBlockDelims; }
  bool IsDelimByte(byte_type c) const { return bytes[c]; }
  uint32_t ClassifyBlock(const byte_type* block) const {
    return classify::ClassifyAnyEqBlock(block, {delims.data(), ndelims});
  }

  std::array<bool, 256> bytes{};
  std::array<byte_type, kMaxBlockDelims> delims{};
  size_t ndelims = 0;
};

inline constexpr size_t kHorspoolNeedleThreshold = 8;

struct OneStringFinder {
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
  bstring delim;
  std::boyer_moore_horspool_searcher<bstring::const_iterator> searcher;

  explicit OneLongStringFinder(bstring&& delimiter)
    : delim{std::move(delimiter)}, searcher{delim.begin(), delim.end()} {}

  auto FindNextDelim(bytes_view data) const {
    auto next = std::search(data.begin(), data.end(), searcher);
    return std::make_pair(next, delim.size());
  }
};

struct MultiStringFinder {
  static constexpr size_t kMaxBlockFirsts = 8;

  explicit MultiStringFinder(std::vector<bstring>&& delimiters) {
    for (auto& d : delimiters) {
      SDB_ASSERT(!d.empty());
      if (!first[d.front()]) {
        first[d.front()] = true;
        if (nfirsts < kMaxBlockFirsts) {
          firsts[nfirsts] = d.front();
        }
        ++nfirsts;
      }
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

  IRS_FORCE_INLINE size_t MatchAt(bytes_view tail) const {
    for (const auto& d : short_delims) {
      if (tail.starts_with(d)) {
        return d.size();
      }
    }
    if (!long_delims.empty()) {
      uint32_t t4 = 0;
      std::memcpy(&t4, tail.data(), std::min<size_t>(tail.size(), sizeof t4));
      for (size_t j = 0; j < long_delims.size(); ++j) {
        if (t4 == long_prefix4[j] && tail.starts_with(long_delims[j])) {
          return long_delims[j].size();
        }
      }
    }
    return 0;
  }

  auto FindNextDelim(bytes_view data) const {
    const auto* p = data.data();
    const size_t size = data.size();
    size_t pos = 0;
    if (nfirsts <= kMaxBlockFirsts) {
      for (; size - pos >= classify::kClassifyBlock;
           pos += classify::kClassifyBlock) {
        auto mask =
          classify::ClassifyAnyEqBlock(p + pos, {firsts.data(), nfirsts});
        while (mask != 0) {
          const size_t at = pos + std::countr_zero(mask);
          if (const auto skip = MatchAt(data.substr(at)); skip != 0) {
            return std::make_pair(data.begin() + at, skip);
          }
          mask &= mask - 1;
        }
      }
    }
    for (; pos < size; ++pos) {
      if (!first[p[pos]]) {
        continue;
      }
      if (const auto skip = MatchAt(data.substr(pos)); skip != 0) {
        return std::make_pair(data.begin() + pos, skip);
      }
    }
    return std::make_pair(data.end(), size_t{0});
  }

  std::vector<bstring> short_delims;
  std::vector<bstring> long_delims;
  std::vector<uint32_t> long_prefix4;
  std::array<bool, 256> first{};
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

  static constexpr bool kFindsDelims =
    requires(const Finder& f) { f.FindNextDelim(bytes_view{}); };
  static constexpr bool kMayBlockScan =
    requires(const Finder& f) { f.CanBlockScan(); };

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t raw, TokenSink& sink) {
    if constexpr (!kFindsDelims) {
      DrainSingleCharValue<Layout>(sink, raw, _finder);
      return true;
    } else {
      if constexpr (kMayBlockScan) {
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
        const auto off =
          static_cast<uint32_t>(std::distance(start, data.data()));
        sink.EmitSlice<Layout>(start, bound, Offs{off, off + size});

        if (next == data.end()) {
          data = {};
        } else {
          data =
            bytes_view(&(*next) + skip, std::distance(next, data.end()) - skip);
        }
      }
      return true;
    }
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
