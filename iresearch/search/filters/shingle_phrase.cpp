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

#include "iresearch/search/filters/shingle_phrase.hpp"

#include <absl/algorithm/container.h>
#include <absl/container/inlined_vector.h>

#include <algorithm>
#include <limits>
#include <span>
#include <vector>

#include "iresearch/analysis/shingle_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"

namespace irs {
namespace {

bool Indexes(const analysis::ShingleTokenizer& tokenizer,
             std::span<const bytes_view> words) noexcept {
  const auto n = words.size();
  if (n == 1 && tokenizer.OutputUnigrams()) {
    return true;
  }
  if (n < tokenizer.MinShingle() || n > tokenizer.MaxShingle() ||
      (n > 1 && tokenizer.Base().Traits().explicit_pos)) {
    return false;
  }
  return n == tokenizer.MinShingle() || !tokenizer.HasFrequentWords() ||
         absl::c_any_of(
           words, [&](bytes_view word) { return tokenizer.IsFrequent(word); });
}

bstring Join(bytes_view separator, std::span<const bytes_view> words) {
  SDB_ASSERT(!words.empty());
  auto size = separator.size() * (words.size() - 1);
  for (const auto word : words) {
    size += word.size();
  }
  bstring out;
  out.reserve(size);
  out.append(words.front());
  for (const auto word : words.subspan(1)) {
    out.append(separator).append(word);
  }
  return out;
}

class Windows {
 public:
  Windows(const analysis::ShingleTokenizer& tokenizer,
          std::span<const bytes_view> tokens,
          std::span<const PosAttr::value_t> positions)
    : _tokenizer{tokenizer},
      _tokens{tokens},
      _positions{positions},
      _run_end(tokens.size()) {
    auto end = tokens.size();
    for (auto i = tokens.size(); i-- != 0;) {
      if (i + 1 != tokens.size() && positions[i + 1] - positions[i] != 1) {
        end = i + 1;
      }
      _run_end[i] = end;
    }
  }

  size_t Size() const noexcept { return _tokens.size(); }

  size_t Max() const noexcept { return _tokenizer.MaxShingle(); }

  size_t RunEnd(size_t i) const noexcept { return _run_end[i]; }

  PosAttr::value_t Position(size_t i) const noexcept { return _positions[i]; }

  bool Indexed(size_t begin, size_t count) const noexcept {
    return Indexes(_tokenizer, _tokens.subspan(begin, count));
  }

  size_t Largest(size_t begin) const noexcept {
    const auto reach = std::min(Max(), _run_end[begin] - begin);
    for (auto count = reach; count >= 2; --count) {
      if (Indexed(begin, count)) {
        return count;
      }
    }
    return 0;
  }

  bstring Term(size_t begin, size_t count) const {
    return Join(_tokenizer.Separator(), _tokens.subspan(begin, count));
  }

 private:
  const analysis::ShingleTokenizer& _tokenizer;
  std::span<const bytes_view> _tokens;
  std::span<const PosAttr::value_t> _positions;
  std::vector<size_t> _run_end;
};

constexpr auto kNone = std::numeric_limits<size_t>::max();

size_t Cover(const Windows& windows, PosAttr::value_t entry_min,
             PosAttr::value_t entry_max, ByPhraseOptions& out, bool& shingled) {
  size_t prev = kNone;
  const auto emit = [&](size_t start, size_t count) {
    auto offs_min = entry_min;
    auto offs_max = entry_max;
    if (prev != kNone) {
      offs_min = offs_max = windows.Position(start) - windows.Position(prev);
    }
    out.push_back<ByTermOptions>(offs_min, offs_max).term =
      windows.Term(start, count);
    shingled |= count > 1;
    prev = start;
  };
  for (size_t i = 0, m = windows.Size(); i != m;) {
    const auto run_end = windows.RunEnd(i);
    auto run_prev = kNone;
    while (i != run_end) {
      auto start = i;
      auto count = windows.Largest(i);
      if (count == 0 && run_prev != kNone) {
        for (auto cand = std::min(windows.Max(), run_end - run_prev - 1);
             cand >= 2; --cand) {
          if (windows.Indexed(run_end - cand, cand)) {
            start = run_end - cand;
            count = cand;
            break;
          }
        }
      }
      if (count == 0) {
        if (!windows.Indexed(i, 1)) {
          return kNone;
        }
        count = 1;
      }
      emit(start, count);
      run_prev = start;
      i = start + count;
    }
  }
  return prev;
}

bool CoverPhrase(const analysis::ShingleTokenizer& tokenizer,
                 const ByPhraseOptions& phrase, ByPhraseOptions& out) {
  std::vector<bytes_view> tokens;
  std::vector<PosAttr::value_t> positions;
  PosAttr::value_t entry_min = 0;
  PosAttr::value_t entry_max = 0;
  PosAttr::value_t lag = 0;
  bool shingled = false;
  bool patterns = false;
  const auto flush = [&] {
    if (tokens.empty()) {
      return true;
    }
    const Windows windows{tokenizer, tokens, positions};
    const auto last =
      Cover(windows, entry_min + lag, entry_max + lag, out, shingled);
    if (last == kNone) {
      return false;
    }
    lag = positions.back() - positions[last];
    tokens.clear();
    positions.clear();
    return true;
  };
  for (const auto& info : phrase) {
    const auto* term = std::get_if<ByTermOptions>(&info.part);
    if (term && !tokens.empty() && info.offs_min == info.offs_max) {
      positions.push_back(positions.back() + info.offs_max);
      tokens.emplace_back(term->term);
      continue;
    }
    if (!flush()) {
      return false;
    }
    if (term) {
      entry_min = info.offs_min;
      entry_max = info.offs_max;
      tokens.emplace_back(term->term);
      positions.push_back(0);
      continue;
    }
    if (!tokenizer.OutputUnigrams()) {
      return false;
    }
    patterns |= ByPhraseOptions::KindOf(info.part) == SlotKind::Expansion;
    std::visit(
      [&]<typename Part>(const Part& part) {
        out.push_back<Part>(info.offs_min + lag, info.offs_max + lag) = part;
      },
      info.part);
    lag = 0;
  }
  if (!flush() || !shingled || (patterns && tokenizer.Separator().empty())) {
    return false;
  }
  out.set_word_separator(tokenizer.Separator());
  return true;
}

}  // namespace

std::optional<bstring> ShingleTerm(const analysis::ShingleTokenizer& tokenizer,
                                   const ByPhraseOptions& phrase) {
  if (phrase.slop() != 0) {
    return std::nullopt;
  }
  absl::InlinedVector<bytes_view, 8> words;
  for (const auto& info : phrase) {
    const auto* term = std::get_if<ByTermOptions>(&info.part);
    if (!term ||
        (!words.empty() && (info.offs_min != 1 || info.offs_max != 1))) {
      return std::nullopt;
    }
    words.emplace_back(term->term);
  }
  if (!Indexes(tokenizer, words)) {
    return std::nullopt;
  }
  return Join(tokenizer.Separator(), words);
}

std::optional<ByPhraseOptions> ShingleCover(
  const analysis::ShingleTokenizer& tokenizer, const ByPhraseOptions& phrase) {
  ByPhraseOptions cover;
  if (phrase.slop() != 0 || !CoverPhrase(tokenizer, phrase, cover)) {
    return std::nullopt;
  }
  return cover;
}

}  // namespace irs
