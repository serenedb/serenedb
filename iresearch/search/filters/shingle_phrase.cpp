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
#include <span>

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

size_t Largest(const analysis::ShingleTokenizer& tokenizer,
               std::span<const bytes_view> words) noexcept {
  for (auto count = std::min<size_t>(tokenizer.MaxShingle(), words.size());
       count >= 2; --count) {
    if (Indexes(tokenizer, words.first(count))) {
      return count;
    }
  }
  return 0;
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
  if (phrase.slop() != 0) {
    return std::nullopt;
  }
  ByPhraseOptions cover;
  absl::InlinedVector<bytes_view, 8> run;
  PosAttr::value_t run_min = 0;
  PosAttr::value_t run_max = 0;
  PosAttr::value_t lag = 0;
  bool shingled = false;
  const auto flush = [&] {
    if (run.empty()) {
      return true;
    }
    const std::span<const bytes_view> words{run};
    auto offs_min = run_min + lag;
    auto offs_max = run_max + lag;
    size_t prev = 0;
    for (size_t i = 0; i != words.size();) {
      auto start = i;
      auto count = Largest(tokenizer, words.subspan(i));
      if (count == 0 && i != 0) {
        for (auto tail = std::min<size_t>(tokenizer.MaxShingle(),
                                          words.size() - prev - 1);
             tail >= 2; --tail) {
          if (Indexes(tokenizer, words.last(tail))) {
            start = words.size() - tail;
            count = tail;
            break;
          }
        }
      }
      if (count == 0) {
        if (!Indexes(tokenizer, words.subspan(i, 1))) {
          return false;
        }
        count = 1;
      }
      if (i != 0) {
        offs_min = offs_max = static_cast<PosAttr::value_t>(start - prev);
      }
      cover.push_back<ByTermOptions>(offs_min, offs_max).term =
        Join(tokenizer.Separator(), words.subspan(start, count));
      shingled |= count > 1;
      prev = start;
      i = start + count;
    }
    lag = static_cast<PosAttr::value_t>(words.size() - 1 - prev);
    run.clear();
    return true;
  };
  for (const auto& info : phrase) {
    const auto* term = std::get_if<ByTermOptions>(&info.part);
    if (term && !run.empty() && info.offs_min == 1 && info.offs_max == 1) {
      run.emplace_back(term->term);
      continue;
    }
    if (!flush()) {
      return std::nullopt;
    }
    if (term) {
      run_min = info.offs_min;
      run_max = info.offs_max;
      run.emplace_back(term->term);
      continue;
    }
    if (!tokenizer.OutputUnigrams() ||
        (tokenizer.Separator().empty() &&
         ByPhraseOptions::KindOf(info.part) == SlotKind::Expansion)) {
      return std::nullopt;
    }
    std::visit(
      [&]<typename Part>(const Part& part) {
        cover.push_back<Part>(info.offs_min + lag, info.offs_max + lag) = part;
      },
      info.part);
    lag = 0;
  }
  if (!flush() || !shingled) {
    return std::nullopt;
  }
  cover.set_word_separator(tokenizer.Separator());
  return cover;
}

}  // namespace irs
