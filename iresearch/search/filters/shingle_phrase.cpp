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

#include <algorithm>
#include <limits>
#include <span>
#include <vector>

#include "iresearch/analysis/shingle_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/search/detail/phrase_verify.hpp"

namespace irs {
namespace {

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

  size_t Min() const noexcept { return std::max(_tokenizer.MinShingle(), 2U); }

  size_t Max() const noexcept { return _tokenizer.MaxShingle(); }

  size_t RunEnd(size_t i) const noexcept { return _run_end[i]; }

  PosAttr::value_t Position(size_t i) const noexcept { return _positions[i]; }

  bool Indexed(size_t begin, size_t count) const noexcept {
    if (begin + count > _run_end[begin]) {
      return false;
    }
    if (count == 1) {
      return _tokenizer.OutputUnigrams();
    }
    if (count < Min() || count > Max()) {
      return false;
    }
    if (!_tokenizer.HasFrequentWords() || count == Min()) {
      return true;
    }
    return absl::c_any_of(_tokens.subspan(begin, count), [&](bytes_view token) {
      return _tokenizer.IsFrequent(token);
    });
  }

  size_t Largest(size_t begin) const noexcept {
    const auto reach = std::min(Max(), _run_end[begin] - begin);
    for (auto count = reach; count >= Min(); --count) {
      if (Indexed(begin, count)) {
        return count;
      }
    }
    return 0;
  }

  bstring Term(size_t begin, size_t count) const {
    return _tokenizer.Join(_tokens.subspan(begin, count));
  }

 private:
  const analysis::ShingleTokenizer& _tokenizer;
  std::span<const bytes_view> _tokens;
  std::span<const PosAttr::value_t> _positions;
  std::vector<size_t> _run_end;
};

bool Cover(const Windows& windows, size_t m, ByPhraseOptions& out) {
  constexpr auto kNone = std::numeric_limits<size_t>::max();
  size_t prev = kNone;
  const auto emit = [&](size_t start, size_t count) {
    auto& part = prev == kNone
                   ? out.push_back<ByTermOptions>()
                   : out.push_back<ByTermOptions>(windows.Position(start) -
                                                  windows.Position(prev) - 1);
    part.term = windows.Term(start, count);
    prev = start;
  };
  for (size_t i = 0; i != m;) {
    const auto run_end = windows.RunEnd(i);
    auto run_prev = kNone;
    while (i != run_end) {
      auto start = i;
      auto count = windows.Largest(i);
      if (count == 0 && run_prev != kNone) {
        for (auto cand = std::min(windows.Max(), run_end - run_prev - 1);
             cand >= windows.Min(); --cand) {
          if (windows.Indexed(run_end - cand, cand)) {
            start = run_end - cand;
            count = cand;
            break;
          }
        }
      }
      if (count == 0) {
        if (!windows.Indexed(i, 1)) {
          return false;
        }
        count = 1;
      }
      emit(start, count);
      run_prev = start;
      i = start + count;
    }
  }
  return true;
}

bool Legs(const Windows& windows, size_t m, std::vector<bstring>& legs) {
  std::vector<bool> covered(m);
  for (size_t i = 0; i != m; ++i) {
    const auto count = windows.Largest(i);
    if (count == 0 ||
        std::all_of(covered.begin() + i, covered.begin() + i + count,
                    [](bool c) { return c; })) {
      continue;
    }
    legs.push_back(windows.Term(i, count));
    std::fill(covered.begin() + i, covered.begin() + i + count, true);
  }
  for (size_t i = 0; i != m; ++i) {
    if (covered[i]) {
      continue;
    }
    if (!windows.Indexed(i, 1)) {
      return false;
    }
    legs.push_back(windows.Term(i, 1));
  }
  absl::c_sort(legs);
  legs.erase(std::unique(legs.begin(), legs.end()), legs.end());
  return !legs.empty();
}

}  // namespace

ShinglePhrasePlan PlanShinglePhrase(
  const analysis::ShingleTokenizer& tokenizer, const ByPhraseOptions& phrase,
  bool positional, std::shared_ptr<const PhraseTokenSourceFactory> source) {
  ShinglePhrasePlan plan;
  if (phrase.empty() || phrase.slop() != 0) {
    return plan;
  }
  std::vector<bytes_view> tokens;
  std::vector<PosAttr::value_t> positions;
  PosAttr::value_t pos = 0;
  for (const auto& info : phrase) {
    const auto* term = std::get_if<ByTermOptions>(&info.part);
    if (!term || info.offs_min != info.offs_max ||
        (!tokens.empty() && info.offs_max == 0)) {
      return plan;
    }
    pos += info.offs_max;
    tokens.emplace_back(term->term);
    positions.push_back(pos);
  }
  const auto m = tokens.size();
  const Windows windows{tokenizer, tokens, positions};
  if (windows.Indexed(0, m)) {
    plan.kind = ShinglePhrasePlan::Kind::Term;
    plan.term = windows.Term(0, m);
    return plan;
  }
  if (positional) {
    if (Cover(windows, m, plan.phrase)) {
      plan.kind = ShinglePhrasePlan::Kind::Phrase;
    }
    return plan;
  }
  std::vector<bstring> legs;
  if (!source || !Legs(windows, m, legs)) {
    return plan;
  }
  for (auto& leg : legs) {
    plan.phrase.push_back<ByTermOptions>().term = std::move(leg);
  }
  plan.phrase.set_verifier(
    std::make_shared<PhraseVerifier>(std::move(source), phrase));
  plan.kind = ShinglePhrasePlan::Kind::Phrase;
  return plan;
}

}  // namespace irs
