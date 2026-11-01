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

#include "iresearch/search/detail/compiled_phrase.hpp"

#include <absl/algorithm/container.h>
#include <absl/functional/overload.h>

#include <algorithm>
#include <limits>
#include <span>
#include <utility>

namespace irs {
namespace {

size_t WordCount(const ByPhraseOptions& phrase) noexcept {
  size_t count = 0;
  for (const auto& info : phrase) {
    const auto* shingle = std::get_if<ShingleOptions>(&info.part);
    count += shingle ? shingle->words.size() : 1;
  }
  return count;
}

void AddWordStats(const ByPhraseOptions& phrase, const TermReader& reader,
                  std::span<PhraseWordStat> words) {
  auto* stat = words.data();
  const auto lookup = [&](bytes_view word) {
    const auto meta = reader.Lookup(word);
    stat->docs += meta.docs_count;
    stat->freq += meta.freq;
    ++stat;
  };
  for (const auto& info : phrase) {
    std::visit(absl::Overload{
                 [&](const ShingleOptions& shingle) {
                   absl::c_for_each(shingle.words, lookup);
                 },
                 [&](const ByTermOptions& word) { lookup(word.term); },
                 [&](const auto&) { ++stat; },
               },
               info.part);
  }
}

TermPredicate::ptr PatternOf(const ByPhraseOptions::PhrasePart& part,
                             bytes_view separator) {
  auto pattern = std::visit(
    []<typename Part>(const Part& options) -> TermPredicate::ptr {
      if constexpr (std::is_same_v<Part, ByPrefixOptions>) {
        return MakeTermPredicate(
          [prefix = bstring{options.term}](bytes_view term) {
            return term.starts_with(prefix);
          });
      } else if constexpr (std::is_same_v<Part, ByRangeOptions>) {
        return MakeTermPredicate([range = options.range](bytes_view term) {
          return RangeAcceptor{{&range}, {&range}}(term);
        });
      } else if constexpr (std::is_same_v<Part, AutomatonOptions> ||
                           std::is_same_v<Part, LevenshteinAutomatonOptions>) {
        return options.source ? options.source->Predicate() : nullptr;
      } else {
        return nullptr;
      }
    },
    part);
  if (!pattern || separator.empty()) {
    return pattern;
  }
  return MakeTermPredicate([separator = bstring{separator},
                            word = std::move(pattern)](bytes_view term) {
    return term.find(separator) == bytes_view::npos && word->Accepts(term);
  });
}

void LayoutSlop(CompiledPhrase& phrase) {
  const auto& slots = phrase.slots;
  auto& slop = phrase.slop;
  const auto n = static_cast<uint32_t>(slots.size());
  slop.offsets.assign(n, 0);
  for (uint32_t i = 1; i != n; ++i) {
    slop.offsets[i] = slop.offsets[i - 1] + slots[i].offs_max;
  }

  std::vector<uint32_t> groups(n);
  absl::c_iota(groups, uint32_t{0});
  const auto find = [&](uint32_t x) {
    while (groups[x] != x) {
      groups[x] = groups[groups[x]];
      x = groups[x];
    }
    return x;
  };
  phrase.accept.ForEachMapped([&](const CompiledPhrase::Accept& entry) {
    const auto root = find(entry.slots.front());
    for (size_t i = 1; i < entry.slots.size(); ++i) {
      groups[find(entry.slots[i])] = root;
    }
  });
  for (const auto p : phrase.pattern_slots) {
    groups[find(p)] = find(phrase.pattern_slots.front());
    for (const auto& term : phrase.terms) {
      if (slots[p].pattern->Accepts(term)) {
        groups[find(phrase.accept.Find(term)->slots.front())] = find(p);
      }
    }
  }
  for (uint32_t i = 0; i != n; ++i) {
    groups[i] = find(i);
  }
  detail::slop::BuildGroupPairs(
    n, [&](size_t i) { return groups[i]; }, slop.pairs);
}

void LayoutAutomaton(CompiledPhrase& phrase) {
  CompiledPhrase::Automaton layout;
  uint64_t bit = 0;
  for (size_t k = 0; k != phrase.slots.size(); ++k) {
    auto& slot = phrase.slots[k];
    if (k != 0) {
      const auto prev = bit;
      bit = prev + slot.offs_max;
      if (bit >= CompiledPhrase::kMaxBits) {
        return;
      }
      for (auto g = prev + 1; g < bit; ++g) {
        layout.wild |= uint64_t{1} << g;
      }
      if (slot.offs_min < slot.offs_max) {
        uint64_t range = 0;
        for (auto j = prev + slot.offs_min - 1; j + 2 <= bit; ++j) {
          range |= uint64_t{1} << j;
        }
        layout.extras.emplace_back(range, uint64_t{1} << bit);
      }
    }
    slot.bit = uint64_t{1} << bit;
  }
  layout.last = uint64_t{1} << bit;
  phrase.accept.ForEachMapped([&](CompiledPhrase::Accept& entry) {
    for (const auto slot : entry.slots) {
      entry.mask |= phrase.slots[slot].bit;
    }
  });
  phrase.automaton = std::move(layout);
}

std::optional<CompiledPhrase::Anchor> AnchorOf(
  std::span<const CompiledPhrase::Slot> slots,
  std::span<const PhraseWordStat> words) {
  constexpr auto kUnknown = std::numeric_limits<double>::max();
  std::optional<CompiledPhrase::Anchor> anchor;
  double best = kUnknown;
  for (uint32_t k = 0; k != slots.size(); ++k) {
    if (!slots[k].word) {
      continue;
    }
    const auto& word = *slots[k].word;
    const auto [docs, freq] = k < words.size() ? words[k] : PhraseWordStat{};
    double cost = kUnknown;
    if (docs != 0) {
      cost = freq != 0 ? static_cast<double>(freq) / static_cast<double>(docs)
                       : static_cast<double>(docs);
    }
    if (!anchor || cost < best ||
        (cost == best &&
         word.GetSize() > slots[anchor->slot].word->GetSize())) {
      anchor = CompiledPhrase::Anchor{.slot = k};
      best = cost;
    }
  }
  if (!anchor) {
    return anchor;
  }
  for (uint32_t k = 1; k != slots.size(); ++k) {
    (k <= anchor->slot ? anchor->left : anchor->right) += slots[k].offs_max;
  }
  return anchor;
}

bool IsAnchored(const ByPhraseOptions& phrase) noexcept {
  const std::span parts{phrase.begin(), phrase.end()};
  return phrase.slop() == 0 &&
         (parts.empty() ||
          absl::c_none_of(parts.subspan(1),
                          [](const auto& info) { return info.offs_min == 0; }));
}

}  // namespace

std::vector<PhraseWordStat> PhraseWordStats(const ByPhraseOptions& phrase,
                                            const TermReader& reader) {
  std::vector<PhraseWordStat> words;
  if (!IsAnchored(phrase)) {
    return words;
  }
  words.resize(WordCount(phrase));
  AddWordStats(phrase, reader, words);
  return words;
}

std::vector<PhraseWordStat> PhraseWordStats(const ByPhraseOptions& phrase,
                                            const IndexReader& index,
                                            field_id field) {
  std::vector<PhraseWordStat> words;
  if (!IsAnchored(phrase)) {
    return words;
  }
  words.resize(WordCount(phrase));
  for (const auto& segment : index) {
    if (const auto* reader = segment.field(field)) {
      AddWordStats(phrase, *reader, words);
    }
  }
  return words;
}

bool IsStandalone(const ByPhraseOptions& phrase) noexcept {
  return absl::c_all_of(phrase, [](const auto& info) {
    return std::visit(
      []<typename Part>(const Part& part) {
        if constexpr (std::is_same_v<Part, ByTermOptions> ||
                      std::is_same_v<Part, ShingleOptions> ||
                      std::is_same_v<Part, TermSetOptions> ||
                      std::is_same_v<Part, ByPrefixOptions> ||
                      std::is_same_v<Part, ByRangeOptions>) {
          return true;
        } else if constexpr (std::is_same_v<Part, AutomatonOptions>) {
          return static_cast<bool>(part.source);
        } else if constexpr (std::is_same_v<Part,
                                            LevenshteinAutomatonOptions>) {
          return part.max_terms == 0 && part.source;
        } else {
          return false;
        }
      },
      info.part);
  });
}

bool HasPatternParts(const ByPhraseOptions& phrase) noexcept {
  return absl::c_any_of(phrase, [](const auto& info) {
    return ByPhraseOptions::KindOf(info.part) == SlotKind::Expansion;
  });
}

CompiledPhrase::CompiledPhrase(const ByPhraseOptions& phrase,
                               std::vector<std::vector<bstring>> expanded,
                               std::span<const PhraseWordStat> words)
  : slop{.max = phrase.slop()} {
  SDB_ASSERT(!expanded.empty() || IsStandalone(phrase));
  const auto n = static_cast<uint32_t>(WordCount(phrase));
  slots.resize(n);
  std::vector<uint32_t> term_slots;
  std::vector<std::optional<size_t>> word_of(n);
  uint32_t slot = 0;
  const auto add = [&](auto&& term) {
    terms.emplace_back(std::forward<decltype(term)>(term));
    term_slots.emplace_back(slot);
  };
  const auto add_word = [&](bytes_view word) {
    word_of[slot] = terms.size();
    add(word);
  };
  size_t part = 0;
  for (const auto& info : phrase) {
    slots[slot].offs_min = info.offs_min;
    slots[slot].offs_max = info.offs_max;
    switch (ByPhraseOptions::KindOf(info.part)) {
      case SlotKind::Term: {
        const auto* shingle = std::get_if<ShingleOptions>(&info.part);
        if (!shingle) {
          add_word(std::get<ByTermOptions>(info.part).term);
          break;
        }
        add_word(shingle->words.front());
        for (const auto& word : std::span{shingle->words}.subspan(1)) {
          ++slot;
          slots[slot].offs_min = slots[slot].offs_max = 1;
          add_word(word);
        }
      } break;
      case SlotKind::Set:
        for (const auto& term : std::get<TermSetOptions>(info.part).terms) {
          add(term);
        }
        break;
      case SlotKind::Expansion:
        if (part < expanded.size()) {
          for (auto& term : expanded[part]) {
            add(std::move(term));
          }
        } else if (auto pattern =
                     PatternOf(info.part, phrase.word_separator())) {
          slots[slot].pattern = std::move(pattern);
          pattern_slots.emplace_back(slot);
        }
        break;
    }
    ++slot;
    ++part;
  }
  accept.Reserve(terms);
  for (size_t i = 0; i != terms.size(); ++i) {
    auto& listed = accept[terms[i]].slots;
    if (listed.empty() || listed.back() != term_slots[i]) {
      listed.emplace_back(term_slots[i]);
    }
  }
  for (uint32_t i = 0; i != n; ++i) {
    if (word_of[i]) {
      slots[i].word =
        MakeTermView(ViewCast<char>(bytes_view{terms[*word_of[i]]}));
    }
  }

  if (!IsAnchored(phrase)) {
    if (slop.max != 0 && n > 1) {
      LayoutSlop(*this);
    }
    return;
  }
  LayoutAutomaton(*this);
  anchor = AnchorOf(slots, words);
}

bool AcceptsTerm(const CompiledPhrase& phrase, const CompiledPhrase::Slot& slot,
                 const duckdb::string_t& term) {
  if (slot.pattern) {
    return slot.pattern->Accepts(AsBytesView(term));
  }
  const auto* found = phrase.accept.Find(term);
  return found &&
         absl::c_linear_search(
           found->slots, static_cast<uint32_t>(&slot - phrase.slots.data()));
}

uint64_t MaskOf(const CompiledPhrase& phrase, const duckdb::string_t& term) {
  uint64_t mask = 0;
  if (const auto* found = phrase.accept.Find(term)) {
    mask = found->mask;
  }
  ForEachPattern(phrase, term,
                 [&](uint32_t slot) { mask |= phrase.slots[slot].bit; });
  return mask;
}

}  // namespace irs
