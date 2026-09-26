////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "collectors.hpp"

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"

namespace irs {
namespace {

byte_type* Mutable(const byte_type* stats) noexcept {
  return const_cast<byte_type*>(stats);
}

}  // namespace

void FieldCollector::Collect(const TermReader& field) noexcept {
  docs_with_field += field.docs_count();
  if (const auto* freq = irs::get<FreqAttr>(field)) {
    total_term_freq += freq->value;
  }
}

void FieldPrepareCollector::Finish(StatsArena&) {
  if (_scorer == nullptr) {
    return;
  }
  const auto field = _counters.TotalField();
  _scorer->collect(Mutable(_stats), &field, nullptr);
}

ByTermsCollector::ByTermsCollector(const Scorer* scorer, size_t size,
                                   StatsArena& stats, uint32_t threads)
  : FieldPrepareCollector{scorer, stats, threads, size, false},
    _size{size},
    _slot{StatsSlot(scorer)} {
  SDB_ASSERT(size != 0);
  _stats = stats.Allocate(_slot * size);
}

void ByTermsCollector::Finish(StatsArena&) {
  if (_scorer == nullptr) {
    return;
  }
  const auto field = _counters.TotalField();
  for (size_t i = 0; i != _size; ++i) {
    const auto term = _counters.TotalTerm(i);
    _scorer->collect(Mutable(_stats) + i * _slot, &field, &term);
  }
}

void SlotsCollector::Finish(StatsArena&) {
  if (_scorer == nullptr) {
    return;
  }
  const auto field = _counters.TotalField();
  auto* const slot = Mutable(_stats);
  for (size_t i = 0, n = _counters.Terms(); i != n; ++i) {
    const auto term = _counters.TotalTerm(i);
    if (term.docs_with_term == 0) {
      continue;
    }
    _scorer->collect(slot, &field, &term);
  }
}

void ExpandedSlotsCollector::Finish(StatsArena& stats) {
  SlotsCollector::Finish(stats);
  if (_scorer == nullptr) {
    return;
  }
  const auto threads = _counters.Threads();
  const auto field = _counters.TotalField();
  auto* const slot = Mutable(_stats);
  Terms merged;
  std::vector<const Terms::value_type*> ordered;
  for (size_t i = 0; i != _expanded_size; ++i) {
    merged.clear();
    for (uint32_t t = 0; t != threads; ++t) {
      for (const auto& [term, counter] : Expanded(t, i)) {
        auto& one = merged[term];
        one.docs_with_term += counter.docs_with_term;
        one.total_term_freq += counter.total_term_freq;
      }
    }
    ordered.clear();
    for (const auto& entry : merged) {
      ordered.push_back(&entry);
    }
    absl::c_sort(ordered, [](const auto* lhs, const auto* rhs) {
      return lhs->first < rhs->first;
    });
    for (const auto* entry : ordered) {
      _scorer->collect(slot, &field, &entry->second);
    }
  }
}

void AllCollector::Finish(StatsArena&) {
  if (_scorer == nullptr) {
    return;
  }
  _scorer->collect(Mutable(_stats), nullptr, nullptr);
}

void CompoundCollector::Finish(StatsArena& stats) {
  for (auto& child : _children) {
    if (child != nullptr) {
      child->Finish(stats);
    }
  }
}

}  // namespace irs
