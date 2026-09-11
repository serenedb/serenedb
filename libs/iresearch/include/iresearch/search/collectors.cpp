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

void PhraseCollector::Finish(StatsArena&) {
  if (_scorer == nullptr) {
    return;
  }
  const auto threads = _counters.Threads();
  const auto field = _counters.TotalField();
  auto* const slot = Mutable(_stats);
  for (size_t p = 0; p != _size; ++p) {
    size_t terms = 0;
    for (uint32_t t = 0; t != threads; ++t) {
      terms = std::max(terms, Part(t, p).size());
    }
    for (size_t i = 0; i != terms; ++i) {
      TermCollector term;
      for (uint32_t t = 0; t != threads; ++t) {
        const auto& part = Part(t, p);
        if (i < part.size()) {
          term.docs_with_term += part[i].docs_with_term;
          term.total_term_freq += part[i].total_term_freq;
        }
      }
      _scorer->collect(slot, &field, &term);
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
