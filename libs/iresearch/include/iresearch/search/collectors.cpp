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

void FieldCollector::Collect(const TermReader& field) noexcept {
  docs_with_field += field.docs_count();
  if (const auto* freq = irs::get<FreqAttr>(field)) {
    total_term_freq += freq->value;
  }
}

void FieldCollector::Merge(FieldCollector& dst, const FieldCollector& src) {
  dst.docs_with_field += src.docs_with_field;
  dst.total_term_freq += src.total_term_freq;
}

void TermCollector::Collect(const AttributeProvider& term_attrs) noexcept {
  if (const auto* meta = irs::get<TermMeta>(term_attrs)) {
    docs_with_term += meta->docs_count;
    total_term_freq += meta->freq;
  }
}

void TermCollector::Merge(TermCollector& dst, const TermCollector& src) {
  dst.docs_with_term += src.docs_with_term;
  dst.total_term_freq += src.total_term_freq;
}

void FillStats(bstring& stats_buf, const Scorer* scorer,
               const FieldCollector* field, const TermCollector* term) {
  if (!scorer) {
    return;
  }
  SDB_ASSERT(field);
  stats_buf.resize(GetStatsSize(scorer));
  scorer->collect(stats_buf.data(), field, term);
}

void FieldPrepareCollector::Merge(PrepareCollector&& other) {
  auto& field = sdb::basics::downCast<FieldPrepareCollector>(other);
  FieldCollector::Merge(_field, field._field);
}

StatsBuffer FieldPrepareCollector::Finish(IResourceManager& memory) {
  StatsBuffer::Storage stats{{memory}};
  if (_scorer) {
    bstring stat;
    FillStats(stat, _scorer, &_field, nullptr);
    stats.emplace_back(std::move(stat));
  }
  return StatsBuffer{std::move(stats), _scorer};
}

void ByTermsCollector::Merge(PrepareCollector&& other) {
  auto& rhs = sdb::basics::downCast<ByTermsCollector>(other);
  FieldCollector::Merge(_field, rhs._field);
  SDB_ASSERT(_terms.size() == rhs._terms.size());
  for (size_t i = 0, size = _terms.size(); i < size; ++i) {
    TermCollector::Merge(_terms[i], rhs._terms[i]);
  }
}

StatsBuffer ByTermsCollector::Finish(IResourceManager& memory) {
  StatsBuffer::Storage stats{{memory}};
  if (_scorer) {
    stats.reserve(_terms.size());
    for (auto& term : _terms) {
      bstring stat;
      FillStats(stat, _scorer, &_field, &term);
      stats.emplace_back(std::move(stat));
    }
  }
  return StatsBuffer{std::move(stats), _scorer};
}

void PhraseCollector::Merge(PrepareCollector&& other) {
  auto& rhs = sdb::basics::downCast<PhraseCollector>(other);
  FieldCollector::Merge(_field, rhs._field);
  for (size_t i = 0, size = _parts.size(); i < size; ++i) {
    auto& dst = _parts[i];
    auto& src = rhs._parts[i];
    if (dst.size() < src.size()) {
      dst.resize(src.size());
    }
    for (size_t j = 0, n = src.size(); j < n; ++j) {
      TermCollector::Merge(dst[j], src[j]);
    }
  }
}

StatsBuffer PhraseCollector::Finish(IResourceManager& memory) {
  StatsBuffer::Storage stats{{memory}};
  if (_scorer) {
    bstring stat(GetStatsSize(_scorer), 0);
    for (auto& part : _parts) {
      for (auto& term : part) {
        FillStats(stat, _scorer, &_field, &term);
      }
    }
    stats.emplace_back(std::move(stat));
  }
  return StatsBuffer{std::move(stats), _scorer};
}

StatsBuffer AllCollector::Finish(IResourceManager& memory) {
  StatsBuffer::Storage stats{{memory}};
  if (_scorer) {
    bstring stat(GetStatsSize(_scorer), 0);
    _scorer->collect(stat.data(), nullptr, nullptr);
    stats.emplace_back(std::move(stat));
  }
  return StatsBuffer{std::move(stats), _scorer};
}

StatsBuffer NoopCollector::Finish(IResourceManager& memory) {
  return StatsBuffer{StatsBuffer::Storage{{memory}}, nullptr};
}

void CompoundCollector::Merge(PrepareCollector&& other) {
  auto& rhs = sdb::basics::downCast<CompoundCollector>(other);
  SDB_ASSERT(_children.size() == rhs._children.size());
  for (size_t i = 0, size = _children.size(); i < size; ++i) {
    _children[i]->Merge(std::move(*rhs._children[i]));
  }
}

void CompoundCollector::MergeAll(MergeVisitor visit) {
  for (size_t i = 0, size = _children.size(); i < size; ++i) {
    _children[i]->MergeAll([&, i](MergeSink sink) {
      visit([&](PrepareCollector& other) {
        auto& rhs = sdb::basics::downCast<CompoundCollector>(other);
        SDB_ASSERT(_children.size() == rhs._children.size());
        sink(*rhs._children[i]);
      });
    });
  }
}

StatsBuffer CompoundCollector::Finish(IResourceManager& memory) {
  StatsBuffer stats{StatsBuffer::Storage{{memory}}, _scorer};
  for (auto& child : _children) {
    stats.AddChild(child->Finish(memory));
  }
  return stats;
}

}  // namespace irs
