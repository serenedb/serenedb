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

namespace {

using namespace irs;

struct NoopTermCollector final : TermCollector {
  void collect(const SubReader&, const TermReader&,
               const AttributeProvider&) final {}
  void reset() final {}
  void collect(bytes_view) final {}
  void write(DataOutput&) const final {}
};

static NoopTermCollector gNoopTermStats;

}  // namespace
namespace irs {

void FieldCollector::Data::collect(const TermReader& field) noexcept {
  docs_with_field += field.docs_count();
  if (const auto* freq = irs::get<FreqAttr>(field)) {
    total_term_freq += freq->value;
  }
}

TermCollectorWrapper::collector_type& TermCollectorWrapper::noop() noexcept {
  return gNoopTermStats;
}

TermCollectors::TermCollectors(const Scorer* scorer, size_t size)
  : CollectorsBase<TermCollectorWrapper>{scorer ? size : size_t{0}, scorer} {
  for (auto& collector : _collectors) {
    collector = scorer->PrepareTermCollector();
    SDB_ASSERT(collector);
  }
}

void TermCollectors::collect(const SubReader& segment, const TermReader& field,
                             size_t term_idx,
                             const AttributeProvider& attrs) const {
  if (_scorer) {
    SDB_ASSERT(term_idx < _collectors.size());
    SDB_ASSERT(_collectors[term_idx]);
    _collectors[term_idx]->collect(segment, field, attrs);
  }
}

size_t TermCollectors::push_back() {
  if (!_scorer) {
    return 0;
  }
  const auto term_offset = _collectors.size();
  _collectors.emplace_back(_scorer->PrepareTermCollector());
  return term_offset;
}

void TermCollectors::finish(byte_type* stats_buf, size_t term_idx,
                            const FieldCollector::Data* field_data,
                            const IndexReader& /*index*/) const {
  if (_scorer) {
    SDB_ASSERT(field_data);
    _scorer->collect(stats_buf, field_data, _collectors[term_idx].get());
  }
}

}  // namespace irs
