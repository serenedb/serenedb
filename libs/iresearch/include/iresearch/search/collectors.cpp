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

namespace {

using namespace irs;

struct NoopFieldCollector final : FieldCollector {
  void collect(const SubReader&, const TermReader&) final {}
  void reset() final {}
  void collect(bytes_view) final {}
  void write(DataOutput&) const final {}
};

struct NoopTermCollector final : TermCollector {
  void collect(const SubReader&, const TermReader&,
               const AttributeProvider&) final {}
  void reset() final {}
  void collect(bytes_view) final {}
  void write(DataOutput&) const final {}
};

static NoopFieldCollector gNoopFieldStats;
static NoopTermCollector gNoopTermStats;

}  // namespace

namespace irs {

FieldCollectorWrapper::collector_type& FieldCollectorWrapper::noop() noexcept {
  return gNoopFieldStats;
}

FieldCollectors::FieldCollectors(const Scorer* scorer)
  : CollectorsBase<FieldCollectorWrapper>{size_t{scorer != nullptr}, scorer} {
  if (scorer) {
    _collectors.front() = scorer->PrepareFieldCollector();
    SDB_ASSERT(_collectors.front());
  }
}

void FieldCollectors::collect(const SubReader& segment,
                              const TermReader& field) const {
  if (!_collectors.empty()) {
    _collectors.front()->collect(segment, field);
  }
}

void FieldCollectors::finish(byte_type* stats_buf) const {
  // special case where term statistics collection is not applicable
  // e.g. by_column_existence filter
  if (_scorer) {
    SDB_ASSERT(_collectors.size() == 1);
    _scorer->collect(stats_buf, _collectors.front().get(), nullptr);
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
                            const FieldCollectors& field_collectors,
                            const IndexReader& /*index*/) const {
  if (_scorer) {
    SDB_ASSERT(field_collectors.front());
    _scorer->collect(stats_buf, field_collectors.front(),
                     _collectors[term_idx].get());
  }
}

}  // namespace irs
