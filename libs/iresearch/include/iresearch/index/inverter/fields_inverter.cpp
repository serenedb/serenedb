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

// The out-of-line FieldsInverter members live here rather than in a header:
// Flush drives the columnar-flush scatter + burst_trie writer (keeping those
// heavy dependencies out of the widely-included fields_inverter.hpp), and the
// ctor/dtor are out-of-line because ScatterScratch is only complete here.
#include <absl/algorithm/container.h>

#include "iresearch/formats/index/burst_trie.hpp"
#include "iresearch/index/inverter/columnar_readers.hpp"

namespace irs {

FieldsInverter::FieldsInverter(InverterMemory mem)
  : _mem{mem},
    _arena{mem.allocator},
    _fields{ManagedTypedAllocator<FieldInverter>{mem.rm}} {}

FieldsInverter::~FieldsInverter() = default;

void FieldsInverter::Flush(burst_trie::FieldWriter& fw, FlushState& state,
                           std::span<const BasicTermReader* const> extra) {
  if (!_scatter) {
    _scatter = std::make_unique<ScatterScratch>(_mem.rm);
  }
  IndexFeatures index_features{IndexFeatures::None};

  ManagedVector<const FieldInverter*> sorted_fields{
    ManagedTypedAllocator<const FieldInverter*>{_mem.rm}};
  sorted_fields.reserve(_fields.size());
  for (auto& entry : _fields) {
    sorted_fields.push_back(&entry);
    index_features |= static_cast<IndexFeatures>(entry.Meta().index_features);
  }
  for (const auto* reader : extra) {
    index_features |= reader->properties().index_features;
  }
  state.index_features = index_features;

  absl::c_sort(sorted_fields,
               [](const FieldInverter* lhs, const FieldInverter* rhs) noexcept {
                 return lhs->Meta().id < rhs->Meta().id;
               });

  ManagedVector<const BasicTermReader*> sorted_extra{
    ManagedTypedAllocator<const BasicTermReader*>{_mem.rm}};
  sorted_extra.assign(extra.begin(), extra.end());
  absl::c_sort(sorted_extra,
               [](const BasicTermReader* lhs, const BasicTermReader* rhs) {
                 return lhs->id() < rhs->id();
               });

  ScatteredField scattered{_mem, *_scatter};
  ColumnarTermReader terms{_mem.rm};

  Finally release_scratch = [this]() noexcept { _scatter->Release(); };
  fw.prepare(state);
  size_t fi = 0;
  size_t ei = 0;
  const size_t fn = sorted_fields.size();
  const size_t en = sorted_extra.size();
  while (fi < fn || ei < en) {
    const bool take_field =
      ei >= en ||
      (fi < fn && sorted_fields[fi]->Meta().id < sorted_extra[ei]->id());
    if (take_field) {
      scattered.Reset(*sorted_fields[fi]);
      ++fi;
      if (scattered.TermCount() == 0) {
        continue;
      }
      terms.Reset(scattered);
      fw.write(terms);
    } else {
      fw.write(*sorted_extra[ei]);
      ++ei;
    }
  }
  fw.end();
}

}  // namespace irs
