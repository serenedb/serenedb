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

#include <absl/algorithm/container.h>

#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/field_writer.hpp"
#include "iresearch/index/inverter/columnar_readers.hpp"

namespace irs {
namespace {

bool HasTerms(const FieldInverter* field) noexcept {
  return field && field->Log().Size() != 0;
}

}  // namespace

void FieldsInverter::Flush(term_dict::FieldWriter& fw, FlushState& state,
                           std::span<const BasicTermReader* const> extra) {
  IndexFeatures index_features{IndexFeatures::None};
  uint64_t keyed_dicts = extra.size();

  ManagedVector<const FieldsMap::value_type*> sorted{
    ManagedTypedAllocator<const FieldsMap::value_type*>{_mem.rm}};
  sorted.reserve(_fields_map.size());
  for (const auto& item : _fields_map) {
    const auto& entry = item.second;
    bool used = false;
    for (const auto member : LayoutMembers(entry.layout)) {
      if (!IsTermMember(member)) {
        used = used || !_docs[entry.docs + DocIndex(member)].empty();
      } else if (const auto* field =
                   _terms[entry.terms + TermIndex(member)].field;
                 HasTerms(field)) {
        keyed_dicts += field->Dictionary().Size() != 1;
        index_features |= field->Meta().index_features;
        used = true;
      }
    }
    if (used) {
      sorted.push_back(&item);
    }
  }
  for (const auto* reader : extra) {
    index_features |= reader->properties().index_features;
  }
  state.index_features = index_features;

  absl::c_sort(sorted, [](const FieldsMap::value_type* lhs,
                          const FieldsMap::value_type* rhs) noexcept {
    return lhs->first < rhs->first;
  });

  ManagedVector<const BasicTermReader*> sorted_extra{
    ManagedTypedAllocator<const BasicTermReader*>{_mem.rm}};
  sorted_extra.assign(extra.begin(), extra.end());
  absl::c_sort(sorted_extra,
               [](const BasicTermReader* lhs, const BasicTermReader* rhs) {
                 return lhs->id() < rhs->id();
               });

  ScatterScratch scratch{_mem.rm};
  ScatteredField scattered{_mem, scratch};
  ColumnarTermReader terms;
  SlotTermReader slot_terms;

  fw.prepare(state, keyed_dicts);
  size_t ei = 0;
  for (const auto* item : sorted) {
    const auto id = item->first;
    const auto& entry = item->second;
    for (; ei < sorted_extra.size() && sorted_extra[ei]->id() < id; ++ei) {
      fw.write(StringLayout::kStrings, *sorted_extra[ei]);
    }
    const bool has_extra =
      ei < sorted_extra.size() && sorted_extra[ei]->id() == id;
    SDB_ASSERT(!has_extra || entry.layout == DictLayout::String);
    for (const auto member : LayoutMembers(entry.layout)) {
      const DictRef dict{.layout = entry.layout, .member = member};
      if (!IsTermMember(member)) {
        if (const auto& docs = _docs[entry.docs + DocIndex(member)];
            !docs.empty()) {
          slot_terms.Reset(id, {docs.data(), docs.size()});
          fw.write(dict, slot_terms);
        }
        continue;
      }
      const auto* field = _terms[entry.terms + TermIndex(member)].field;
      if (has_extra && dict == StringLayout::kStrings) {
        SDB_ASSERT(!HasTerms(field));
        fw.write(dict, *sorted_extra[ei++]);
      } else if (HasTerms(field)) {
        scattered.Reset(*field);
        SDB_ASSERT(scattered.TermCount() != 0);
        terms.Reset(scattered);
        fw.write(dict, terms);
      }
    }
  }
  for (; ei < sorted_extra.size(); ++ei) {
    fw.write(StringLayout::kStrings, *sorted_extra[ei]);
  }
  fw.end();
}

}  // namespace irs
