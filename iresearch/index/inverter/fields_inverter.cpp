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

FieldsInverter::FlushNeeds FieldsInverter::Needs() const noexcept {
  FlushNeeds needs;
  for (const auto& field : _fields) {
    if (!HasTerms(&field)) {
      continue;
    }
    const auto terms = field.Dictionary().Size();
    needs.max_terms = std::max(needs.max_terms, terms);
    needs.terms += terms;
    needs.blocks =
      std::max(needs.blocks, ScatteredField::Blocks(field, field.Log().Size()));
    needs.ranges = needs.ranges || IsSubsetOf(IndexFeatures::Ranges,
                                              field.Meta().index_features);
  }
  needs.terms += _docs.size();
  return needs;
}

size_t FieldsInverter::FlushBound(size_t docs,
                                  size_t extra_terms) const noexcept {
  const auto needs = Needs();
  return ScatterScratch::Bytes(needs.max_terms, needs.blocks) +
         term_dict::FieldWriter::Bound(needs.terms + extra_terms, docs,
                                       needs.ranges);
}

void FieldsInverter::Flush(term_dict::FieldWriter& fw, FlushState& state,
                           std::span<const BasicTermReader* const> extra,
                           uint64_t extra_terms) {
  IndexFeatures index_features{IndexFeatures::None};
  uint64_t keyed_dicts = extra.size();

  std::vector<const FieldsMap::value_type*> sorted;
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

  std::vector<const BasicTermReader*> sorted_extra;
  sorted_extra.assign(extra.begin(), extra.end());
  absl::c_sort(sorted_extra,
               [](const BasicTermReader* lhs, const BasicTermReader* rhs) {
                 return lhs->id() < rhs->id();
               });

  const auto needs = Needs();
  InverterMemory mem{state.budget ? state.budget->Allocator() : _mem.allocator};
  ScatterScratch scratch{state.budget};
  scratch.Reserve(needs.max_terms, needs.blocks, mem.allocator);
  ScatteredField scattered{mem, scratch};
  ColumnarTermReader terms;

  fw.prepare(state, keyed_dicts, needs.terms + extra_terms);
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
          fw.WriteSlot(id, dict, {docs.data(), docs.size()});
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
