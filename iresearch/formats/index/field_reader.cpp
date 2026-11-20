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

#include "iresearch/formats/index/field_reader.hpp"

#include <utility>
#include <vector>

#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::term_dict {

uint64_t FieldReader::CountMappedMemory() const {
  return _pr.CountMappedMemory() + _store.CountMappedMemory();
}

void FieldReader::prepare(const ReaderState& state) {
  SDB_ASSERT(state.idx);
  auto terms = state.idx->TakeTerms();
  _store.Open(state.idx->ReopenIn());
  if (!_store.Opened()) {
    SDB_ENSURE(terms.fields.empty(),
               "term dictionary: fields but no `.idx` stream");
    return;
  }

  const auto& heads = terms.fields;
  Separators separators;
  if (!heads.empty()) {
    _store.Bind(terms);
    if (const auto blocks = _store.BlockCount(); blocks != 0) {
      separators = DecodeSeparators(
        ViewCast<byte_type>(std::string_view{terms.separators}), blocks - 1);
    } else if (!terms.separators.empty()) {
      Corrupted("separators");
    }
  }

  IndexFeatures features = IndexFeatures::None;
  for (const auto& head : heads) {
    features |= head.features;
  }
  _pr.prepare(state, features);

  uint64_t single_inline = 0;
  for (const auto& head : heads) {
    single_inline += head.record_inline.size();
  }
  if (single_inline != 0) {
    _single_inline.reserve(single_inline + kSlack);
    _store.Account(_single_inline.capacity());
  }
  _by_id.reserve(heads.size());
  _dicts.reserve(heads.size());
  bstring prefix;
  uint32_t ordinal = 0;
  for (const auto& head : heads) {
    if (head.term_count == 1) {
      _dicts.push_back(&_singles.emplace_back(*this, head));
    } else {
      const uint32_t own = ordinal++;
      std::vector<const TermReaderImpl*> levels;
      levels.reserve(head.levels.size());
      for (const auto& level : head.levels) {
        prefix.clear();
        AppendOrdinal(prefix, ordinal++, terms.ordinal_bytes);
        levels.push_back(
          &_keyed.emplace_back(*this, level, prefix, separators));
      }
      prefix.clear();
      AppendOrdinal(prefix, own, terms.ordinal_bytes);
      _dicts.push_back(&_keyed.emplace_back(*this, head, prefix, separators,
                                            std::move(levels)));
    }
    ++_by_id
        .try_emplace(
          head.id,
          FieldDicts{.first = static_cast<uint32_t>(_dicts.size() - 1)})
        .first->second.count;
    if (_ids.empty() || _ids.back() != head.id) {
      _ids.push_back(head.id);
    }
  }
  if (single_inline != 0) {
    _single_inline.resize(_single_inline.size() + kSlack);
  }
  _store.FillCopies();
}

const byte_type* FieldReader::KeepInline(bytes_view bytes) {
  SDB_ASSERT(_single_inline.size() + bytes.size() + kSlack <=
             _single_inline.capacity());
  const auto at = _single_inline.size();
  _single_inline.insert(_single_inline.end(), bytes.begin(), bytes.end());
  return _single_inline.data() + at;
}

std::span<const TermReader* const> FieldReader::fields(field_id id) const {
  const auto it = _by_id.find(id);
  if (it == _by_id.end()) {
    return {};
  }
  return std::span{_dicts}.subspan(it->second.first, it->second.count);
}

}  // namespace irs::term_dict
