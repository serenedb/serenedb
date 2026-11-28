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

#include <duckdb/storage/external_file_cache/input_file_reader.hpp>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::term_dict {

void FieldReader::prepare(const ReaderState& state) {
  SDB_ASSERT(state.idx);
  auto terms = state.idx->TakeTerms();
  _store.Open(state.idx->File(),
              duckdb::BufferManager::GetBufferManager(state.db));
  if (!_store.Opened()) {
    SDB_ENSURE(terms.fields.empty(),
               "term dictionary: fields but no `.idx` stream");
    return;
  }

  const auto& heads = terms.fields;
  const auto& separators = _separators;
  duckdb::InputFileReader in{_store.Input()};
  if (!heads.empty()) {
    _store.Bind(terms);
    in.Seek(terms.separators.offset);
    ReadArray(in, _separators.offsets);
    ReadArray(in, _separators.arena);
    _store.Account(SeparatorBytes(separators));
  }

  _pr.prepare(state);

  _by_id.reserve(heads.size());
  _dicts.reserve(heads.size());
  size_t keyed = 0;
  for (const auto& head : heads) {
    keyed += head.term_count == 1 ? 0 : 1 + head.levels.size();
  }
  _keyed.reserve(keyed);
  bstring prefix;
  uint32_t ordinal = 0;
  for (const auto& head : heads) {
    if (head.term_count == 1) {
      _dicts.push_back(&_singles.emplace_back(*this, head));
    } else {
      const uint32_t own = ordinal++;
      const auto first = _keyed.size();
      for (const auto& level : head.levels) {
        prefix.clear();
        AppendOrdinal(prefix, ordinal++, terms.ordinal_bytes);
        in.Seek(level.index.offset);
        _keyed.emplace_back(*this, level, prefix, separators, in);
      }
      const std::span<const TermReaderImpl> levels{_keyed.data() + first,
                                                   head.levels.size()};
      prefix.clear();
      AppendOrdinal(prefix, own, terms.ordinal_bytes);
      in.Seek(head.index.offset);
      _dicts.push_back(
        &_keyed.emplace_back(*this, head, prefix, separators, in, levels));
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
  _store.Finish();
}

std::span<const TermReader* const> FieldReader::fields(field_id id) const {
  const auto it = _by_id.find(id);
  if (it == _by_id.end()) {
    return {};
  }
  return std::span{_dicts}.subspan(it->second.first, it->second.count);
}

}  // namespace irs::term_dict
