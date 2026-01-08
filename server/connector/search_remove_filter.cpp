////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "search_remove_filter.hpp"

#include <iresearch/index/index_reader.hpp>

namespace sdb::connector::search {

irs::DocIterator::ptr SearchRemoveFilterBase::execute(
  const irs::ExecutionContext& ctx) const {
  _segment = &ctx.segment;
  _pk_field = _segment->field(kPkFieldName);
  SDB_ASSERT(_pk_field);
  _pos = 0;
  _doc.value = irs::doc_limits::invalid();
  return irs::memory::to_managed<irs::DocIterator>(
    const_cast<SearchRemoveFilterBase&>(*this));
}

irs::doc_id_t SearchRemoveFilter::advance() {
  auto* docs_mask = _segment->docs_mask();

  while (true) {
    if (_pos == _pks.size()) [[unlikely]] {
      _doc.value = irs::doc_limits::eof();
      if (_pks.empty()) [[unlikely]] {
        _pks = {};
      }
      return irs::doc_limits::eof();
    }
    auto& pk = _pks[_pos];

    // Remove all occurences of the PK in segment if any.
    // There is only one alive PK in the entire index.
    // But same value PKs might exist in deleted documents and this number
    // of documents is arbitrary. So we need to check all of them.
    // In general we do not expect too many delete/insert of same PK
    // between consolidations. So postings list should be short.
    auto terms = _pk_field->iterator(irs::SeekMode::RandomOnly);
    if (!terms || !terms->seek(irs::bytes_view(
                    reinterpret_cast<irs::byte_type*>(pk.data()), pk.size()))) {
      // PK not found in this segment
      ++_pos;
      continue;
    }

    auto doc = irs::doc_limits::eof();
    auto docs = _pk_field->postings(*terms->cookie(), irs::IndexFeatures::None);
    while (!irs::doc_limits::eof(doc = docs->advance())) {
      SDB_ASSERT(irs::doc_limits::valid(doc));
      if (!docs_mask || !docs_mask->contains(doc)) {
        break;
      }
    }
    // if PK found alive it should be the only one in the entire index.
    pk = _pks.back();
    _pks.pop_back();
    _doc.value = doc;
    return doc;
  }
}

}  // namespace sdb::connector::search
