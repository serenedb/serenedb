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
  _segment_mask = ctx.segment.docs_mask();
  _pending_mask = ctx.pending_docs_mask;
  _pk_field = _segment->field(kPkFieldName);
  SDB_ASSERT(_pk_field);
  _pos = 0;
  _doc.value = irs::doc_limits::invalid();
  return irs::memory::to_managed<irs::DocIterator>(
    const_cast<SearchRemoveFilterBase&>(*this));
}

irs::doc_id_t SearchRemoveFilter::advance() {
  while (true) {
    if (_pos == _pks.size()) [[unlikely]] {
      _doc.value = irs::doc_limits::eof();
      if (_pks.empty()) [[unlikely]] {
        _pks = {};
      }
      return irs::doc_limits::eof();
    }
    auto& pk = _pks[_pos];

    // Remove all occurrences of the PK in segment if any.
    // There is only one alive PK in the entire index. In general
    // that means we can remove pk from list once we found it.
    // But there are some edge cases:
    // 1. Same value PKs might exist in deleted documents and this number
    // of documents is arbitrary. So we need to check all of them.
    // In general we do not expect too many delete/insert of same PK
    // between consolidations. So postings list should be short.

    // 2. Also we might have Delete/Insert sequence in a single batch,
    // So we must check pending docs mask as well in order to not fire on
    // already deleted documents by queries in the same batch. Also Removals
    // might be skipped during flushed segment processing due to ticks (e.g.
    // remove arrived before insert) but that is not a problem. As if we reached
    // "tick" limit we anyway should not find anymore valid targets.

    // 3. For segments with sorted field it should also work:
    // E.G. if we have INSERT PK1 FIELD_SORTED_2 | DELETE PK1 | INSERT PK1
    // FIELD_SORTED_1 Due to documents are sorted for storing after applying
    // queries it will still see documents in insertion order.

    auto doc = irs::doc_limits::eof();
    auto acceptor = [&](irs::doc_id_t found_doc) {
      if ((_segment_mask && _segment_mask->contains(found_doc)) ||
          (_pending_mask && _pending_mask->contains(found_doc))) {
        return true;  // skip deleted
      }
      // found alive document with this PK
      doc = found_doc;
      return false;
    };

    _pk_field->read_documents(pk, acceptor);

    if (irs::doc_limits::eof(doc)) {
      ++_pos;
      continue;
    }

    // if PK found alive it should be the only one in the entire index.
    pk = _pks.back();
    _pks.pop_back();
    _doc.value = doc;
    return doc;
  }
}

}  // namespace sdb::connector::search
