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

#include "iresearch/formats/index/term_parts.hpp"

#include <absl/algorithm/container.h>

#include <iterator>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs {

TermPartWriter::TermPartWriter(IndexFeatures features) noexcept
  : _has_freq{IsSubsetOf(IndexFeatures::Freq, features)},
    _has_pos{IsSubsetOf(IndexFeatures::Pos, features)},
    _has_offs{IsSubsetOf(IndexFeatures::Offs, features)} {
  SDB_ASSERT(!IsSubsetOf(IndexFeatures::Vec, features));
}

void TermPartWriter::Add(bytes_view term, const PostingMeta& meta) {
  SDB_ASSERT(meta.docs_count != 0);
  SDB_ASSERT(_count == 0 || bytes_view{_last} < term);
  SDB_ASSERT(meta.doc_start >= _prev.doc);
  const auto shared =
    static_cast<uint32_t>(absl::c_mismatch(_last, term).first - _last.begin());
  auto out = std::back_inserter(_terms);
  WriteVarint(shared, out);
  WriteVarint(static_cast<uint32_t>(term.size() - shared), out);
  _terms.append(term.substr(shared));
  WriteVarint(static_cast<uint32_t>(meta.docs_count), out);
  if (_has_freq) {
    SDB_ASSERT(meta.freq >= meta.docs_count);
    WriteVarint(static_cast<uint32_t>(meta.freq - meta.docs_count), out);
  }
  WriteVarint(static_cast<uint32_t>(meta.doc_delta), out);
  _terms.push_back(meta.inline_size);
  if (meta.inline_size != 0) {
    _terms.append(meta.inline_data, meta.inline_size);
  }
  WriteVarint(static_cast<uint64_t>(meta.doc_start - _prev.doc), out);
  if (_has_pos) {
    SDB_ASSERT(meta.pos_start >= _prev.pos);
    WriteVarint(static_cast<uint64_t>(meta.pos_start - _prev.pos), out);
    WriteVarint(static_cast<uint32_t>(meta.pos_offset), out);
    WriteVarint(static_cast<uint32_t>(meta.pos_extent), out);
    if (_has_offs) {
      SDB_ASSERT(meta.pay_start >= _prev.pay);
      WriteVarint(static_cast<uint64_t>(meta.pay_start - _prev.pay), out);
      WriteVarint(static_cast<uint32_t>(meta.pay_extent), out);
    }
  }
  _last.assign(term);
  _prev = {
    .doc = meta.doc_start,
    .pos = meta.pos_start,
    .pay = meta.pay_start,
  };
  _total_doc_freq += meta.docs_count;
  _total_term_freq += meta.freq;
  _inline_bytes += meta.inline_size;
  ++_count;
}

TermPartReader::TermPartReader(bytes_view terms, IndexFeatures features,
                               PostingBases bases) noexcept
  : _in{terms.data()},
    _end{terms.data() + terms.size()},
    _meta{
      .doc_start = bases.doc,
      .pos_start = bases.pos,
      .pay_start = bases.pay,
    },
    _has_freq{IsSubsetOf(IndexFeatures::Freq, features)},
    _has_pos{IsSubsetOf(IndexFeatures::Pos, features)},
    _has_offs{IsSubsetOf(IndexFeatures::Offs, features)} {
  SDB_ASSERT(!IsSubsetOf(IndexFeatures::Vec, features));
}

bool TermPartReader::Next() {
  if (_in == _end) {
    return false;
  }
  const auto shared = vread<uint32_t>(_in);
  const auto suffix = vread<uint32_t>(_in);
  SDB_ASSERT(shared <= _term.size());
  _term.resize(shared);
  _term.append(_in, suffix);
  _in += suffix;
  _meta.docs_count = vread<uint32_t>(_in);
  _meta.freq = _has_freq ? _meta.docs_count + vread<uint32_t>(_in) : 0;
  _meta.doc_delta = vread<uint32_t>(_in);
  _meta.inline_size = *_in++;
  _meta.inline_data = _meta.inline_size != 0 ? _in : nullptr;
  _in += _meta.inline_size;
  _meta.doc_start += vread<uint64_t>(_in);
  if (_has_pos) {
    _meta.pos_start += vread<uint64_t>(_in);
    _meta.pos_offset = static_cast<uint16_t>(vread<uint32_t>(_in));
    _meta.pos_extent = vread<uint32_t>(_in);
    if (_has_offs) {
      _meta.pay_start += vread<uint64_t>(_in);
      _meta.pay_extent = vread<uint32_t>(_in);
    }
  }
  SDB_ASSERT(_in <= _end);
  return true;
}

}  // namespace irs
