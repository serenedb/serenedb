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

#include <algorithm>

#include "iresearch/utils/assert.hpp"

namespace irs {
namespace {

bool Has(IndexFeatures features, IndexFeatures feature) noexcept {
  return IndexFeatures::None != (features & feature);
}

}  // namespace

TermPartWriter::TermPartWriter(IndexFeatures features) noexcept
  : _has_freq{Has(features, IndexFeatures::Freq)},
    _has_pos{Has(features, IndexFeatures::Pos)},
    _has_offs{Has(features, IndexFeatures::Offs)} {}

void TermPartWriter::Add(bytes_view term, const PostingMeta& meta) {
  SDB_ASSERT(meta.docs_count != 0);
  SDB_ASSERT(_count == 0 || bytes_view{_last} < term);
  SDB_ASSERT(meta.doc_start >= _prev.doc_start);
  const auto shared = static_cast<uint32_t>(
    std::mismatch(_last.begin(),
                  _last.begin() + std::min(_last.size(), term.size()),
                  term.begin())
      .first -
    _last.begin());
  _out.WriteV32(shared);
  _out.WriteV32(static_cast<uint32_t>(term.size() - shared));
  _out.WriteData(term.data() + shared, term.size() - shared);
  _out.WriteV32(meta.docs_count);
  if (_has_freq) {
    SDB_ASSERT(meta.freq >= meta.docs_count);
    _out.WriteV32(meta.freq - meta.docs_count);
  }
  _out.WriteV32(meta.doc_delta);
  _out.WriteByte(meta.inline_size);
  if (meta.inline_size != 0) {
    _out.WriteData(meta.inline_data, meta.inline_size);
  }
  _out.WriteV64(meta.doc_start - _prev.doc_start);
  if (_has_pos) {
    SDB_ASSERT(meta.pos_start >= _prev.pos_start);
    _out.WriteV64(meta.pos_start - _prev.pos_start);
    _out.WriteV32(meta.pos_offset);
    _out.WriteV32(meta.pos_extent);
    if (_has_offs) {
      SDB_ASSERT(meta.pay_start >= _prev.pay_start);
      _out.WriteV64(meta.pay_start - _prev.pay_start);
      _out.WriteV32(meta.pay_extent);
    }
  }
  _last.assign(term);
  _prev.doc_start = meta.doc_start;
  _prev.pos_start = meta.pos_start;
  _prev.pay_start = meta.pay_start;
  _total_doc_freq += meta.docs_count;
  _total_term_freq += meta.freq;
  ++_count;
}

TermPartReader::TermPartReader(bytes_view terms,
                               IndexFeatures features) noexcept
  : _in{terms},
    _has_freq{Has(features, IndexFeatures::Freq)},
    _has_pos{Has(features, IndexFeatures::Pos)},
    _has_offs{Has(features, IndexFeatures::Offs)} {}

bool TermPartReader::Next() {
  if (_in.IsEOF()) {
    return false;
  }
  const auto shared = _in.ReadV32();
  const auto suffix = _in.ReadV32();
  SDB_ASSERT(shared <= _term.size());
  _term.resize(shared);
  _term.append(_in.ReadStable(suffix), suffix);
  _meta.docs_count = _in.ReadV32();
  _meta.freq = _has_freq ? _meta.docs_count + _in.ReadV32() : 0;
  _meta.doc_delta = _in.ReadV32();
  _meta.inline_size = _in.ReadByte();
  _meta.inline_data =
    _meta.inline_size != 0 ? _in.ReadStable(_meta.inline_size) : nullptr;
  _meta.doc_start += _in.ReadV64();
  if (_has_pos) {
    _meta.pos_start += _in.ReadV64();
    _meta.pos_offset = static_cast<uint16_t>(_in.ReadV32());
    _meta.pos_extent = _in.ReadV32();
    if (_has_offs) {
      _meta.pay_start += _in.ReadV64();
      _meta.pay_extent = _in.ReadV32();
    }
  }
  return true;
}

}  // namespace irs
