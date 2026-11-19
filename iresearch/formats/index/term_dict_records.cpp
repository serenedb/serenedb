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

#include "iresearch/formats/index/term_dict_records.hpp"

#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::term_dict {
namespace {

uint64_t VSize(uint64_t value) noexcept {
  return bytes_io<uint64_t, sizeof(uint64_t)>::vsize(value);
}

uint32_t Bits(uint64_t max) noexcept {
  const auto bits = static_cast<uint32_t>(std::bit_width(max));
  SDB_ASSERT(bits <= kMaxRecordBits);
  return bits;
}

uint64_t Anchors(uint64_t count) noexcept {
  return count == 0 ? 0 : (count - 1) / kRecordAnchor;
}

}  // namespace

RunView ParseRun(const byte_type* p, PostingFeatures features) {
  RunView view;
  view.count = vread<uint32_t>(p);
  const byte_type shape = *p++;
  const bool singles = (shape & kRunSingles) != 0;
  const bool multis = (shape & kRunMultis) != 0;
  const bool inlines = (shape & kRunInline) != 0;
  const bool stored = (shape & kRunStored) != 0;
  if (view.count == 0 || (shape & ~kRunKinds) != 0 || (!singles && !multis) ||
      multis != (inlines || stored)) [[unlikely]] {
    Corrupted("posting run");
  }
  const auto bits = [&] {
    const uint32_t value = *p++;
    if (value > kMaxRecordBits) [[unlikely]] {
      Corrupted("posting run width");
    }
    return value;
  };
  const auto sparse = [&](Sparse& column, uint32_t values) {
    column.count = vread<uint32_t>(p);
    if (column.count != 0) {
      column.keys.bits = bits();
      for (uint32_t v = 0; v != values; ++v) {
        column.values[v].bits = bits();
      }
    }
  };
  if (singles) {
    view.single_base = vread<uint64_t>(p);
    view.singles.bits = bits();
  }
  if (multis) {
    view.docs.bits = bits();
  }
  if (features.freq) {
    view.excess.bits = bits();
  }
  if (inlines) {
    view.sizes.bits = bits();
    view.inline_anchors.bits = bits();
  }
  if (stored) {
    view.start_base = vread<uint64_t>(p);
    view.starts.bits = bits();
    sparse(view.big, 1);
  }
  if (features.pos) {
    view.pos_start = vread<uint64_t>(p);
    view.pos_offset = vread<uint32_t>(p);
    if (view.pos_offset >= PosGroup::kPositions) [[unlikely]] {
      Corrupted("posting run position");
    }
    if (features.offs) {
      view.pay_start = vread<uint64_t>(p);
    }
    view.freq_anchors.bits = bits();
    sparse(view.crossings, PositionValues(features));
    sparse(view.extents, PositionValues(features));
  } else if (features.vec) {
    view.vec_base = vread<uint64_t>(p);
    view.vec_starts.bits = bits();
    view.lanes.bits = bits();
  }

  const uint32_t count = view.count;
  if (singles && multis) {
    view.multi_bits = p;
    p += PackedBytes(count, 1);
    view.multis = RankAt(view.multi_bits, count);
    if (view.multis == 0 || view.multis == count) [[unlikely]] {
      Corrupted("posting run kinds");
    }
  } else {
    view.multis = multis ? count : 0;
  }
  if (inlines && stored) {
    view.inline_bits = p;
    p += PackedBytes(view.multis, 1);
    view.inlines = RankAt(view.inline_bits, view.multis);
    if (view.inlines == 0 || view.inlines == view.multis) [[unlikely]] {
      Corrupted("posting run kinds");
    }
  } else {
    view.inlines = inlines ? view.multis : 0;
  }
  const uint32_t stored_count = view.multis - view.inlines;
  const auto column = [&](Packed& packed, uint64_t size) {
    packed.data = p;
    p += PackedBytes(size, packed.bits);
  };
  const auto sparse_data = [&](Sparse& sparse, uint32_t domain,
                               uint32_t values) {
    if (sparse.count > domain) [[unlikely]] {
      Corrupted("posting run sparse column");
    }
    column(sparse.keys, sparse.count);
    for (uint32_t v = 0; v != values; ++v) {
      column(sparse.values[v], sparse.count);
    }
  };
  column(view.singles, count - view.multis);
  column(view.docs, view.multis);
  column(view.excess, features.freq ? count : 0);
  column(view.sizes, view.inlines);
  column(view.inline_anchors, Anchors(view.inlines));
  column(view.starts, stored_count);
  sparse_data(view.big, stored_count, 1);
  if (features.pos) {
    column(view.freq_anchors, Anchors(count));
    sparse_data(view.crossings, count, PositionValues(features));
    sparse_data(view.extents, count, PositionValues(features));
  } else if (features.vec) {
    column(view.vec_starts, count);
    column(view.lanes, count);
  }
  view.inline_area = p;
  return view;
}

void RecordRun::Clear() noexcept {
  _stats = {};
  _multi_bits.clear();
  _inline_bits.clear();
  _singles.clear();
  _docs.clear();
  _excess.clear();
  _sizes.clear();
  _inline_anchors.clear();
  _inline.clear();
  _starts.clear();
  _freq_anchors.clear();
  _vec_starts.clear();
  _lanes.clear();
  _big.Clear();
  _crossings.Clear();
  _extents.Clear();
}

RecordRun::Stats RecordRun::Stage(const PostingMeta& meta) const {
  SDB_ASSERT(meta.docs_count != 0);
  Stats s = _stats;
  const uint32_t i = s.count++;
  if (meta.docs_count == 1) {
    SDB_ASSERT(meta.inline_size == 0);
    s.singles.Add(meta.doc_delta);
  } else {
    const uint32_t stored = s.multis - s.inlines;
    ++s.multis;
    s.docs = std::max<uint64_t>(s.docs, meta.docs_count - 2);
    if (meta.inline_size != 0) {
      if (s.inlines != 0 && s.inlines % kRecordAnchor == 0) {
        s.inline_anchor = s.inline_bytes;
      }
      ++s.inlines;
      s.sizes = std::max<uint64_t>(s.sizes, meta.inline_size - 1);
      s.inline_bytes += meta.inline_size;
    } else {
      s.starts.Add(meta.doc_start);
      if (meta.docs_count > doc_limits::kBlockSize) {
        s.big.Add(stored, meta.doc_delta, 0);
      }
    }
  }
  if (_features.freq) {
    SDB_ASSERT(meta.freq >= meta.docs_count);
    s.excess = std::max<uint64_t>(s.excess, meta.freq - meta.docs_count);
  }
  if (_features.pos) {
    if (i == 0) {
      s.pos_start = meta.pos_start;
      s.pay_start = meta.pay_start;
      s.pos_offset = meta.pos_offset;
    } else {
      const uint64_t at = s.pos_offset + s.freq;
      const uint64_t group = at / PosGroup::kPositions;
      const bool crossed = group != s.last_group;
      SDB_ENSURE(
        meta.pos_offset == at % PosGroup::kPositions &&
          crossed == (meta.pos_start != s.pos_start + s.crossings.max[0]) &&
          (crossed || meta.pay_start == s.pay_start + s.crossings.max[1]),
        "term dictionary: positions of a term do not follow the previous "
        "term's");
      if (crossed) {
        s.crossings.Add(i, meta.pos_start - s.pos_start,
                        _features.offs ? meta.pay_start - s.pay_start : 0);
      }
      s.last_group = group;
    }
    if (i != 0 && i % kRecordAnchor == 0) {
      s.freq_anchor = s.freq;
    }
    s.freq += meta.freq;
    if (meta.freq > pos_limits::kBlockSize) {
      s.extents.Add(i, meta.pos_extent, _features.offs ? meta.pay_extent : 0);
    }
  } else if (_features.vec) {
    s.vec_starts.Add(meta.pay_start);
    s.lanes = std::max<uint64_t>(s.lanes, meta.pos_offset);
  }
  return s;
}

void RecordRun::Commit(const PostingMeta& meta, const Stats& staged) {
  const auto& old = _stats;
  const auto& s = staged;
  SDB_ASSERT(s.count == old.count + 1);
  const uint32_t i = old.count;
  const bool multi = meta.docs_count != 1;
  _multi_bits.push_back(uint8_t{multi});
  if (!multi) {
    _singles.push_back(meta.doc_delta);
  } else {
    _docs.push_back(meta.docs_count - 2);
    const bool inlined = meta.inline_size != 0;
    _inline_bits.push_back(uint8_t{inlined});
    if (inlined) {
      _sizes.push_back(meta.inline_size - 1);
      _inline.append(meta.Inline());
      if (old.inlines != 0 && old.inlines % kRecordAnchor == 0) {
        _inline_anchors.push_back(old.inline_bytes);
      }
    } else {
      _starts.push_back(meta.doc_start);
      if (s.big.count != old.big.count) {
        _big.Add(s.big.last_key, meta.doc_delta, 0);
      }
    }
  }
  if (_features.freq) {
    _excess.push_back(meta.freq - meta.docs_count);
  }
  if (_features.pos) {
    if (i != 0 && i % kRecordAnchor == 0) {
      _freq_anchors.push_back(old.freq);
    }
    if (s.crossings.count != old.crossings.count) {
      _crossings.Add(i, meta.pos_start - s.pos_start,
                     _features.offs ? meta.pay_start - s.pay_start : 0);
    }
    if (s.extents.count != old.extents.count) {
      _extents.Add(i, meta.pos_extent, _features.offs ? meta.pay_extent : 0);
    }
  } else if (_features.vec) {
    _vec_starts.push_back(meta.pay_start);
    _lanes.push_back(meta.pos_offset);
  }
  _stats = s;
}

uint64_t RecordRun::SparseBytes(const SparseStats& stats,
                                uint32_t values) const noexcept {
  uint64_t size = VSize(stats.count);
  if (stats.count != 0) {
    size += 1 + values + PackedBytes(stats.count, stats.KeyBits());
    for (uint32_t v = 0; v != values; ++v) {
      size += PackedBytes(stats.count, Bits(stats.max[v]));
    }
  }
  return size;
}

uint64_t RecordRun::SizeOf(const Stats& s) const noexcept {
  if (s.count == 0) {
    return 0;
  }
  const uint32_t singles = s.count - s.multis;
  const uint32_t stored = s.multis - s.inlines;
  uint64_t size = VSize(s.count) + 1;
  if (singles != 0) {
    size += VSize(s.singles.min) + 1 + PackedBytes(singles, s.singles.Bits());
  }
  if (s.multis != 0) {
    size += 1 + PackedBytes(s.multis, Bits(s.docs));
  }
  if (singles != 0 && s.multis != 0) {
    size += PackedBytes(s.count, 1);
  }
  if (_features.freq) {
    size += 1 + PackedBytes(s.count, Bits(s.excess));
  }
  if (s.inlines != 0) {
    size += 2 + PackedBytes(s.inlines, Bits(s.sizes)) +
            PackedBytes(Anchors(s.inlines), Bits(s.inline_anchor)) +
            s.inline_bytes;
  }
  if (s.inlines != 0 && stored != 0) {
    size += PackedBytes(s.multis, 1);
  }
  if (stored != 0) {
    size += VSize(s.starts.min) + 1 + PackedBytes(stored, s.starts.Bits()) +
            SparseBytes(s.big, 1);
  }
  if (_features.pos) {
    size += VSize(s.pos_start) + VSize(s.pos_offset) +
            (_features.offs ? VSize(s.pay_start) : 0) + 1 +
            PackedBytes(Anchors(s.count), Bits(s.freq_anchor)) +
            SparseBytes(s.crossings, PositionValues(_features)) +
            SparseBytes(s.extents, PositionValues(_features));
  } else if (_features.vec) {
    size += VSize(s.vec_starts.min) + 2 +
            PackedBytes(s.count, s.vec_starts.Bits()) +
            PackedBytes(s.count, Bits(s.lanes));
  }
  return size;
}

void RecordRun::WriteSparse(DataOutput& out, const SparseStats& stats,
                            const SparseColumn& column, uint32_t values) const {
  SDB_ASSERT(column.keys.size() == stats.count);
  WritePacked(out, stats.count, stats.KeyBits(),
              [&](uint64_t j) { return column.keys[j]; });
  for (uint32_t v = 0; v != values; ++v) {
    WritePacked(out, stats.count, Bits(stats.max[v]),
                [&](uint64_t j) { return column.values[v][j]; });
  }
}

void RecordRun::Write(DataOutput& out) const {
  const auto& s = _stats;
  SDB_ASSERT(s.count != 0);
  const uint32_t singles = s.count - s.multis;
  const uint32_t stored = s.multis - s.inlines;
  const uint32_t extras = PositionValues(_features);
  const auto sparse_header = [&](const SparseStats& stats, uint32_t values) {
    out.WriteV32(stats.count);
    if (stats.count != 0) {
      out.WriteByte(static_cast<byte_type>(stats.KeyBits()));
      for (uint32_t v = 0; v != values; ++v) {
        out.WriteByte(static_cast<byte_type>(Bits(stats.max[v])));
      }
    }
  };

  out.WriteV32(s.count);
  out.WriteByte(static_cast<byte_type>(
    (singles != 0 ? kRunSingles : 0) | (s.multis != 0 ? kRunMultis : 0) |
    (s.inlines != 0 ? kRunInline : 0) | (stored != 0 ? kRunStored : 0)));
  if (singles != 0) {
    out.WriteV64(s.singles.min);
    out.WriteByte(static_cast<byte_type>(s.singles.Bits()));
  }
  if (s.multis != 0) {
    out.WriteByte(static_cast<byte_type>(Bits(s.docs)));
  }
  if (_features.freq) {
    out.WriteByte(static_cast<byte_type>(Bits(s.excess)));
  }
  if (s.inlines != 0) {
    out.WriteByte(static_cast<byte_type>(Bits(s.sizes)));
    out.WriteByte(static_cast<byte_type>(Bits(s.inline_anchor)));
  }
  if (stored != 0) {
    out.WriteV64(s.starts.min);
    out.WriteByte(static_cast<byte_type>(s.starts.Bits()));
    sparse_header(s.big, 1);
  }
  if (_features.pos) {
    out.WriteV64(s.pos_start);
    out.WriteV32(s.pos_offset);
    if (_features.offs) {
      out.WriteV64(s.pay_start);
    }
    out.WriteByte(static_cast<byte_type>(Bits(s.freq_anchor)));
    sparse_header(s.crossings, extras);
    sparse_header(s.extents, extras);
  } else if (_features.vec) {
    out.WriteV64(s.vec_starts.min);
    out.WriteByte(static_cast<byte_type>(s.vec_starts.Bits()));
    out.WriteByte(static_cast<byte_type>(Bits(s.lanes)));
  }

  if (singles != 0 && s.multis != 0) {
    WritePacked(out, s.count, 1, [&](uint64_t j) { return _multi_bits[j]; });
  }
  if (s.inlines != 0 && stored != 0) {
    WritePacked(out, s.multis, 1, [&](uint64_t j) { return _inline_bits[j]; });
  }
  WritePacked(out, singles, s.singles.Bits(),
              [&](uint64_t j) { return _singles[j] - s.singles.min; });
  WritePacked(out, s.multis, Bits(s.docs),
              [&](uint64_t j) { return _docs[j]; });
  if (_features.freq) {
    WritePacked(out, s.count, Bits(s.excess),
                [&](uint64_t j) { return _excess[j]; });
  }
  WritePacked(out, s.inlines, Bits(s.sizes),
              [&](uint64_t j) { return _sizes[j]; });
  WritePacked(out, _inline_anchors.size(), Bits(s.inline_anchor),
              [&](uint64_t j) { return _inline_anchors[j]; });
  WritePacked(out, stored, s.starts.Bits(),
              [&](uint64_t j) { return _starts[j] - s.starts.min; });
  WriteSparse(out, s.big, _big, 1);
  if (_features.pos) {
    WritePacked(out, _freq_anchors.size(), Bits(s.freq_anchor),
                [&](uint64_t j) { return _freq_anchors[j]; });
    WriteSparse(out, s.crossings, _crossings, extras);
    WriteSparse(out, s.extents, _extents, extras);
  } else if (_features.vec) {
    WritePacked(out, s.count, s.vec_starts.Bits(),
                [&](uint64_t j) { return _vec_starts[j] - s.vec_starts.min; });
    WritePacked(out, s.count, Bits(s.lanes),
                [&](uint64_t j) { return _lanes[j]; });
  }
  SDB_ASSERT(_inline.size() == s.inline_bytes);
  out.WriteData(_inline.data(), _inline.size());
}

}  // namespace irs::term_dict
