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
#include <absl/base/internal/endian.h>

#include <algorithm>
#include <cstring>

#include "iresearch/formats/index/term_dict.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/lower_bound.hpp"

namespace irs::term_dict {
namespace {

constexpr uint32_t kLeafFanout = 64;
constexpr uint32_t kFanout = 16;
constexpr uint64_t kHeavyDocs = uint64_t{1} << 14;
constexpr byte_type kZeros[kSlack]{};

uint32_t Load32(const byte_type* p, uint32_t i) noexcept {
  return absl::little_endian::Load32(p + uint64_t{i} * sizeof(uint32_t));
}

class RangeWalk {
 public:
  RangeWalk(const TermReaderImpl& field, bytes_view from, bytes_view to,
            TermReader::RangeVisitor visit) noexcept
    : _field{field},
      _levels{field.RangeLevels()},
      _cursor{field},
      _visit{visit},
      _from{from},
      _to{to},
      _prefix{field.RangePrefix()},
      _stride{field.KeyWidth() - field.RangePrefix()} {
    if (!_levels.empty()) {
      std::memcpy(_lower, field.min().data(), _prefix);
      std::memcpy(_upper, field.min().data(), _prefix);
      _from_rest = _from.empty() ? _from : _from.substr(_prefix);
      _to_rest = _to.empty() ? _to : _to.substr(_prefix);
    }
  }

  void Run() {
    if (_levels.empty()) {
      Terms(_from, _to);
      return;
    }
    const size_t top = _levels.size() - 1;
    Pieces(top, Find(top), _levels[top].count);
  }

 private:
  bytes_view Start(size_t level, uint32_t i) const noexcept {
    return {_levels[level].starts + uint64_t{i} * _stride, _stride};
  }

  bytes_view Key(byte_type* buffer, bytes_view rest) const noexcept {
    std::memcpy(buffer + _prefix, rest.data(), _stride);
    return {buffer, _prefix + _stride};
  }

  uint32_t Find(size_t level) const noexcept {
    if (_from.empty()) {
      return 0;
    }
    const auto lo = static_cast<uint32_t>(
      PartitionIndex(0, _levels[level].count, [&](size_t i) {
        return Start(level, static_cast<uint32_t>(i)) <= _from_rest;
      }));
    return lo == 0 ? 0 : lo - 1;
  }

  bool Pieces(size_t level, uint32_t begin, uint32_t end) {
    const auto& range = _levels[level];
    for (uint32_t i = begin; i != end; ++i) {
      const auto start = Start(level, i);
      if (!_to.empty() && start >= _to_rest) {
        return false;
      }
      const bool last = i + 1 == range.count;
      const auto next = last ? bytes_view{} : Start(level, i + 1);
      if (!_from.empty() && !last && next <= _from_rest) {
        continue;
      }
      const bool from_side = _from.empty() || _from_rest <= start;
      const bool to_side = _to.empty() || (!last && next <= _to_rest);
      if (from_side && to_side) {
        PostingMeta meta;
        PostingsReader::decode(range.records + Load32(range.offsets, i),
                               _field.meta().index_features, meta);
        _visit(Key(_lower, start), meta);
      } else if (level == 0) {
        const auto lower = from_side ? Key(_lower, start) : _from;
        const auto upper = last || !to_side ? _to : Key(_upper, next);
        if (!Terms(lower, upper)) {
          return false;
        }
      } else if (!Pieces(level - 1, Load32(range.firsts, i),
                         last ? _levels[level - 1].count
                              : Load32(range.firsts, i + 1))) {
        return false;
      }
    }
    return true;
  }

  bool Terms(bytes_view lower, bytes_view upper) {
    if (_cursor.SeekGe(lower) == SeekResult::End) {
      return false;
    }
    do {
      const bytes_view key = _cursor.Term();
      if (!upper.empty() && key >= upper) {
        return true;
      }
      _visit(key, _cursor.Meta());
    } while (_cursor.Advance());
    return false;
  }

  const TermReaderImpl& _field;
  std::span<const RangeLevel> _levels;
  Cursor _cursor;
  TermReader::RangeVisitor _visit;
  bytes_view _from;
  bytes_view _to;
  bytes_view _from_rest;
  bytes_view _to_rest;
  uint32_t _prefix;
  uint32_t _stride;
  byte_type _lower[kMaxStride];
  byte_type _upper[kMaxStride];
};

}  // namespace

void RangeWriter::Begin(PostingsWriter& pw) {
  _pw = &pw;
  TeeLeaf();
}

void RangeWriter::TeeLeaf() noexcept {
  _pw->Tee(&_levels[0].group.docs, 2 * kHeavyDocs);
}

void RangeWriter::Add(bytes_view term, const PostingMeta& meta) {
  auto& group = _levels[0].group;
  if (meta.docs_count >= kHeavyDocs) {
    group.docs.resize(group.docs_count);
  }
  SDB_ASSERT(group.docs.size() ==
             group.docs_count +
               (meta.docs_count >= kHeavyDocs ? 0 : meta.docs_count));
  Push(0, {
            .key = term,
            .meta = meta,
            .docs = {},
            .docs_count = meta.docs_count,
            .index = 0,
          });
}

void RangeWriter::Push(size_t level, const Child& child) {
  auto& range = _levels[level];
  if (child.docs_count >= kHeavyDocs) {
    Close(level);
    Release(level);
    auto& held = range.held;
    held.key.assign(child.key);
    held.docs.clear();
    held.meta = child.meta;
    held.docs_count = child.docs_count;
    held.first = child.index;
    held.join = false;
    range.holding = true;
    return;
  }
  auto& group = range.group;
  if (range.children == 0) {
    group.key.assign(child.key);
    group.meta = child.meta;
    group.first = child.index;
  }
  if (level != 0) {
    group.docs.insert(group.docs.end(), child.docs.begin(), child.docs.end());
  }
  group.docs_count += child.docs_count;
  const uint32_t fanout = level == 0 ? kLeafFanout : kFanout;
  if (++range.children == fanout || group.docs_count >= kHeavyDocs) {
    Close(level);
  }
}

void RangeWriter::Close(size_t level) {
  auto& range = _levels[level];
  if (range.children == 0) {
    return;
  }
  Release(level);
  std::swap(range.group, range.held);
  range.held.join = range.children > 1;
  range.holding = true;
  range.children = 0;
  range.group.docs.clear();
  range.group.docs_count = 0;
}

void RangeWriter::Release(size_t level) {
  auto& range = _levels[level];
  if (!range.holding) {
    return;
  }
  range.holding = false;
  auto& piece = range.held;
  if (piece.join) {
    absl::c_sort(piece.docs);
    piece.docs.erase(std::ranges::unique(piece.docs).begin(), piece.docs.end());
    piece.meta = {};
    _pw->Tee(nullptr, 0);
    _pw->WritePostings(
      PostingRows{
        .span = {.docs = piece.docs.data(), .count = piece.docs.size()},
      },
      piece.meta);
    TeeLeaf();
    piece.docs_count = piece.meta.docs_count;
  }
  _record.Clear();
  _pw->EncodeAbsolute(_record, piece.meta);
  range.records.append(_record.View());
  range.ends.push_back(static_cast<uint32_t>(range.records.size()));
  range.starts.append(piece.key);
  if (level != 0) {
    range.firsts.push_back(piece.first);
  }
  const uint32_t index = range.count++;
  if (level + 1 != kMaxRangeLevels) {
    Push(level + 1, {
                      .key = piece.key,
                      .meta = piece.meta,
                      .docs = piece.docs,
                      .docs_count = piece.docs_count,
                      .index = index,
                    });
  }
}

void RangeWriter::Finish(uint64_t terms) {
  size_t built = 0;
  for (; built != kMaxRangeLevels; ++built) {
    Close(built);
    if (_levels[built].count == 0) {
      break;
    }
    Release(built);
  }
  uint64_t below = terms;
  _height = 0;
  while (_height != built && _levels[_height].count < below) {
    below = _levels[_height].count;
    ++_height;
  }
}

void RangeWriter::Write(bstring& region, size_t prefix,
                        std::vector<uint64_t>& counts) const {
  region.clear();
  counts.clear();
  if (_height == 0) {
    return;
  }
  BytesOutput out{region};
  for (size_t level = 0; level != _height; ++level) {
    const auto& range = _levels[level];
    const size_t width = range.starts.size() / range.count;
    counts.push_back(range.count);
    for (uint32_t i = 0; i != range.count; ++i) {
      out.WriteData(range.starts.data() + i * width + prefix, width - prefix);
    }
    for (const auto first : range.firsts) {
      out.WriteU32(first);
    }
    out.WriteU32(0);
    for (const auto end : range.ends) {
      out.WriteU32(end);
    }
    out.WriteData(range.records.data(), range.records.size());
  }
  out.WriteData(kZeros, kSlack);
}

void RangeWriter::Clear() noexcept {
  for (auto& range : _levels) {
    range.starts.clear();
    range.firsts.clear();
    range.ends.clear();
    range.records.clear();
    range.group.docs.clear();
    range.group.docs_count = 0;
    range.held.docs.clear();
    range.count = 0;
    range.children = 0;
    range.holding = false;
  }
  if (_pw) {
    _pw->Tee(nullptr, 0);
  }
  _pw = nullptr;
  _height = 0;
}

void TermReaderImpl::VisitRange(bytes_view from, bytes_view to,
                                RangeVisitor visit) const {
  if (_terms == 0 || (!from.empty() && from > max()) ||
      (!to.empty() && to <= min())) {
    return;
  }
  if (from <= min()) {
    from = {};
  }
  if (!to.empty() && to > max()) {
    to = {};
  }
  if (Single()) {
    visit(min(), _single);
    return;
  }
  RangeWalk{*this, from, to, visit}.Run();
}

}  // namespace irs::term_dict
