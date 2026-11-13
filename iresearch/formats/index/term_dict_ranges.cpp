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

#include "iresearch/formats/index/term_dict_ranges.hpp"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <vector>

#include "iresearch/formats/index/field_reader.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/containers/small_vector.hpp"
#include "iresearch/utils/lower_bound.hpp"

namespace irs::term_dict {
namespace {

constexpr uint32_t kLeafFanout = 64;
constexpr uint32_t kFanout = 16;
constexpr uint64_t kHeavyDocs = uint64_t{1} << 14;
constexpr uint64_t kTopRangePostings = 16;
constexpr byte_type kZeros[kSlack]{};

uint32_t Load32(const byte_type* p, uint32_t i) noexcept {
  return absl::little_endian::Load32(p + uint64_t{i} * sizeof(uint32_t));
}

bool Point(bytes_view from, bytes_view to) noexcept {
  return to.size() == from.size() + 1 && to.back() == 0 && to.starts_with(from);
}

bool Narrow(const TermReaderImpl& field, bytes_view& from,
            bytes_view& to) noexcept {
  if ((!from.empty() && from > field.max()) ||
      (!to.empty() && (to <= field.min() || to <= from))) {
    return false;
  }
  if (from <= field.min()) {
    from = {};
  }
  if (!to.empty() && to > field.max()) {
    to = {};
  }
  return true;
}

bool Ascending(Cursor& cursor, bytes_view from, bytes_view to,
               TermReader::TermVisitor visit) {
  if (from.empty()) {
    cursor.First();
  } else if (cursor.SeekGe(from, true) == SeekResult::End) {
    return true;
  }
  do {
    const bytes_view term = cursor.Term();
    if (!to.empty() && term >= to) {
      return true;
    }
    if (!visit(term, cursor.Meta())) {
      return false;
    }
  } while (cursor.Advance());
  return true;
}

struct BufferedTerm {
  uint32_t key;
  uint32_t size;
  PostingMeta meta;
};

bool Descending(const TermReaderImpl& field, bytes_view from, bytes_view to,
                TermReader::TermVisitor visit) {
  Cursor cursor{field};
  PaddedKey probe;
  const uint32_t last = to.empty()
                          ? field.Blocks() - 1
                          : field.BlockOf(probe.Assign(field.KeyPrefix(), to));
  containers::SmallVector<byte_type, kRestartInterval * kMaxStride> keys;
  std::array<BufferedTerm, kRestartInterval> terms;
  for (uint32_t block = last + 1; block-- != 0;) {
    cursor.Load(block);
    const uint32_t first = block == 0 ? field.FirstEntry() : 0;
    const uint32_t stop = cursor.Stop();
    for (uint32_t group = (stop - 1) / kRestartInterval + 1;
         group-- != first / kRestartInterval;) {
      const uint32_t end = std::min((group + 1) * kRestartInterval, stop);
      cursor.Group(group);
      while (cursor.Entry() < first) {
        cursor.Next();
      }
      keys.clear();
      uint32_t count = 0;
      bool below = false;
      for (;;) {
        const bytes_view term = cursor.Term();
        if (!to.empty() && term >= to) {
          break;
        }
        if (!from.empty() && term < from) {
          below = true;
        } else {
          terms[count++] = {.key = static_cast<uint32_t>(keys.size()),
                            .size = static_cast<uint32_t>(term.size()),
                            .meta = cursor.Meta()};
          keys.insert(keys.end(), term.begin(), term.end());
        }
        if (cursor.Entry() + 1 == end) {
          break;
        }
        cursor.Next();
      }
      while (count != 0) {
        const auto& buffered = terms[--count];
        if (!visit({keys.data() + buffered.key, buffered.size},
                   buffered.meta)) {
          return false;
        }
      }
      if (below) {
        return true;
      }
    }
  }
  return true;
}

using RangePostingVisitor =
  absl::FunctionRef<bool(bytes_view, bytes_view, const PostingMeta&)>;

class RangePostingWalk {
 public:
  RangePostingWalk(const TermReaderImpl& field, bytes_view from, bytes_view to,
                   RangePostingVisitor visit) noexcept
    : _field{field},
      _levels{field.RangeLevels()},
      _visit{visit},
      _from{from},
      _to{to},
      _prefix{field.RangePrefix()},
      _stride{field.key_width - field.RangePrefix()} {
    SDB_ASSERT(!_levels.empty());
    std::memcpy(_lower, field.min().data(), _prefix);
    std::memcpy(_upper, field.min().data(), _prefix);
    _from_rest = _from.empty() ? _from : _from.substr(_prefix);
    _to_rest = _to.empty() ? _to : _to.substr(_prefix);
  }

  bool Run() {
    const size_t top = _levels.size() - 1;
    Walk(top, Find(top), _levels[top].count);
    return !_stopped;
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

  bool Walk(size_t level, uint32_t begin, uint32_t end) {
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
        const auto* record = range.records + Load32(range.offsets, i);
        PostingMeta meta;
        meta.inline_data =
          record +
          PostingsReader::decode(record, _field.meta().index_features, meta);
        if (!_visit(Key(_lower, start), last ? bytes_view{} : Key(_upper, next),
                    meta)) {
          _stopped = true;
          return false;
        }
      } else if (level != 0 && !Walk(level - 1, Load32(range.firsts, i),
                                     last ? _levels[level - 1].count
                                          : Load32(range.firsts, i + 1))) {
        return false;
      }
    }
    return true;
  }

  const TermReaderImpl& _field;
  std::span<const RangeLevel> _levels;
  RangePostingVisitor _visit;
  bytes_view _from;
  bytes_view _to;
  bytes_view _from_rest;
  bytes_view _to_rest;
  uint32_t _prefix;
  uint32_t _stride;
  bool _stopped = false;
  byte_type _lower[kMaxStride];
  byte_type _upper[kMaxStride];
};

}  // namespace

void RangeWriter::Begin(PostingsWriter& pw, uint64_t docs) noexcept {
  _pw = &pw;
  _heavy = std::max(kHeavyDocs, docs / kTopRangePostings);
}

DocTee RangeWriter::Tee() noexcept {
  if (!Active()) {
    return {};
  }
  return {.docs = &_levels[0].group.docs, .limit = 2 * _heavy};
}

void RangeWriter::Add(bytes_view term, const PostingMeta& meta) {
  auto& group = _levels[0].group;
  if (meta.docs_count >= _heavy) {
    group.docs.resize(group.docs_count);
  }
  SDB_ASSERT(group.docs.size() ==
             group.docs_count +
               (meta.docs_count >= _heavy ? 0 : meta.docs_count));
  Push(0, {
            .key = term,
            .meta = meta,
            .docs = {},
            .docs_count = meta.docs_count,
            .index = 0,
          });
}

void RangeWriter::Keep(RangePosting& posting,
                       const PostingMeta& meta) noexcept {
  posting.meta = meta;
  if (meta.inline_size != 0) {
    std::memcpy(posting.inline_bytes.data(), meta.inline_data,
                meta.inline_size);
    posting.meta.inline_data = posting.inline_bytes.data();
  }
}

void RangeWriter::Push(size_t level, const Child& child) {
  auto& range = _levels[level];
  if (child.docs_count >= _heavy) {
    SDB_ASSERT(child.meta.inline_size == 0);
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
    Keep(group, child.meta);
    group.first = child.index;
  }
  if (level != 0) {
    group.docs.insert(group.docs.end(), child.docs.begin(), child.docs.end());
  }
  group.docs_count += child.docs_count;
  const uint32_t fanout = level == 0 ? kLeafFanout : kFanout;
  if (++range.children == fanout || group.docs_count >= _heavy) {
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
  auto& posting = range.held;
  if (posting.join) {
    absl::c_sort(posting.docs);
    posting.docs.erase(std::ranges::unique(posting.docs).begin(),
                       posting.docs.end());
    posting.meta = {};
    _pw->WritePostings(
      PostingRows{
        .span = {.docs = posting.docs.data(), .count = posting.docs.size()},
      },
      posting.meta);
    posting.docs_count = posting.meta.docs_count;
  } else if (posting.meta.inline_size != 0) {
    posting.meta.inline_data = posting.inline_bytes.data();
  }
  _record.Clear();
  _pw->EncodeAbsolute(_record, posting.meta);
  range.records.append(_record.View());
  range.records.append(posting.meta.Inline());
  range.ends.push_back(static_cast<uint32_t>(range.records.size()));
  range.starts.append(posting.key);
  if (level != 0) {
    range.firsts.push_back(posting.first);
  }
  const uint32_t index = range.count++;
  if (level + 1 != kMaxRangeLevels) {
    Push(level + 1, {
                      .key = posting.key,
                      .meta = posting.meta,
                      .docs = posting.docs,
                      .docs_count = posting.docs_count,
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
  _pw = nullptr;
  _height = 0;
}

bool TermReaderImpl::VisitRange(bytes_view from, bytes_view to, bool descending,
                                TermVisitor visit) const {
  if (Point(from, to)) {
    const auto meta = Lookup(from);
    return meta.docs_count == 0 || visit(from, meta);
  }
  if (!Narrow(*this, from, to)) {
    return true;
  }
  if (descending) {
    return Descending(*this, from, to, visit);
  }
  Cursor cursor{*this};
  return Ascending(cursor, from, to, visit);
}

bool TermReaderImpl::CoverRange(bytes_view from, bytes_view to,
                                TermVisitor visit) const {
  if (Point(from, to)) {
    if (const auto meta = Lookup(from); meta.docs_count != 0) {
      visit(from, meta);
    }
    return false;
  }
  if (!Narrow(*this, from, to)) {
    return false;
  }
  Cursor cursor{*this};
  byte_type lower[kMaxStride];
  byte_type upper[kMaxStride];
  size_t lower_size = 0;
  size_t upper_size = 0;
  bool covered = false;
  if (!_levels.empty() &&
      !RangePostingWalk{
        *this, from, to,
        [&](bytes_view start, bytes_view end, const PostingMeta& meta) {
          if (!covered) {
            covered = true;
            lower_size = start.size();
            std::memcpy(lower, start.data(), lower_size);
          }
          upper_size = end.size();
          if (upper_size != 0) {
            std::memcpy(upper, end.data(), upper_size);
          }
          return visit(start, meta);
        }}
         .Run()) {
    return true;
  }
  if (!covered) {
    Ascending(cursor, from, to, visit);
    return false;
  }
  bytes_view head_from = from;
  bytes_view head_to{lower, lower_size};
  if (Narrow(*this, head_from, head_to) &&
      !Ascending(cursor, head_from, head_to, visit)) {
    return true;
  }
  bytes_view tail_from{upper, upper_size};
  bytes_view tail_to = to;
  if (upper_size != 0 && Narrow(*this, tail_from, tail_to)) {
    Ascending(cursor, tail_from, tail_to, visit);
  }
  return true;
}

}  // namespace irs::term_dict
