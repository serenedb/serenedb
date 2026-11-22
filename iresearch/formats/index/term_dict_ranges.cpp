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

#include <algorithm>
#include <array>
#include <cstring>
#include <optional>
#include <vector>

#include "iresearch/formats/index/field_reader.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"

namespace irs::term_dict {
namespace {

constexpr uint32_t kLeafFanout = 64;
constexpr uint32_t kFanout = 16;
constexpr uint64_t kHeavyDocs = uint64_t{1} << 14;
constexpr uint64_t kTopRangePostings = 16;

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

template<size_t Width>
IRS_FORCE_INLINE bool Less(bytes_view lhs, bytes_view rhs) noexcept {
  if constexpr (Width == 0) {
    return lhs < rhs;
  } else {
    SDB_ASSERT(lhs.size() == Width && rhs.size() == Width);
    return std::memcmp(lhs.data(), rhs.data(), Width) < 0;
  }
}

class RangeAhead final : public ColdBlocks {
 public:
  explicit RangeAhead(const TermReaderImpl& field) noexcept : _field{&field} {}

  void Arm(Cursor& cursor, bytes_view to) noexcept {
    _to = to;
    _armed = true;
    cursor.WatchCold(*this);
  }

  void Reached(uint32_t block) final {
    if (!std::exchange(_armed, false)) {
      return;
    }
    const auto& field = *_field;
    const uint32_t end = std::max(field.Index().BlockEnd(_to), block + 1);
    const uint64_t offset = field.BlockOffset(block);
    const uint64_t size =
      std::min(field.BlockOffset(end) - offset, duckdb::InputFile::MAX_HINT);
    if (size > duckdb::PageResidency::PAGE_SIZE) {
      field.Store().Input().Hint(offset, size);
    }
  }

 private:
  const TermReaderImpl* _field;
  bytes_view _to;
  bool _armed = false;
};

template<size_t Width>
bool Ascending(Cursor& cursor, RangeAhead& ahead, bytes_view from,
               bytes_view to, TermReader::TermVisitor visit) {
  ahead.Arm(cursor, to);
  if (from.empty()) {
    cursor.First();
  } else if (cursor.SeekGe(from) == SeekResult::End) {
    return true;
  }
  do {
    const bytes_view term = cursor.Term();
    if (!to.empty() && !Less<Width>(term, to)) {
      return true;
    }
    if (!visit(term, cursor.Meta())) {
      return false;
    }
  } while (cursor.Advance());
  return true;
}

using RangeVisitor =
  absl::FunctionRef<bool(bytes_view, bytes_view, const PostingMeta&)>;

template<size_t Width>
class StartBuffer {
 public:
  IRS_FORCE_INLINE bytes_view Assign(bytes_view key) noexcept {
    SDB_ASSERT(key.size() <= kMaxStride);
    if constexpr (Width == 0) {
      std::memcpy(_bytes.data(), key.data(), key.size());
      return {_bytes.data(), key.size()};
    } else {
      SDB_ASSERT(key.size() == Width);
      std::memcpy(_bytes.data(), key.data(), Width);
      return {_bytes.data(), Width};
    }
  }

 private:
  std::array<byte_type, kMaxStride> _bytes;
};

template<size_t Width>
class LevelWalk {
 public:
  LevelWalk(std::span<const TermReaderImpl* const> levels, bytes_view from,
            bytes_view to, RangeVisitor visit) noexcept
    : _levels{levels}, _visit{visit}, _from{from}, _to{to} {
    SDB_ASSERT(!_levels.empty());
  }

  bool Run() {
    size_t top = 0;
    while (top != _levels.size() && Seek(top, _from, _to) &&
           Fits(_entries[top])) {
      ++top;
    }
    if (top == 0) {
      return true;
    }
    for (size_t level = 0; level + 1 != top; ++level) {
      if (!Emit(level, _entries[level + 1].start)) {
        return !_stopped;
      }
    }
    Emit(top - 1, {});
    return !_stopped;
  }

 private:
  struct Entry {
    StartBuffer<Width> starts[2];
    bytes_view start;
    bytes_view next;
    PostingMeta meta;
    bool at = false;
    bool last = false;
  };

  bool Fits(const Entry& entry) const noexcept {
    return _to.empty() || (!entry.last && !Less<Width>(_to, entry.next));
  }

  void Read(Cursor& cursor, Entry& entry) {
    entry.meta = cursor.Meta();
    entry.last = !cursor.Advance();
    entry.next =
      entry.last ? bytes_view{} : entry.starts[!entry.at].Assign(cursor.Term());
  }

  bool Seek(size_t level, bytes_view key, bytes_view end) {
    auto& slot = _cursors[level];
    if (!slot) {
      slot.emplace(*_levels[level]);
      _ahead[level].emplace(*_levels[level]);
    }
    auto& cursor = *slot;
    _ahead[level]->Arm(cursor, end);
    if (key.empty()) {
      cursor.First();
    } else if (cursor.SeekGe(key) == SeekResult::End) {
      return false;
    }
    auto& entry = _entries[level];
    entry.at = false;
    entry.start = entry.starts[0].Assign(cursor.Term());
    if (!_to.empty() && !Less<Width>(entry.start, _to)) {
      return false;
    }
    Read(cursor, entry);
    return true;
  }

  bool Emit(size_t level, bytes_view end) {
    auto& cursor = *_cursors[level];
    auto& entry = _entries[level];
    for (;;) {
      if (!end.empty() && !Less<Width>(entry.start, end)) {
        return true;
      }
      if (!Fits(entry)) {
        if (level != 0 && Seek(level - 1, entry.start, entry.next)) {
          Emit(level - 1, entry.next);
        }
        return false;
      }
      if (!_visit(entry.start, entry.next, entry.meta)) {
        _stopped = true;
        return false;
      }
      if (entry.last) {
        return true;
      }
      entry.at = !entry.at;
      entry.start = entry.next;
      if (!_to.empty() && !Less<Width>(entry.start, _to)) {
        return false;
      }
      Read(cursor, entry);
    }
  }

  std::span<const TermReaderImpl* const> _levels;
  std::array<std::optional<Cursor>, kMaxRangeLevels> _cursors;
  std::array<std::optional<RangeAhead>, kMaxRangeLevels> _ahead;
  std::array<Entry, kMaxRangeLevels> _entries;
  RangeVisitor _visit;
  bytes_view _from;
  bytes_view _to;
  bool _stopped = false;
};

template<size_t Width>
bool Cover(const TermReaderImpl& field, bytes_view from, bytes_view to,
           TermReader::TermVisitor visit) {
  Cursor cursor{field};
  RangeAhead ahead{field};
  byte_type lower[kMaxStride];
  byte_type upper[kMaxStride];
  size_t lower_size = 0;
  size_t upper_size = 0;
  bool covered = false;
  const auto collect = [&](bytes_view start, bytes_view end,
                           const PostingMeta& meta) {
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
  };
  const auto levels = field.RangeLevels();
  if (!levels.empty() && !LevelWalk<Width>{levels, from, to, collect}.Run()) {
    return true;
  }
  if (!covered) {
    Ascending<Width>(cursor, ahead, from, to, visit);
    return false;
  }
  bytes_view head_from = from;
  bytes_view head_to{lower, lower_size};
  if (Narrow(field, head_from, head_to) &&
      !Ascending<Width>(cursor, ahead, head_from, head_to, visit)) {
    return true;
  }
  bytes_view tail_from{upper, upper_size};
  bytes_view tail_to = to;
  if (upper_size != 0 && Narrow(field, tail_from, tail_to)) {
    Ascending<Width>(cursor, ahead, tail_from, tail_to, visit);
  }
  return true;
}

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
          });
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
    held.join = false;
    range.holding = true;
    return;
  }
  auto& group = range.group;
  if (range.children == 0) {
    group.key.assign(child.key);
    group.meta = child.meta;
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
  }
  range.starts.append(posting.key);
  range.metas.emplace_back(posting.meta);
  ++range.count;
  if (level + 1 != kMaxRangeLevels) {
    Push(level + 1, {
                      .key = posting.key,
                      .meta = posting.meta,
                      .docs = posting.docs,
                      .docs_count = posting.docs_count,
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

RangeWriter::Entry RangeWriter::At(size_t level, uint32_t i) const noexcept {
  const auto& range = _levels[level];
  SDB_ASSERT(i < range.count);
  const size_t width = range.starts.size() / range.count;
  return {.key = bytes_view{range.starts}.substr(i * width, width),
          .meta = range.metas[i]};
}

void RangeWriter::Clear() noexcept {
  for (auto& range : _levels) {
    range.starts.clear();
    range.metas.clear();
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
  const size_t width = max().size();
  if (!_range_levels.empty() && (from.empty() || from.size() == width) &&
      (to.empty() || to.size() == width)) {
    switch (width) {
      case sizeof(uint32_t):
        return Cover<sizeof(uint32_t)>(*this, from, to, visit);
      case sizeof(uint64_t):
        return Cover<sizeof(uint64_t)>(*this, from, to, visit);
      case 2 * sizeof(uint64_t):
        return Cover<2 * sizeof(uint64_t)>(*this, from, to, visit);
      default:
        break;
    }
  }
  return Cover<0>(*this, from, to, visit);
}

}  // namespace irs::term_dict
