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
#include <bit>
#include <cstring>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/index/field_reader.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/containers/small_vector.hpp"
#include "iresearch/utils/lower_bound.hpp"

namespace irs::term_dict {
namespace {

constexpr uint32_t kLeafFanout = 64;
constexpr uint32_t kFanout = 16;
constexpr uint64_t kHeavyDocs = uint64_t{1} << 14;
constexpr uint64_t kTopPieces = 16;
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
      _stride{field.key_width - field.RangePrefix()} {
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
        const auto* record = range.records + Load32(range.offsets, i);
        PostingMeta meta;
        meta.inline_data =
          record +
          PostingsReader::decode(record, _field.meta().index_features, meta);
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
    if (_cursor.SeekGe(lower, true) == SeekResult::End) {
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

class PrunedWalk {
 public:
  explicit PrunedWalk(const TermReaderImpl& field, const RangePruner& pruner,
                      TermReader::RangeVisitor visit) noexcept
    : _field{field},
      _levels{field.RangeLevels()},
      _cursor{field},
      _pruner{pruner},
      _visit{visit},
      _prefix{field.RangePrefix()},
      _stride{field.key_width - field.RangePrefix()} {
    if (!_levels.empty()) {
      std::memcpy(_first, field.min().data(), _prefix);
      std::memcpy(_next, field.min().data(), _prefix);
    }
  }

  void Run() {
    if (_levels.empty()) {
      Terms(0, _field.min(), {});
      return;
    }
    const size_t top = _levels.size() - 1;
    Pieces(top, 0, _levels[top].count,
           [&](uint32_t piece) { Descend(top, piece); });
    PartialPieces();
  }

  void Split(uint32_t parts, PrunedSplit& split) {
    split.pieces.clear();
    if (_levels.empty()) {
      Terms(0, _field.min(), {});
      return;
    }
    size_t level = _levels.size() - 1;
    Pieces(level, 0, _levels[level].count,
           [&](uint32_t piece) { split.pieces.push_back(piece); });
    std::vector<uint32_t> next;
    while (level != 0 && !split.pieces.empty() && split.pieces.size() < parts) {
      next.clear();
      for (const auto piece : split.pieces) {
        const auto [begin, end] = Children(level, piece);
        Pieces(level - 1, begin, end,
               [&](uint32_t child) { next.push_back(child); });
      }
      split.pieces.swap(next);
      --level;
    }
    split.level = static_cast<uint32_t>(level);
  }

  void Visit(uint32_t level, std::span<const uint32_t> pieces) {
    if (pieces.empty()) {
      return;
    }
    for (const auto piece : pieces) {
      Descend(level, piece);
    }
    PartialPieces();
  }

 private:
  struct Partial {
    uint32_t piece = 0;
    uint32_t block = 0;
  };

  void PartialPieces() {
    const auto prefix = _field.KeyPrefix();
    const auto count = _levels[0].count;
    uint32_t block = 0;
    for (size_t done = 0; done != _partial.size();) {
      containers::SmallVector<FileRange, kPlannedRanges> ranges;
      uint64_t planned = 0;
      auto end = done;
      for (; end != _partial.size() && planned < kMaxPrefetch; ++end) {
        auto& partial = _partial[end];
        block = _field.BlockFrom(
          block, _probe.Assign(prefix, Start(0, partial.piece, _first)));
        partial.block = block;
        const auto last =
          partial.piece + 1 == count
            ? _field.Blocks() - 1
            : _field.BlockFrom(
                block,
                _probe.Assign(prefix, Start(0, partial.piece + 1, _next)));
        const auto from = _field.BlockOffset(block);
        const auto to = _field.BlockOffset(last + 1);
        AppendRange(ranges, from, to);
        planned += to - from;
      }
      if (!_field.Small()) {
        HintRanges(_field.Owner().Input(), std::span<const FileRange>{ranges});
      }
      for (; done != end; ++done) {
        const auto [piece, first_block] = _partial[done];
        Terms(first_block, Start(0, piece, _first),
              piece + 1 == count ? bytes_view{} : Start(0, piece + 1, _next));
      }
    }
  }

  bytes_view Start(size_t level, uint32_t i, byte_type* buffer) const noexcept {
    std::memcpy(buffer + _prefix, _levels[level].starts + uint64_t{i} * _stride,
                _stride);
    return {buffer, _prefix + _stride};
  }

  bytes_view Before(bytes_view next) noexcept {
    std::memcpy(_last, next.data(), next.size());
    for (size_t i = next.size(); i-- != 0;) {
      if (_last[i]-- != 0) {
        break;
      }
    }
    return {_last, next.size()};
  }

  std::pair<uint32_t, uint32_t> Children(size_t level,
                                         uint32_t piece) const noexcept {
    const auto& range = _levels[level];
    return {Load32(range.firsts, piece), piece + 1 == range.count
                                           ? _levels[level - 1].count
                                           : Load32(range.firsts, piece + 1)};
  }

  void Descend(size_t level, uint32_t piece) {
    if (level == 0) {
      _partial.emplace_back(piece);
      return;
    }
    const auto [begin, end] = Children(level, piece);
    Pieces(level - 1, begin, end,
           [&](uint32_t child) { Descend(level - 1, child); });
  }

  template<typename OnPartial>
  void Pieces(size_t level, uint32_t begin, uint32_t end, OnPartial&& partial) {
    const auto& range = _levels[level];
    for (uint32_t i = begin; i != end; ++i) {
      const bool tail = i + 1 == range.count;
      const auto first = Start(level, i, _first);
      const auto next = tail ? bytes_view{} : Start(level, i + 1, _next);
      switch (_pruner.Fit(first, tail ? _field.max() : Before(next))) {
        case RangeFit::Outside:
          continue;
        case RangeFit::Inside: {
          const auto* record = range.records + Load32(range.offsets, i);
          PostingMeta meta;
          meta.inline_data =
            record +
            PostingsReader::decode(record, _field.meta().index_features, meta);
          _visit(first, meta);
          continue;
        }
        case RangeFit::Partial:
          break;
      }
      partial(i);
    }
  }

  void Terms(uint32_t block, bytes_view lower, bytes_view upper) {
    _cursor.SeekIn(block, lower);
    do {
      if (_cursor.View().fixed) {
        if (!FixedTerms(upper)) {
          return;
        }
        continue;
      }
      const bytes_view key = _cursor.Term();
      if (!upper.empty() && key >= upper) {
        return;
      }
      if (_pruner.Select(key, nullptr, 0, 0, 1) != 0) {
        _visit(key, _cursor.Meta());
      }
    } while (_cursor.Advance());
  }

  bool FixedTerms(bytes_view upper) {
    const auto& view = _cursor.View();
    const auto prefix = Prefix(view);
    std::memcpy(_key, prefix.data(), prefix.size());
    const size_t ordinal = _field.KeyPrefix().size();
    const size_t kept = std::min(prefix.size(), ordinal);
    const auto head = prefix.substr(kept);
    const uint32_t dropped = static_cast<uint32_t>(ordinal - kept);
    const uint32_t width = view.stride - dropped;
    const bytes_view key{_key + ordinal, _field.key_width};
    uint32_t entry = _cursor.Entry();
    const uint32_t stop = _cursor.StopBefore(upper);
    while (entry < stop) {
      const uint32_t count = std::min(stop - entry, RangePruner::kMaxSelect);
      const auto* const keys = view.keys + uint64_t{entry} * view.stride;
      auto selected =
        _pruner.Select(head, keys + dropped, view.stride, width, count);
      for (; selected != 0; selected &= selected - 1) {
        const uint32_t hit = entry + std::countr_zero(selected);
        std::memcpy(_key + prefix.size(),
                    view.keys + uint64_t{hit} * view.stride, view.stride);
        _cursor.MoveTo(hit);
        _visit(key, _cursor.Meta());
      }
      entry += count;
    }
    if (stop != _cursor.BlockStop()) {
      return false;
    }
    _cursor.MoveTo(stop - 1);
    return true;
  }

  static constexpr size_t kMaxOrdinalBytes = 4;
  static constexpr size_t kPlannedRanges = 64;

  const TermReaderImpl& _field;
  std::span<const RangeLevel> _levels;
  Cursor _cursor;
  PaddedKey _probe;
  std::vector<Partial> _partial;
  const RangePruner& _pruner;
  TermReader::RangeVisitor _visit;
  uint32_t _prefix;
  uint32_t _stride;
  byte_type _first[kMaxStride];
  byte_type _next[kMaxStride];
  byte_type _last[kMaxStride];
  byte_type _key[kMaxStride + kMaxOrdinalBytes];
};

}  // namespace

void RangeWriter::Begin(PostingsWriter& pw, uint64_t docs) noexcept {
  _pw = &pw;
  _heavy = std::max(kHeavyDocs, docs / kTopPieces);
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

void RangeWriter::Keep(Piece& piece, const PostingMeta& meta) noexcept {
  piece.meta = meta;
  if (meta.inline_size != 0) {
    std::memcpy(piece.inline_bytes.data(), meta.inline_data, meta.inline_size);
    piece.meta.inline_data = piece.inline_bytes.data();
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
  auto& piece = range.held;
  if (piece.join) {
    absl::c_sort(piece.docs);
    piece.docs.erase(std::ranges::unique(piece.docs).begin(), piece.docs.end());
    piece.meta = {};
    _pw->WritePostings(
      PostingRows{
        .span = {.docs = piece.docs.data(), .count = piece.docs.size()},
      },
      piece.meta);
    piece.docs_count = piece.meta.docs_count;
  } else if (piece.meta.inline_size != 0) {
    piece.meta.inline_data = piece.inline_bytes.data();
  }
  _record.Clear();
  _pw->EncodeAbsolute(_record, piece.meta);
  range.records.append(_record.View());
  range.records.append(piece.meta.Inline());
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

void TermReaderImpl::VisitRange(bytes_view from, bytes_view to,
                                RangeVisitor visit) const {
  if ((!from.empty() && from > max()) || (!to.empty() && to <= min())) {
    return;
  }
  if (to.size() == from.size() + 1 && to.back() == 0 && to.starts_with(from)) {
    if (const auto meta = Lookup(from); meta.docs_count != 0) {
      visit(from, meta);
    }
    return;
  }
  if (from <= min()) {
    from = {};
  }
  if (!to.empty() && to > max()) {
    to = {};
  }
  RangeWalk{*this, from, to, visit}.Run();
}

void TermReaderImpl::VisitPruned(const RangePruner& pruner,
                                 RangeVisitor visit) const {
  PrunedWalk{*this, pruner, visit}.Run();
}

void TermReaderImpl::SplitPruned(const RangePruner& pruner, uint32_t parts,
                                 PrunedSplit& split, RangeVisitor visit) const {
  PrunedWalk{*this, pruner, visit}.Split(parts, split);
}

void TermReaderImpl::VisitSplit(const RangePruner& pruner, uint32_t level,
                                std::span<const uint32_t> pieces,
                                RangeVisitor visit) const {
  PrunedWalk{*this, pruner, visit}.Visit(level, pieces);
}

}  // namespace irs::term_dict
