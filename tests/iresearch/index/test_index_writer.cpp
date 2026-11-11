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

#include "test_index_writer.hpp"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <array>
#include <functional>
#include <iresearch/index/document_mask.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iterator>
#include <limits>
#include <span>
#include <utility>

namespace tests {
namespace {

struct Registry {
  absl::Mutex mutex;
  irs::containers::FlatHashMap<const irs::DirectoryAttributes*,
                               std::vector<irs::SegmentMeta>>
    committed ABSL_GUARDED_BY(mutex);
};

Registry& GetRegistry() {
  static Registry gRegistry;
  return gRegistry;
}

std::vector<irs::SegmentMeta> Committed(const irs::Directory& dir) {
  auto& registry = GetRegistry();
  absl::MutexLock lock{&registry.mutex};
  const auto it = registry.committed.find(&dir.attributes());
  return it == registry.committed.end() ? std::vector<irs::SegmentMeta>{}
                                        : it->second;
}

void Publish(const irs::Directory& dir,
             std::vector<irs::SegmentMeta> segments) {
  auto& registry = GetRegistry();
  absl::MutexLock lock{&registry.mutex};
  registry.committed.insert_or_assign(&dir.attributes(), std::move(segments));
}

irs::DirectoryReader Open(const irs::Directory& dir,
                          std::span<const irs::SegmentMeta> segments,
                          const irs::IndexReaderOptions& options) {
  std::vector<irs::SegmentReader> readers;
  readers.reserve(segments.size());
  for (const auto& meta : segments) {
    readers.emplace_back(irs::SegmentReaderImpl::Open(dir, meta, options));
  }
  return {dir, options, std::move(readers), /*stats=*/true};
}

void AddRemoved(irs::SegmentMeta& meta, irs::DocumentMaskBuilder removed) {
  if (removed.Empty()) {
    return;
  }
  if (meta.docs_mask) {
    removed.Merge(*meta.docs_mask);
  }
  meta.docs_mask =
    std::make_shared<const irs::DocumentMask>(std::move(removed).Finish());
  meta.live_docs_count = meta.docs_count - irs::RemovalCount(meta);
}

uint64_t SegmentId(const irs::SegmentMeta& meta) {
  uint64_t id = 0;
  irs::ParseSegmentId(meta.name, id);
  return id;
}

irs::FileRefs Pin(const irs::Directory& dir,
                  std::span<const irs::SegmentMeta> segments) {
  irs::FileRefs pins;
  auto& refs = dir.attributes().refs();
  for (const auto& meta : segments) {
    for (const auto& file : irs::SegmentFiles(meta.name, meta.kinds)) {
      pins.emplace_back(refs.add(file));
    }
  }
  return pins;
}

irs::doc_id_t DocAt(uint64_t ordinal) {
  return static_cast<irs::doc_id_t>(irs::doc_limits::min() + ordinal);
}

template<typename Visit>
void ForEachDoc(const irs::DocumentMask& docs, Visit&& visit) {
  irs::DocumentMask::Cursor cursor{docs};
  std::array<irs::doc_id_t, 64> batch;
  while (const auto count = cursor.Read(batch)) {
    for (const auto doc : std::span{batch}.first(count)) {
      visit(doc);
    }
  }
}

}  // namespace

IndexWriter::Transaction::Transaction(Transaction&& other) noexcept
  : _owner{std::exchange(other._owner, nullptr)},
    _exclusive{other._exclusive},
    _native{std::move(other._native)},
    _segment{std::exchange(other._segment, nullptr)},
    _first{other._first},
    _inserted{std::exchange(other._inserted, 0)},
    _touched{std::exchange(other._touched, false)},
    _field_options{std::move(other._field_options)},
    _removals{std::move(other._removals)} {}

IndexWriter::Transaction& IndexWriter::Transaction::operator=(
  Transaction&& other) noexcept {
  if (this != &other) {
    Abort();
    _owner = std::exchange(other._owner, nullptr);
    _exclusive = other._exclusive;
    _native = std::move(other._native);
    _segment = std::exchange(other._segment, nullptr);
    _first = other._first;
    _inserted = std::exchange(other._inserted, 0);
    _touched = std::exchange(other._touched, false);
    _field_options = std::move(other._field_options);
    _removals = std::move(other._removals);
  }
  return *this;
}

IndexWriter::Document IndexWriter::Transaction::Insert(
  bool, irs::doc_id_t batch_size) {
  SDB_ASSERT(_owner);
  _touched = true;
  if (_exclusive) {
    auto doc = Native().Insert(batch_size);
    _inserted += batch_size;
    return doc;
  }
  if (!_segment) {
    absl::MutexLock lock{&_owner->_mutex};
    _segment = &_owner->Acquire();
    _first = _segment->docs;
  }
  _segment->trx.SetFieldOptions(_field_options);
  absl::Cleanup lost = [&] {
    absl::MutexLock lock{&_owner->_mutex};
    _segment->docs = 0;
    _segment->spans.clear();
    _first = 0;
  };
  auto doc = _segment->trx.Insert(batch_size);
  std::move(lost).Cancel();
  _segment->docs += batch_size;
  _inserted += batch_size;
  return doc;
}

bool IndexWriter::Transaction::Commit() {
  if (!_owner) {
    return false;
  }
  if (!_touched && _removals.empty()) {
    return true;
  }
  Change change{.removals = std::move(_removals)};
  _removals.clear();
  _inserted = 0;
  _touched = false;
  if (_native.Valid()) {
    for (const auto& flushed : _native.FlushSegments(true)) {
      auto meta = flushed.meta;
      AddRemoved(meta, flushed.docs_mask);
      change.added.push_back(std::move(meta));
    }
    change.pins = Pin(_owner->_dir, change.added);
    _native.Abort();
  }
  absl::MutexLock lock{&_owner->_mutex};
  change.seq = ++_owner->_seq;
  if (_segment) {
    _segment->spans.push_back(
      {.seq = change.seq, .first = _first, .last = _segment->docs});
    _segment->held = false;
    _segment = nullptr;
  }
  _owner->_pending.push_back(std::move(change));
  return true;
}

void IndexWriter::Transaction::Abort() noexcept {
  _removals.clear();
  _inserted = 0;
  _touched = false;
  _native.Abort();
  if (!_segment) {
    return;
  }
  absl::MutexLock lock{&_owner->_mutex};
  if (absl::c_all_of(_segment->spans,
                     [](const Span& span) { return span.seq == 0; })) {
    _segment->trx.Abort();
    _segment->docs = 0;
    _segment->spans.clear();
  } else if (_segment->docs > _first) {
    _segment->spans.push_back({.first = _first, .last = _segment->docs});
  }
  _segment->held = false;
  _segment = nullptr;
}

irs::IndexWriter::Transaction& IndexWriter::Transaction::Native() {
  SDB_ASSERT(_owner);
  SDB_ASSERT(!_segment);
  _exclusive = true;
  _touched = true;
  if (!_native.Valid()) {
    _native = _owner->_writer->GetBatch();
    _native.SetFieldOptions(_field_options);
  }
  return _native;
}

void IndexWriter::Transaction::SetFieldOptions(
  std::shared_ptr<const irs::IndexFieldOptions> options) noexcept {
  _field_options = std::move(options);
  if (_native.Valid()) {
    _native.SetFieldOptions(_field_options);
  }
}

IndexWriter::ptr IndexWriter::Make(irs::Directory& dir, OpenMode mode,
                                   irs::IndexWriterOptions options) {
  return std::make_shared<IndexWriter>(dir, mode, std::move(options));
}

IndexWriter::IndexWriter(irs::Directory& dir, OpenMode mode,
                         irs::IndexWriterOptions options)
  : _dir{dir} {
  if (!options.reader_options.hnsw_bases) {
    _hnsw_bases = std::make_unique<irs::HnswBases>();
    options.reader_options.hnsw_bases = _hnsw_bases.get();
  }
  _reader_options = options.reader_options;
  if (!(mode & kOmAppend)) {
    Publish(dir, {});
  }
  _pins = Pin(dir, Committed(dir));
  _writer = irs::IndexWriter::Make(dir, std::move(options));
  uint64_t top = 0;
  dir.visit([&](std::string_view name) {
    if (uint64_t id = 0; irs::ParseSegmentId(name, id)) {
      top = std::max(top, id);
    }
    return true;
  });
  _writer->ReserveSegmentId(top);
}

IndexWriter::OpenSegment& IndexWriter::Acquire() {
  for (auto& segment : _open) {
    if (!segment->held) {
      segment->held = true;
      return *segment;
    }
  }
  auto& segment = *_open.emplace_back(std::make_unique<OpenSegment>());
  segment.trx = _writer->GetBatch();
  segment.held = true;
  return segment;
}

void IndexWriter::FlushFree() {
  for (auto& segment : _open) {
    if (segment->held || segment->spans.empty()) {
      continue;
    }
    if (absl::c_any_of(segment->spans,
                       [](const Span& span) { return span.seq != 0; })) {
      absl::Cleanup lost = [&] {
        _mutex.AssertHeld();
        std::erase_if(_pending, [&](const Change& change) {
          return absl::c_any_of(segment->spans, [&](const Span& span) {
            return span.seq == change.seq;
          });
        });
        segment->trx.Abort();
        segment->docs = 0;
        segment->spans.clear();
      };
      Flushed flushed;
      uint64_t at = 0;
      for (const auto& entry : segment->trx.FlushSegments(true)) {
        auto meta = entry.meta;
        AddRemoved(meta, entry.docs_mask);
        flushed.starts.push_back(at);
        at += meta.docs_count;
        flushed.metas.push_back(std::move(meta));
      }
      std::move(lost).Cancel();
      SDB_ASSERT(at == 0 || at == segment->docs);
      if (at != 0) {
        flushed.spans = std::move(segment->spans);
        flushed.pins = Pin(_dir, flushed.metas);
        _flushed.push_back(std::move(flushed));
      }
    }
    segment->trx.Abort();
    segment->docs = 0;
    segment->spans.clear();
  }
}

std::vector<irs::SegmentMeta> IndexWriter::Apply(
  std::vector<irs::SegmentMeta> segments, std::span<const Change> changes,
  std::span<const Flushed* const> flushed,
  std::span<irs::IndexWriter::Merged> merges) const {
  struct Unit {
    irs::SegmentMeta meta;
    uint64_t seq = 0;
    const Flushed* flushed = nullptr;
    uint64_t start = 0;
    irs::DocumentMaskBuilder removed;
  };
  std::vector<Unit> units;
  for (auto& meta : segments) {
    units.push_back({.meta = std::move(meta)});
  }
  const auto base = units.size();
  for (const auto* entry : flushed) {
    for (size_t i = 0; i < entry->metas.size(); ++i) {
      auto& unit = units.emplace_back(Unit{
        .meta = entry->metas[i], .flushed = entry, .start = entry->starts[i]});
      const auto end = unit.start + unit.meta.docs_count;
      unit.seq = std::numeric_limits<uint64_t>::max();
      for (const auto& span : entry->spans) {
        const auto first = std::max(span.first, unit.start);
        const auto last = std::min(span.last, end);
        if (first >= last) {
          continue;
        }
        if (span.seq == 0) {
          unit.removed.AddRange(DocAt(first - unit.start),
                                DocAt(last - unit.start));
        } else {
          unit.seq = std::min(unit.seq, span.seq);
        }
      }
    }
  }
  for (const auto& change : changes) {
    uint64_t start = 0;
    for (const auto& meta : change.added) {
      units.push_back({.meta = meta, .seq = change.seq, .start = start});
      start += meta.docs_count;
    }
  }

  irs::containers::FlatHashMap<uint64_t, size_t> positions;
  for (size_t i = 0; i < units.size(); ++i) {
    positions.emplace(SegmentId(units[i].meta), i);
  }
  std::optional<irs::DirectoryReader> current;
  const auto open = [&] -> const irs::DirectoryReader& {
    if (!current) {
      std::vector<irs::SegmentReader> readers;
      readers.reserve(units.size());
      for (const auto& unit : units) {
        readers.emplace_back(
          irs::SegmentReaderImpl::Open(_dir, unit.meta, _reader_options));
      }
      current.emplace(_dir, _reader_options, std::move(readers),
                      /*stats=*/false);
    }
    return *current;
  };
  const auto visible = [](const Unit& unit, irs::doc_id_t doc, uint64_t seq,
                          uint64_t bound) {
    const uint64_t ordinal = unit.start + (doc - irs::doc_limits::min());
    if (!unit.flushed) {
      return unit.seq < seq || (unit.seq == seq && ordinal < bound);
    }
    const auto& spans = unit.flushed->spans;
    const auto it = absl::c_upper_bound(
      spans, ordinal,
      [](uint64_t value, const Span& span) { return value < span.first; });
    if (it == spans.begin()) {
      return false;
    }
    const auto& span = *std::prev(it);
    if (ordinal >= span.last || span.seq == 0) {
      return false;
    }
    return span.seq < seq || (span.seq == seq && ordinal - span.first < bound);
  };
  for (const auto& change : changes) {
    for (const auto& removal : change.removals) {
      auto docs = removal.lazy ? removal.lazy(open()) : removal.docs;
      if (!docs) {
        continue;
      }
      for (const auto& group : docs->groups) {
        const auto it = positions.find(group.segment);
        if (it == positions.end()) {
          continue;
        }
        auto& unit = units[it->second];
        if (it->second < base) {
          unit.removed.Merge(group.docs);
          continue;
        }
        ForEachDoc(group.docs, [&](irs::doc_id_t doc) {
          if (visible(unit, doc, change.seq, removal.bound)) {
            unit.removed.Add(doc);
          }
        });
      }
    }
  }

  std::stable_sort(
    units.begin() + static_cast<ptrdiff_t>(base), units.end(),
    [](const Unit& lhs, const Unit& rhs) { return lhs.seq < rhs.seq; });
  segments.clear();
  std::vector<irs::SegmentMeta> added;
  for (size_t i = 0; i < units.size(); ++i) {
    auto& unit = units[i];
    AddRemoved(unit.meta, std::move(unit.removed));
    if (i < base) {
      segments.push_back(std::move(unit.meta));
    } else if (unit.meta.live_docs_count != 0) {
      added.push_back(std::move(unit.meta));
    }
  }

  for (auto& merged : merges) {
    const auto& candidates = merged.claim.candidates;
    std::vector<size_t> found;
    for (const auto* candidate : candidates) {
      const auto it = absl::c_find_if(segments, [&](const auto& meta) {
        return meta.name == candidate->Meta().name;
      });
      if (it == segments.end()) {
        break;
      }
      found.push_back(static_cast<size_t>(it - segments.begin()));
    }
    if (candidates.empty() || found.size() != candidates.size()) {
      continue;
    }
    auto meta = merged.meta;
    irs::DocumentMaskBuilder carried;
    for (size_t i = 0; i < candidates.size(); ++i) {
      const auto& now = segments[found[i]];
      if (!now.docs_mask) {
        continue;
      }
      irs::DocumentMaskBuilder delta{*now.docs_mask};
      if (const auto* then = candidates[i]->docs_mask()) {
        delta.Subtract(*then);
      }
      if (delta.Empty()) {
        continue;
      }
      const auto docs = std::move(delta).Finish();
      for (const auto& map : merged.maps) {
        if (map.source == SegmentId(now)) {
          irs::MapDocs(map, docs, carried);
        }
      }
    }
    AddRemoved(meta, std::move(carried));
    absl::c_sort(found, std::greater<>{});
    for (const auto position : found) {
      segments.erase(segments.begin() + static_cast<ptrdiff_t>(position));
    }
    segments.push_back(std::move(meta));
  }
  std::erase_if(segments, [](const irs::SegmentMeta& meta) {
    return meta.live_docs_count == 0;
  });
  absl::c_move(added, std::back_inserter(segments));
  return segments;
}

bool IndexWriter::RefreshBegin() {
  absl::MutexLock lock{&_mutex};
  if (_staged) {
    return false;
  }
  FlushFree();
  uint64_t cut = _seq + 1;
  for (const auto& segment : _open) {
    for (const auto& span : segment->spans) {
      if (span.seq != 0) {
        cut = std::min(cut, span.seq);
      }
    }
  }
  std::vector<bool> blocked(_flushed.size());
  for (bool changed = true; changed;) {
    changed = false;
    for (size_t i = 0; i < _flushed.size(); ++i) {
      const auto& spans = _flushed[i].spans;
      if (blocked[i] || absl::c_none_of(spans, [&](const Span& span) {
            return span.seq >= cut;
          })) {
        continue;
      }
      blocked[i] = true;
      changed = true;
      for (const auto& span : spans) {
        if (span.seq != 0) {
          cut = std::min(cut, span.seq);
        }
      }
    }
  }
  Staged staged{.cut = cut, .merges = _merges.size()};
  std::vector<const Flushed*> flushed;
  for (size_t i = 0; i < _flushed.size(); ++i) {
    if (!blocked[i]) {
      staged.flushed.push_back(i);
      flushed.push_back(&_flushed[i]);
    }
  }
  const auto ready = static_cast<size_t>(
    absl::c_find_if(_pending,
                    [&](const Change& change) { return change.seq >= cut; }) -
    _pending.begin());
  if (ready == 0 && staged.merges == 0 && flushed.empty()) {
    return false;
  }
  staged.segments =
    Apply(Committed(_dir), std::span{_pending}.first(ready), flushed, _merges);
  _staged = std::move(staged);
  return true;
}

void IndexWriter::RefreshFinish() {
  std::vector<irs::IndexWriter::Merged> applied;
  absl::MutexLock lock{&_mutex};
  if (!_staged) {
    return;
  }
  auto& staged = *_staged;
  _pins = Pin(_dir, staged.segments);
  Publish(_dir, std::move(staged.segments));
  std::erase_if(_pending,
                [&](const Change& change) { return change.seq < staged.cut; });
  for (auto it = staged.flushed.rbegin(); it != staged.flushed.rend(); ++it) {
    _flushed.erase(_flushed.begin() + static_cast<ptrdiff_t>(*it));
  }
  const auto last = _merges.begin() + static_cast<ptrdiff_t>(staged.merges);
  auto& target =
    _policy_thread.load() == std::this_thread::get_id() ? _retired : applied;
  std::move(_merges.begin(), last, std::back_inserter(target));
  _merges.erase(_merges.begin(), last);
  _staged.reset();
}

void IndexWriter::RefreshAbort() {
  absl::MutexLock lock{&_mutex};
  _staged.reset();
}

bool IndexWriter::RefreshCommit() {
  const bool modified = RefreshBegin();
  RefreshFinish();
  return modified;
}

irs::DirectoryReader IndexWriter::GetSnapshot() const {
  return Open(_dir, Committed(_dir), _reader_options);
}

CompactionResult IndexWriter::Compact(
  const irs::CompactionPolicy& policy,
  const irs::IndexFieldOptions* field_options,
  const irs::MergeWriter::FlushProgress& progress) {
  const irs::CompactionPolicy guarded =
    [&](irs::Compaction& candidates, const irs::IndexReader& reader,
        const irs::CompactingSegments& compacting) {
      _policy_thread.store(std::this_thread::get_id());
      absl::Cleanup left = [&] { _policy_thread.store({}); };
      policy(candidates, reader, compacting);
    };
  auto merged = irs::GetBlocking(_writer->MergeAsync(guarded, GetSnapshot(),
                                                     field_options, progress,
                                                     /*env=*/nullptr));
  std::vector<irs::IndexWriter::Merged> retired;
  {
    absl::MutexLock lock{&_mutex};
    retired = std::move(_retired);
  }
  retired.clear();
  const auto size = merged.claim.candidates.size();
  if (size == 0) {
    return {.size = 0, .error = CompactionError::Ok};
  }
  if (!merged.reader) {
    return {.size = 0, .error = CompactionError::Fail};
  }
  const auto committed = Committed(_dir);
  for (const auto* candidate : merged.claim.candidates) {
    if (absl::c_none_of(committed, [&](const irs::SegmentMeta& meta) {
          return meta.name == candidate->Meta().name;
        })) {
      return {.size = 0, .error = CompactionError::Busy};
    }
  }
  absl::MutexLock lock{&_mutex};
  _merges.push_back(std::move(merged));
  return {.size = size, .error = CompactionError::Ok};
}

irs::DirectoryReader OpenReader(const irs::Directory& dir,
                                const irs::IndexReaderOptions& options) {
  return Open(dir, Committed(dir), options);
}

}  // namespace tests
