////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "index_writer.hpp"

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/strings/str_cat.h>

#include <cstdint>
#include <shared_mutex>
#include <type_traits>
#include <yaclib/async/contract.hpp>
#include <yaclib/async/make.hpp>

#include "basics/assert.h"
#include "basics/resource_manager.hpp"
#include "basics/shared.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/index/comparer.hpp"
#include "iresearch/index/directory_reader_impl.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/merge_writer.hpp"
#include "iresearch/index/segment_reader.hpp"
#include "iresearch/index/segment_reader_impl.hpp"
#include "iresearch/index/segment_writer.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/compression.hpp"
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

// do-nothing progress reporter, used as fallback if no other progress
// reporter is used
const ProgressReportCallback kNoProgress =
  [](std::string_view /*phase*/, size_t /*current*/, size_t /*total*/) {
    // intentionally do nothing
  };

const ColumnInfoProvider kDefaultColumnInfo = [](std::string_view) {
  // no compression, no encryption
  return ColumnInfo{irs::Type<compression::None>::get(), {}, false};
};

const FeatureInfoProvider kDefaultFeatureInfo = [](irs::IndexFeatures) {
  // no compression, no encryption
  return std::pair{ColumnInfo{irs::Type<compression::None>::get(), {}, false},
                   FeatureWriterFactory{}};
};

struct FlushedSegmentContext {
  FlushedSegmentContext(std::shared_ptr<const SegmentReaderImpl>&& reader,
                        IndexWriter::SegmentContext& segment,
                        IndexWriter::FlushedSegment& flushed,
                        const ResourceManagementOptions& rm,
                        absl::flat_hash_set<uint64_t>& matched_replace_ticks)
    : reader{std::move(reader)},
      segment{segment},
      flushed{flushed},
      replace_hits(segment.queries.size(), 0),
      matched_replace_ticks{matched_replace_ticks} {
    SDB_ASSERT(this->reader != nullptr);
    if (flushed.docs_mask.count != doc_limits::eof()) {
      SDB_ASSERT(flushed.document_mask.empty());
      Init(rm);
    }
  }

  std::shared_ptr<const SegmentReaderImpl> reader;
  IndexWriter::SegmentContext& segment;
  IndexWriter::FlushedSegment& flushed;
  std::vector<uint8_t> replace_hits;
  absl::flat_hash_set<uint64_t>& matched_replace_ticks;

  void RemoveByPacket(const IndexWriter::DeletePacket& packet) {
    if (!packet.filter) {
      return;
    }

    auto prepared = packet.filter->prepare({.index = *reader});
    if (!prepared) {
      return;
    }

    auto itr = prepared->execute(
      {.segment = *reader, .pending_docs_mask = &flushed.document_mask});
    if (!itr) {
      return;
    }

    bool matched_any = false;
    auto* flushed_docs = segment.flushed_docs.data() + flushed.GetDocsBegin();
    while (itr->next()) {
      const auto new_doc = itr->value();
      const auto old_doc = New2Old(new_doc);
      const auto& doc = flushed_docs[old_doc - doc_limits::min()];

      if (packet.tick < doc.tick) {
        continue;
      }

      bool source_replace_matched = false;
      if (doc.query_id != writer_limits::kInvalidOffset) {
        SDB_ASSERT(doc.query_id < segment.queries.size());
        SDB_ASSERT(doc.query_id < replace_hits.size());
        source_replace_matched =
          replace_hits[doc.query_id] ||
          matched_replace_ticks.contains(segment.queries[doc.query_id].tick);
      }

      // Preserve old replace dependency semantics: a replace packet should not
      // match docs produced by an unresolved replace.
      if (packet.kind == IndexWriter::DeletePacketKind::KReplace &&
          doc.query_id != writer_limits::kInvalidOffset &&
          !source_replace_matched) {
        continue;
      }

      if (!flushed.document_mask.insert(new_doc).second) {
        continue;
      }

      matched_any = true;

      if (packet.kind == IndexWriter::DeletePacketKind::KReplace &&
          doc.query_id != writer_limits::kInvalidOffset) {
        replace_hits[doc.query_id] = 1;
      }
    }

    if (packet.kind != IndexWriter::DeletePacketKind::KReplace ||
        !matched_any) {
      return;
    }

    matched_replace_ticks.emplace(packet.tick);

    // If replace removed a non-replace document, mark the originating replace
    // query by packet identity to preserve old Done/DependsOn semantics.
    for (size_t i = 0; i < segment.queries.size(); ++i) {
      const auto& query = segment.queries[i];
      if (query.tick == packet.tick && query.filter == packet.filter) {
        replace_hits[i] = 1;
        break;
      }
    }
  }

  bool MakeDocumentMask(uint64_t tick, DocumentMask& document_mask,
                        IndexSegment& index) {
    if (flushed.document_mask.size() == flushed.meta.docs_count) {
      return true;
    }
    const auto begin = flushed.GetDocsBegin();
    const auto end = flushed.GetDocsEnd();
    SDB_ASSERT(begin < end);
    SDB_ASSERT(segment.flushed_docs[begin].tick <= tick);
    const auto flushed_last_tick = segment.flushed_docs[end - 1].tick;
    if (flushed_last_tick <= tick) {
      document_mask = std::move(flushed.document_mask);
      index = std::move(flushed);
      return false;
    }
    // intentionally copy
    document_mask = flushed.document_mask;
    for (auto rbegin = end - 1;
         rbegin > begin && segment.flushed_docs[rbegin].tick > tick; --rbegin) {
      const auto old_doc = rbegin - begin + doc_limits::min();
      const auto new_doc = Old2New(old_doc);
      document_mask.insert(new_doc);
    }
    if (document_mask.size() == flushed.meta.docs_count) {
      return true;
    }
    index = flushed;
    return false;
  }

  void MaskUnusedReplace(uint64_t first_tick, uint64_t last_tick) const;

 private:
  void Init(const ResourceManagementOptions& rm) {
    if (!flushed.old2new.empty() && flushed.new2old.empty()) {
      flushed.new2old =
        decltype(flushed.new2old){flushed.old2new.size(), {*rm.transactions}};
      for (doc_id_t old_id = 0; const auto new_id : flushed.old2new) {
        flushed.new2old[new_id] = old_id++;
      }
    }

    DocumentMask document_mask{{*rm.readers}};

    SDB_ASSERT(flushed.GetDocsBegin() < flushed.GetDocsEnd());
    const auto end = flushed.GetDocsEnd() - flushed.GetDocsBegin();
    const auto invalid_end = static_cast<size_t>(flushed.meta.docs_count);

    // TODO(mbkkt) Is it good?
    document_mask.reserve(flushed.docs_mask.count + (invalid_end - end));

    // translate removes
    // https://lemire.me/blog/2018/02/21/iterating-over-set-bits-quickly
    const auto word_count =
      std::min(bitset::bits_to_words(end), flushed.docs_mask.set.words());
    for (size_t word_idx = 0; word_idx != word_count; ++word_idx) {
      auto word = flushed.docs_mask.set[word_idx];
      const auto old_doc =
        word_idx * BitsRequired<bitset::word_t>() + doc_limits::min();
      while (word != 0) {
        const auto t = word & -word;
        const auto offset = std::countr_zero(word);
        const auto new_doc = Old2New(old_doc + offset);
        document_mask.insert(new_doc);
        word ^= t;
      }
    }
    SDB_ASSERT(document_mask.size() == flushed.docs_mask.count);

    // in case of bad_alloc it's possible that we still need docs_mask
    // so we cannot reset it here: flushed.docs_mask = {};

    // lazy apply rollback
    for (auto old_doc = end + doc_limits::min(),
              end_doc = invalid_end + doc_limits::min();
         old_doc < end_doc; ++old_doc) {
      const auto new_doc = Old2New(old_doc);
      document_mask.insert(new_doc);
    }

    flushed.document_mask = std::move(document_mask);
    flushed.docs_mask.set = {};
    // set count to invalid
    flushed.docs_mask.count = doc_limits::eof();
  }

  static doc_id_t Translate(const auto& map, auto from) noexcept {
    SDB_ASSERT(doc_limits::invalid() < from);
    SDB_ASSERT(from <= doc_limits::eof());
    if (map.empty()) {
      return static_cast<doc_id_t>(from);
    }
    SDB_ASSERT(from < map.size());
    return map[from];
  }

  doc_id_t New2Old(auto new_doc) const noexcept {
    return Translate(flushed.new2old, new_doc);
  }
  doc_id_t Old2New(auto old_doc) const noexcept {
    return Translate(flushed.old2new, old_doc);
  }
};

// Apply any document removals based on filters in the segment.
// modifications where to get document update_contexts from
// docs_mask where to apply document removals to
// readers readers by segment name
// meta key used to get reader for the segment to evaluate
// Return if any new records were added (modification_queries_ modified).
bool RemoveFromExistingSegmentByPacket(DocumentMask& deleted_docs,
                                       const IndexWriter::DeletePacket& packet,
                                       const SubReader& reader) {
  if (packet.filter == nullptr) {
    return false;
  }

  auto prepared = packet.filter->prepare({.index = reader});

  if (!prepared) [[unlikely]] {
    return false;  // skip invalid prepared filters
  }

  auto itr =
    prepared->execute({.segment = reader, .pending_docs_mask = &deleted_docs});

  if (!itr) [[unlikely]] {
    return false;  // skip invalid iterators
  }

  bool matched = false;
  const auto* docs_mask = reader.docs_mask();
  while (itr->next()) {
    const auto doc_id = itr->value();

    // if the indexed doc_id was already masked then it should be skipped
    if (docs_mask && docs_mask->contains(doc_id)) {
      continue;  // the current modification query does not match any records
    }
    if (deleted_docs.insert(doc_id).second) {
      matched = true;
    }
  }
  return matched;
}

bool RemoveFromImportedSegmentByPacket(DocumentMask& deleted_docs,
                                       const IndexWriter::DeletePacket& packet,
                                       const SubReader& reader) {
  if (packet.filter == nullptr) {
    return false;
  }

  auto prepared = packet.filter->prepare({.index = reader});
  if (!prepared) [[unlikely]] {
    return false;  // skip invalid prepared filters
  }

  auto itr =
    prepared->execute({.segment = reader, .pending_docs_mask = &deleted_docs});
  if (!itr) [[unlikely]] {
    return false;  // skip invalid iterators
  }

  bool modified = false;
  while (itr->next()) {
    const auto doc_id = itr->value();
    if (deleted_docs.insert(doc_id).second) {
      modified = true;
    }
  }

  return modified;
}

// Mask documents created by replace which did not have any matches.
void FlushedSegmentContext::MaskUnusedReplace(uint64_t first_tick,
                                              uint64_t last_tick) const {
  const auto begin = flushed.GetDocsBegin();
  const auto end = flushed.GetDocsEnd();
  const std::span docs{segment.flushed_docs.data() + begin,
                       segment.flushed_docs.data() + end};
  for (const auto& doc : docs) {
    if (doc.tick <= first_tick) {
      continue;
    }
    if (last_tick < doc.tick) {
      break;
    }
    if (doc.query_id == writer_limits::kInvalidOffset) {
      continue;
    }

    SDB_ASSERT(doc.query_id < replace_hits.size());

    const bool matched_outside =
      matched_replace_ticks.contains(segment.queries[doc.query_id].tick);

    if (!replace_hits[doc.query_id] && !matched_outside) {
      const auto new_doc =
        Old2New(static_cast<size_t>(&doc - docs.data()) + doc_limits::min());
      flushed.document_mask.insert(new_doc);
    }
  }
}

struct CandidateMapping {
  const SubReader* new_segment{};
  struct Old {
    const SubReader* segment{};
    size_t index{};  // within merge_writer
  } old;
};

// mapping: name -> { new segment, old segment }
using CandidatesMapping =
  absl::flat_hash_map<std::string_view, CandidateMapping>;

struct MapCandidatesResult {
  // Number of mapped candidates.
  size_t count{0};
  bool has_removals{false};
};

// candidates_mapping output mapping
// candidates candidates for mapping
// segments map against a specified segments
MapCandidatesResult MapCandidates(CandidatesMapping& candidates_mapping,
                                  ConsolidationView candidates,
                                  const auto& index) {
  size_t num_candidates = 0;
  candidates_mapping.reserve(candidates.size());
  for (const auto* candidate : candidates) {
    candidates_mapping.emplace(
      candidate->Meta().name,
      CandidateMapping{.old = {candidate, num_candidates++}});
  }

  size_t found = 0;
  bool has_removals = false;
  const auto candidate_not_found = candidates_mapping.end();

  for (const auto& segment : index) {
    const auto& meta = segment.Meta();
    const auto it = candidates_mapping.find(meta.name);

    if (candidate_not_found == it) {
      // not a candidate
      continue;
    }

    auto& mapping = it->second;
    const auto* new_segment = mapping.new_segment;

    if (new_segment && new_segment->Meta().version >= meta.version) {
      // mapping already has a newer segment version
      continue;
    }

    SDB_ASSERT(mapping.old.segment);
    if constexpr (std::is_same_v<SegmentReader,
                                 std::decay_t<decltype(segment)>>) {
      mapping.new_segment = segment.GetImpl().get();
    } else {
      mapping.new_segment = &segment;
    }

    // FIXME(gnusi): can't we just check pointers?
    SDB_ASSERT(mapping.old.segment);
    has_removals |= (meta.version != mapping.old.segment->Meta().version);

    if (++found == num_candidates) {
      break;
    }
  }

  return {found, has_removals};
}

bool MapRemovals(const CandidatesMapping& candidates_mapping,
                 const MergeWriter& merger, DocumentMask& docs_mask) {
  SDB_ASSERT(merger);

  for (auto& mapping : candidates_mapping) {
    const auto& segment_mapping = mapping.second;
    const auto* new_segment = segment_mapping.new_segment;
    SDB_ASSERT(new_segment);
    const auto& new_meta = new_segment->Meta();
    SDB_ASSERT(segment_mapping.old.segment);
    const auto& old_meta = segment_mapping.old.segment->Meta();

    if (new_meta.version != old_meta.version) {
      const auto& merge_ctx = merger[segment_mapping.old.index];
      auto merged_itr = merge_ctx.reader->docs_iterator();
      auto current_itr = new_segment->docs_iterator();

      // this only masks documents of a single segment
      // this works due to the current architectural approach of segments,
      // either removals are new and will be applied during flush_all()
      // or removals are in the docs_mask and still be applied by the reader
      // passed to the merge_writer

      // no more docs in merged reader
      if (!merged_itr->next()) {
        if (current_itr->next()) {
          SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
                   "Failed to map removals for consolidated segment '",
                   old_meta.name, "' version '", old_meta.version,
                   "' from current segment '", new_meta.name, "' version '",
                   new_meta.version, "', current segment has doc_id '",
                   current_itr->value(),
                   "' not present in the consolidated segment");

          return false;  // current reader has unmerged docs
        }

        continue;  // continue wih next mapping
      }

      // mask all remaining doc_ids
      if (!current_itr->next()) {
        do {
          SDB_ASSERT(doc_limits::valid(merge_ctx.doc_map(
            merged_itr->value())));  // doc_id must have a valid mapping
          docs_mask.insert(merge_ctx.doc_map(merged_itr->value()));
        } while (merged_itr->next());

        continue;  // continue wih next mapping
      }

      // validate that all docs in the current reader were merged, and add any
      // removed docs to the merged mask
      for (;;) {
        while (merged_itr->value() < current_itr->value()) {
          // doc_id must have a valid mapping
          SDB_ASSERT(doc_limits::valid(merge_ctx.doc_map(merged_itr->value())));
          docs_mask.insert(merge_ctx.doc_map(merged_itr->value()));

          if (!merged_itr->next()) {
            SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
                     "Failed to map removals for consolidated segment '",
                     old_meta.name, "' version '", old_meta.version,
                     "' from current segment '", new_meta.name, "' version '",
                     new_meta.version, "', current segment has doc_id '",
                     current_itr->value(),
                     "' not present in the consolidated segment");

            return false;  // current reader has unmerged docs
          }
        }

        if (merged_itr->value() > current_itr->value()) {
          SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
                   "Failed to map removals for consolidated segment '",
                   old_meta.name, "' version '", old_meta.version,
                   "' from current segment '", new_meta.name, "' version '",
                   new_meta.version, "', current segment has doc_id '",
                   current_itr->value(),
                   "' not present in the consolidated segment");

          return false;  // current reader has unmerged docs
        }

        // no more docs in merged reader
        if (!merged_itr->next()) {
          if (current_itr->next()) {
            SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
                     "Failed to map removals for consolidated segment '",
                     old_meta.name, "' version '", old_meta.version,
                     "' from current segment '", new_meta.name, "' version '",
                     new_meta.version, "', current segment has doc_id '",
                     current_itr->value(),
                     "' not present in the consolidated segment");

            return false;  // current reader has unmerged docs
          }

          break;  // continue wih next mapping
        }

        // mask all remaining doc_ids
        if (!current_itr->next()) {
          do {
            // doc_id must have a valid mapping
            SDB_ASSERT(
              doc_limits::valid(merge_ctx.doc_map(merged_itr->value())));
            docs_mask.insert(merge_ctx.doc_map(merged_itr->value()));
          } while (merged_itr->next());

          break;  // continue wih next mapping
        }
      }
    }
  }

  return true;
}

std::string ToString(ConsolidationView consolidation) {
  std::string str;

  size_t total_size = 0;
  size_t total_docs_count = 0;
  size_t total_live_docs_count = 0;

  for (const auto* segment : consolidation) {
    auto& meta = segment->Meta();

    absl::StrAppend(&str, "Name='", meta.name,
                    "', docs_count=", meta.docs_count,
                    ", live_docs_count=", meta.live_docs_count,
                    ", size=", meta.byte_size, "\n");

    total_docs_count += meta.docs_count;
    total_live_docs_count += meta.live_docs_count;
    total_size += meta.byte_size;
  }

  absl::StrAppend(&str, "Total: segments=", consolidation.size(),
                  ", docs_count=", total_docs_count,
                  ", live_docs_count=", total_live_docs_count,
                  " size=", total_size, "");

  return str;
}

bool IsInitialCommit(const DirectoryMeta& meta) noexcept {
  // Initial commit is always for required for empty directory
  return meta.filename.empty();
}

struct PartialSync {
  explicit PartialSync(size_t segment_index) noexcept
    : segment_index{segment_index} {}

  size_t segment_index;  // Index of the segment within index meta
};

std::vector<std::string_view> GetFilesToSync(
  std::span<const IndexSegment> segments,
  std::span<const PartialSync> partial_sync, size_t partial_sync_threshold) {
  // TODO(gnusi): make format dependent?
  static constexpr size_t kMaxFilesPerSegment = 8;

  SDB_ASSERT(partial_sync_threshold <= segments.size());
  const size_t full_sync_count = segments.size() - partial_sync_threshold;

  std::vector<std::string_view> files_to_sync;
  // +1 for index meta
  files_to_sync.reserve(1 + partial_sync.size() +
                        full_sync_count * kMaxFilesPerSegment);

  for (auto sync : partial_sync) {
    SDB_ASSERT(sync.segment_index < partial_sync_threshold);
    const auto& segment = segments[sync.segment_index];
    files_to_sync.emplace_back(segment.filename);
  }

  std::for_each(segments.begin() + partial_sync_threshold, segments.end(),
                [&files_to_sync](const IndexSegment& segment) {
                  files_to_sync.emplace_back(segment.filename);
                  const auto& files = segment.meta.files;
                  SDB_ASSERT(files.size() <= kMaxFilesPerSegment);
                  files_to_sync.insert(files_to_sync.end(), files.begin(),
                                       files.end());
                });

  return files_to_sync;
}

uint64_t LimitTick(uint64_t tick, uint64_t def) noexcept {
  return tick != writer_limits::kMaxTick ? tick : def;
}

auto CopyMask(const Directory& dir, const auto& segment) {
  if (const auto* mask = segment.docs_mask(); mask) {
    return std::make_shared<DocumentMask>(*mask);
  } else {
    return std::make_shared<DocumentMask>(*dir.ResourceManager().readers);
  }
}

}  // namespace

using namespace std::chrono_literals;

IndexWriter::ActiveSegmentContext::ActiveSegmentContext(
  std::shared_ptr<SegmentContext> segment, std::atomic_size_t& segments_active,
  FlushContext* flush, size_t pending_segment_offset) noexcept
  : _segment{std::move(segment)},
    _segments_active{&segments_active},
    _flush{flush},
    _pending_segment_offset{pending_segment_offset} {
  SDB_ASSERT(_segment != nullptr);
}

IndexWriter::ActiveSegmentContext::~ActiveSegmentContext() {
  if (_segments_active == nullptr) {
    SDB_ASSERT(_segment == nullptr);
    SDB_ASSERT(_flush == nullptr);
    return;
  }
  _segment.reset();
  _segments_active->fetch_sub(1, std::memory_order_relaxed);
  if (_flush != nullptr) {
    _flush->pending.Done();
  }
}

IndexWriter::ActiveSegmentContext::ActiveSegmentContext(
  ActiveSegmentContext&& other) noexcept
  : _segment{std::move(other._segment)},
    _segments_active{std::exchange(other._segments_active, nullptr)},
    _flush{std::exchange(other._flush, nullptr)},
    _pending_segment_offset{std::exchange(other._pending_segment_offset,
                                          writer_limits::kInvalidOffset)} {}

IndexWriter::ActiveSegmentContext& IndexWriter::ActiveSegmentContext::operator=(
  ActiveSegmentContext&& other) noexcept {
  if (this != &other) {
    std::swap(_segment, other._segment);
    std::swap(_segments_active, other._segments_active);
    std::swap(_flush, other._flush);
    std::swap(_pending_segment_offset, other._pending_segment_offset);
  }
  return *this;
}

void IndexWriter::DeletePacketQueue::AppendBatch(
  std::vector<DeletePacket>&& batch) {
  if (batch.empty()) {
    return;
  }

  std::lock_guard lock{_mutex};
  _packets.reserve(_packets.size() + batch.size());
  for (auto& packet : batch) {
    packet.id = ++_next_id;
#ifdef SDB_DEV
    if (!_packets.empty()) {
      SDB_ASSERT(_packets.back().id < packet.id);
    }
#endif
    _packets.emplace_back(std::move(packet));
  }
}

size_t IndexWriter::DeletePacketQueue::Size() const noexcept {
  std::lock_guard lock{_mutex};
  return _packets.size();
}

std::vector<IndexWriter::DeletePacket>
IndexWriter::DeletePacketQueue::Snapshot() const {
  std::lock_guard lock{_mutex};
  return _packets;
}

void IndexWriter::DeletePacketQueue::PruneUpTo(
  uint64_t barrier_delete_packet_id) {
  std::lock_guard lock{_mutex};
  auto it =
    std::find_if(_packets.begin(), _packets.end(),
                 [barrier_delete_packet_id](const DeletePacket& packet) {
                   return packet.id > barrier_delete_packet_id;
                 });
  _packets.erase(_packets.begin(), it);
}

void IndexWriter::EnqueueDeletePackets(
  const SegmentContext& segment, std::span<const PendingDeletePacketRef> refs) {
  std::vector<DeletePacket> batch;
  batch.reserve(refs.size());

  for (const auto& ref : refs) {
    SDB_ASSERT(ref.query_index < segment.queries.size());
    const auto& query = segment.queries[ref.query_index];

    if (query.filter == nullptr) {
      continue;
    }

    batch.emplace_back(DeletePacket{
      .tick = query.tick,
      .kind = ref.kind,
      .filter = query.filter,
    });
    SDB_ASSERT(writer_limits::kMinTick < query.tick);
  }

  _delete_packets.AppendBatch(std::move(batch));
}

IndexWriter::Document::Document(SegmentContext& segment,
                                SegmentWriter::DocContext doc,
                                doc_id_t batch_size, QueryContext* query)
  : _writer{*segment.active.writer}, _query{query} {
  SDB_ASSERT(segment.active.writer != nullptr);
  // ensure Reset() will be noexcept
  _doc_id = _writer.begin(doc, batch_size);
  SDB_ASSERT(irs::doc_limits::valid(_doc_id));
  segment.buffered_docs.store(_writer.buffered_docs(),
                              std::memory_order_relaxed);
}

IndexWriter::Document::~Document() noexcept { Finish(); }

void IndexWriter::Document::Finish() noexcept {
  try {
    _writer.commit();
  } catch (...) {
    _writer.rollback();
  }
  if (!_writer.valid() && _query != nullptr) {
    // TODO(mbkkt) segment.queries.pop_back()?
    _query->filter = nullptr;
  }
}

void IndexWriter::Transaction::Reset() noexcept {
  _pending_delete_packets.clear();
  // TODO(mbkkt) rename Reset() to Rollback()
  if (auto* segment = _active.Segment(); segment != nullptr) {
    segment->DiscardPendingFlush();
    segment->Rollback();
  }
}

void IndexWriter::Transaction::RegisterFlush() noexcept {
  if (_active.Segment() != nullptr && _active.Flush() == nullptr) {
    _writer->GetFlushContext()->AddToPending(_active);
  }
}

bool IndexWriter::Transaction::CommitImpl(uint64_t last_tick) noexcept try {
  auto* segment = _active.Segment();
  SDB_ASSERT(segment != nullptr);

  // Ensure segment registration is done before packet enqueue,
  // so CommitImpl has no throwing steps after queue mutation.
  RegisterFlush();

  segment->Commit(_queries, last_tick);

  _writer->EnqueueDeletePackets(*segment, _pending_delete_packets);
  _pending_delete_packets.clear();

  _writer->GetFlushContext()->Emplace(std::move(_active));
  SDB_ASSERT(_active.Segment() == nullptr);
  return true;
} catch (...) {
  _pending_delete_packets.clear();
  if (_active.Segment() != nullptr) {
    // TODO(mbkkt) Use intrusive list to avoid possibility bad_alloc here
    Abort();
  }
  return false;
}

void IndexWriter::Transaction::Abort() noexcept {
  _pending_delete_packets.clear();
  auto* segment = _active.Segment();
  if (segment == nullptr) {
    return;  // nothing to do
  }
  segment->DiscardPendingFlush();
  if (_active.Flush() == nullptr) {
    segment->Reset();  // reset before returning to pool
    _active = {};      // back to pool no needed Rollback
    return;
  }
  segment->Rollback();
  // cannot throw because active_.Flush() not null
  _writer->GetFlushContext()->Emplace(std::move(_active));
  SDB_ASSERT(_active.Segment() == nullptr);
}

void IndexWriter::Transaction::UpdateSegment(bool disable_flush) {
  SDB_ASSERT(Valid());
  while (_active.Segment() == nullptr) {  // lazy init
    _active = _writer->GetSegmentContext();
  }

  auto& segment = *_active.Segment();
  auto& writer = *segment.active.writer;
  auto& async_flush = _writer->_async_flush;

  if (writer.initialized()) [[likely]] {
    if (disable_flush || !_writer->FlushRequired(writer)) {
      return;
    }
    // Force flush of a full segment
    SDB_TRACE("xxxxx", sdb::Logger::IRESEARCH, "Flushing segment '",
              writer.name(), "', docs=", writer.buffered_docs(),
              ", memory=", writer.memory_active(),
              ", docs limit=", _writer->_segment_limits.Docs(),
              ", memory limit=", _writer->_segment_limits.Memory());

    auto meta_name = segment.active.writer_meta.meta.name;
    try {
      segment.StartFlush(async_flush);
    } catch (...) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("while flushing segment '", meta_name,
                             "', error: failed to flush segment"));
      // TODO(mbkkt) What the goal are we want to achieve
      //  with keeping already flushed data?
      segment.Reset(true);
      throw;
    }
  }
  segment.Prepare();
}

bool IndexWriter::FlushRequired(const SegmentWriter& segment) const noexcept {
  return _segment_limits.Memory() <= segment.memory_active() ||
         _segment_limits.Docs() <= segment.buffered_docs();
}

void IndexWriter::FlushContext::Emplace(ActiveSegmentContext&& active) {
  SDB_ASSERT(active._segment != nullptr);

  if (active._segment->first_tick == writer_limits::kMaxTick) {
    // Reset all segment data because there wasn't successful transactions
    active._segment->Reset();
    active = {};  // release
    return;
  }

  auto* flush = active._flush;
  const bool is_null = flush == nullptr;
  if (!is_null && flush != this) {
    active = {};  // release
    return;
  }

  std::lock_guard lock{pending.Mutex()};
  auto* node = [&] {
    if (is_null) {
      return &pending_segments.emplace_back(std::move(active._segment),
                                            pending_segments.size());
    }
    SDB_ASSERT(active._pending_segment_offset < pending_segments.size());
    auto& segment_context = pending_segments[active._pending_segment_offset];
    SDB_ASSERT(segment_context.segment == active._segment);
    return &segment_context;
  }();
  pending_freelist.push(*node);
  active = {};
}

void IndexWriter::FlushContext::AddToPending(ActiveSegmentContext& active) {
  std::lock_guard lock{pending.Mutex()};
  const auto size_before = pending_segments.size();
  SDB_ASSERT(active._segment != nullptr);
  pending_segments.emplace_back(active._segment, size_before);
  active._flush = this;
  active._pending_segment_offset = size_before;
  pending.Add();
}

void IndexWriter::FlushContext::Reset() noexcept {
  // reset before returning to pool
  for (auto& segment : segments) {
    // use_count here isn't used for synchronization
    if (segment.use_count() == 1) {
      segment->Reset();
    }
  }

  imports.clear();
  cached.clear();
  segments.clear();
  segment_mask.clear();

  for (auto& entry : pending_segments) {
    if (auto& segment = entry.segment; segment != nullptr) {
      segment->Reset();
    }
  }
  ClearPending();
  dir->clear_refs();
}

void IndexWriter::Cleanup(FlushContext& curr, FlushContext* next) noexcept {
  for (auto& import : curr.imports) {
    auto& candidates = import.consolidation_ctx.candidates;
    for (const auto* candidate : candidates) {
      _consolidating.segments.erase(candidate->Meta().name);
    }
  }
  for (const auto& entry : curr.cached) {
    _consolidating.segments.erase(entry.second->Meta().name);
  }
  if (next != nullptr) {
    for (const auto& entry : next->cached) {
      _consolidating.segments.erase(entry.second->Meta().name);
    }
  }
}

uint64_t IndexWriter::FlushContext::FlushPending(uint64_t committed_tick,
                                                 uint64_t tick,
                                                 AsyncFlush& async_flush) {
  // if tick is not equal uint64_max, as result of bad_alloc it's possible here
  // that not all segments which should be committed by next FlushContext
  // (fully or partially) will be moved to it.
  // I consider it's ok, because in such situation you rely on tick,
  // but you cannot assume anything about your IndexWriter::Transaction between
  // last successfully committed tick and state before you understand that
  // IndexWriter::Commit(tick) is failed in multi-threaded environment.
  // Some Transactions after tick is initially in current FlushContext.
  // Some Transactions after tick is initially in next FlushContext.
  // From outside view you cannot distinct them!
  // So even if I will make this moving deterministic it's not helpful at all.
  // Also on practice such situation is almost impossible.
  // Probably in future we can implement some out of sync logic for IndexWriter.
  // But now it's unnecessary for our usage.

  SDB_ASSERT(next != nullptr);
  auto& next_segments = next->segments;
  SDB_ASSERT(next_segments.empty());
  size_t to_next_pending_segments = 0;
  uint64_t flushed_tick = committed_tick;
  for (auto& entry : pending_segments) {
    auto& segment = entry.segment;
    SDB_ASSERT(segment != nullptr);
    const auto first_tick = segment->first_tick;
    const auto last_tick = segment->last_tick;
    if (first_tick <= tick) {
      // This assert is really paranoid, it's not required just try to detect
      // situation when we commit on tick but forgot to call RegisterFlush().
      // This assert can work only if any transaction which committed after last
      // Commit will has greater first tick than committed tick.
      SDB_ASSERT(committed_tick < first_tick);
      flushed_tick = std::max(flushed_tick, last_tick);
      segment->StartFlush(async_flush);
      // segment->Flush();
      if (tick < last_tick) {
        next_segments.push_back(segment);
      }
      segments.push_back(std::move(segment));
    } else {
      ++to_next_pending_segments;
    }
  }

  if (to_next_pending_segments != 0) {
    std::lock_guard lock{next->pending.Mutex()};
    for (auto& entry : pending_segments) {
      if (auto& segment = entry.segment; segment != nullptr) {
        SDB_ASSERT(tick < segment->first_tick);
        auto& node = next->pending_segments.emplace_back(
          std::move(segment), next->pending_segments.size());
        next->pending_freelist.push(node);
      }
    }
  }

#ifdef SDB_DEV
  for (auto& entry : pending_segments) {
    SDB_ASSERT(entry.segment == nullptr);
  }
#endif
  ClearPending();
  return flushed_tick;
}

IndexWriter::SegmentContext::SegmentContext(
  Directory& dir, segment_meta_generator_t&& meta_generator,
  const SegmentWriterOptions& options)
  : active(dir, options),
    options(options),
    queries{{options.resource_manager}},
    flushed{{options.resource_manager}},
    flushed_docs{{options.resource_manager}},
    meta_generator{std::move(meta_generator)} {
  SDB_ASSERT(this->meta_generator);
}

IndexWriter::SegmentContext::FlushOutput
IndexWriter::SegmentContext::RunPhysicalFlush(SealedGeneration& sealed) {
  DocsMask docs_mask{.set{flushed_docs.get_allocator()}};
  auto old2new = sealed.writer->flush(sealed.writer_meta, docs_mask);
  return {std::move(old2new), std::move(docs_mask)};
}

void IndexWriter::SegmentContext::HarvestFlushOutput(
  SealedGeneration&& sealed, FlushOutput&& output,
  std::span<SegmentWriter::DocContext> docs_context) {
  if (sealed.writer_meta.meta.live_docs_count == 0) {
    return;
  }

  SDB_ASSERT(sealed.writer_meta.meta.live_docs_count <=
             sealed.writer_meta.meta.docs_count);
  SDB_ASSERT(sealed.writer_meta.meta.docs_count == docs_context.size());

  auto refs = sealed.dir->GetRefs();
  flushed.emplace_back(std::move(sealed.writer_meta), std::move(output.old2new),
                       std::move(output.docs_mask), flushed_docs.size(),
                       std::move(refs));
  try {
    flushed_docs.insert(flushed_docs.end(), docs_context.begin(),
                        docs_context.end());
  } catch (...) {
    flushed.pop_back();
    throw;
  }
  flushed_queries = queries.size();
  committed_flushed_docs += sealed.committed_buffered_docs;
}

void IndexWriter::SegmentContext::StartFlush(
  IndexWriter::AsyncFlush& async_flush) {
  if (pending_flush.has_value()) {
    HarvestPendingFlush();
  }

  if (!active.writer->initialized() || active.writer->buffered_docs() == 0) {
    flushed_queries = queries.size();
    SDB_ASSERT(active.committed_buffered_docs == 0);
    return;
  }
  SDB_ASSERT(active.writer->buffered_docs() <= doc_limits::eof());
  SDB_ASSERT(!pending_flush.has_value());

  auto [f, p] = yaclib::MakeContract<FlushOutput>();
  pending_flush.emplace(TakeActiveGeneration(), std::move(f));

  auto run_flush = [this, &sealed = pending_flush->sealed,
                    promise = std::move(p)] mutable noexcept {
    try {
      auto res = RunPhysicalFlush(sealed);
      std::move(promise).Set(std::move(res));
    } catch (...) {
      std::move(promise).Set(std::current_exception());
    }
  };

  async_flush.Run(std::move(run_flush), [&] {
    return pending_flush->sealed.committed_buffered_docs == 0;
  });
}

void IndexWriter::SegmentContext::HarvestPendingFlush() {
  SDB_ASSERT(pending_flush.has_value());
  auto pending = std::exchange(pending_flush, std::nullopt);

  Finally reset_writer = [&]() noexcept {
    pending->sealed.writer->reset();
    pending->sealed.dir->clear_refs();
  };

  yaclib::Wait(pending->output);
  auto output = std::move(pending->output).Get().Ok();

  const auto docs_context = pending->sealed.writer->docs_context();
  HarvestFlushOutput(std::move(pending->sealed), std::move(output),
                     docs_context);
}

void IndexWriter::SegmentContext::DiscardPendingFlush() noexcept {
  if (!pending_flush.has_value()) {
    return;
  }
  try {
    HarvestPendingFlush();
  } catch (...) {
    // Best effort in noexcept path: ignore flush errors and continue rollback.
  }
}

void IndexWriter::SegmentContext::DrainPendingFlush() {
  if (pending_flush.has_value()) {
    HarvestPendingFlush();
  }
}

IndexWriter::SegmentContext::ptr IndexWriter::SegmentContext::make(
  Directory& dir, segment_meta_generator_t&& meta_generator,
  const SegmentWriterOptions& segment_writer_options) {
  return std::make_unique<SegmentContext>(dir, std::move(meta_generator),
                                          segment_writer_options);
}

void IndexWriter::SegmentContext::Prepare() {
  SDB_ASSERT(!active.writer->initialized());

  active.writer_meta.filename.clear();
  active.writer_meta.meta = meta_generator();
  active.writer->reset(active.writer_meta.meta);
}

void IndexWriter::SegmentContext::Reset(bool store_flushed) noexcept {
  SDB_ASSERT(!pending_flush.has_value());

  buffered_docs.store(0, std::memory_order_relaxed);

  if (store_flushed) [[unlikely]] {
    queries.resize(flushed_queries);
    committed_queries = std::min(committed_queries, flushed_queries);
  } else {
    queries.clear();
    flushed_queries = 0;
    committed_queries = 0;

    flushed.clear();
    flushed_docs.clear();
    committed_flushed_docs = 0;

    // TODO(mbkkt) What about ticks in case of store_flushed?
    //  Of course it's valid but maybe we can decrease range?
    first_tick = writer_limits::kMaxTick;
    last_tick = writer_limits::kMinTick;
    has_replace = false;
  }
  active.committed_buffered_docs = 0;

  if (active.writer->initialized()) {
    active.writer->reset();  // try to reduce number of files flushed below
  }

  // TODO(mbkkt) Is it ok to release refs in case of store_flushed?
  // release refs only after clearing writer state to ensure
  // 'writer_' does not hold any files
  active.dir->clear_refs();
}

void IndexWriter::SegmentContext::Rollback() noexcept {
  SDB_ASSERT(!pending_flush.has_value());

  // rollback modification queries
  SDB_ASSERT(committed_queries <= queries.size());
  queries.resize(committed_queries);

  // truncate uncommitted flushed tail
  const auto end = flushed.end();
  // TODO(mbkkt) reverse find order
  auto it = std::find_if(flushed.begin(), end, [&](const auto& flushed) {
    return committed_flushed_docs < flushed.GetDocsEnd();
  });
  if (it != end) {
    // because committed docs point to flushed
    SDB_ASSERT(active.committed_buffered_docs == 0);
    const auto docs_end = it->GetDocsEnd();
    if (it->SetCommitted(committed_flushed_docs)) {
      flushed.erase(it + 1, end);
      SDB_ASSERT(committed_flushed_docs < docs_end);
      flushed_docs.resize(docs_end);
      committed_flushed_docs = docs_end;
    } else {
      flushed.erase(it, end);
      flushed_docs.resize(committed_flushed_docs);
    }
  }

  // rollback inserts located inside the writer
  if (active.committed_buffered_docs == 0) {
    active.writer->reset();
    return;
  }
  const auto buffered_docs = active.writer->buffered_docs();
  for (auto
         doc_id = buffered_docs - 1 + doc_limits::min(),
         doc_id_rend = active.committed_buffered_docs - 1 + doc_limits::min();
       doc_id > doc_id_rend; --doc_id) {
    SDB_ASSERT(doc_limits::invalid() < doc_id);
    SDB_ASSERT(doc_id <= doc_limits::eof());
    active.writer->remove(static_cast<doc_id_t>(doc_id));
  }

  // If it will be first or last part of flushed segment in future,
  // we need valid ticks for this documents
  // TODO(mbkkt) Maybe we can just move docs_begin/end when FlushedSegment
  //  will be ready? Also what about assign last_committed_tick
  //  only for first and last value in range?
  const auto docs = active.writer->docs_context();
  const auto last_committed_tick =
    docs[active.committed_buffered_docs - 1].tick;
  std::for_each(docs.begin() + active.committed_buffered_docs, docs.end(),
                [&](auto& doc) {
                  doc.tick = last_committed_tick;
                  doc.query_id = writer_limits::kInvalidOffset;
                });

  active.committed_buffered_docs = buffered_docs;
}

void IndexWriter::SegmentContext::Commit(uint64_t commit_queries,
                                         uint64_t commit_last_tick) {
  DrainPendingFlush();

  SDB_ASSERT(commit_last_tick < writer_limits::kMaxTick);
  SDB_ASSERT(commit_queries <= commit_last_tick);
  const auto commit_first_tick = commit_last_tick - commit_queries;
  SDB_ASSERT(writer_limits::kMinTick < commit_first_tick);

  auto update_tick = [&](auto& entry) noexcept {
    entry.tick += commit_first_tick;
  };

  std::for_each(queries.begin() + committed_queries, queries.end(),
                update_tick);
  committed_queries = queries.size();

  std::for_each(flushed_docs.begin() + committed_flushed_docs,
                flushed_docs.end(), update_tick);
  committed_flushed_docs = flushed_docs.size();

  const auto docs = active.writer->docs_context();
  std::for_each(docs.begin() + active.committed_buffered_docs, docs.end(),
                update_tick);
  active.committed_buffered_docs = docs.size();

  if (first_tick == writer_limits::kMaxTick) {
    first_tick = commit_first_tick;
  }
  SDB_ASSERT(last_tick <= commit_last_tick);
  last_tick = commit_last_tick;
}

IndexWriter::IndexWriter(
  ConstructToken, IndexLock::ptr&& lock, IndexFileRefs::ref_t&& lock_file_ref,
  Directory& dir, Format::ptr codec, size_t segment_pool_size,
  const SegmentOptions& segment_limits, const Comparer* comparator,
  const ColumnInfoProvider& column_info,
  const FeatureInfoProvider& feature_info,
  const PayloadProvider& meta_payload_provider,
  std::shared_ptr<const DirectoryReaderImpl>&& committed_reader,
  yaclib::IExecutorPtr executor, size_t remaining_flushes_slots)
  : _feature_info{feature_info},
    _column_info{column_info},
    _meta_payload_provider{meta_payload_provider},
    _comparator{comparator},
    _codec{std::move(codec)},
    _dir{dir},
    _committed_reader{std::move(committed_reader)},
    _segment_limits{segment_limits},
    _segment_writer_pool{segment_pool_size},
    _seg_counter{_committed_reader->Meta().index_meta.seg_counter},
    _last_gen{_committed_reader->Meta().index_meta.gen},
    _writer{_codec->get_index_meta_writer()},
    _write_lock{std::move(lock)},
    _write_lock_file_ref{std::move(lock_file_ref)},
    _async_flush{
      .executor{std::move(executor)},
      .flush_semaphore{static_cast<ptrdiff_t>(remaining_flushes_slots)}} {
  SDB_ASSERT(column_info);   // ensured by 'make'
  SDB_ASSERT(feature_info);  // ensured by 'make'
  SDB_ASSERT(_codec);

  _wand_scorer = _committed_reader->Options().scorer;
  if (_wand_scorer) {
    _wand_features |= _wand_scorer->GetIndexFeatures();
  }

  _committed_delete_state_shadow.resize(_committed_reader->size());
  SDB_ASSERT(_committed_delete_state_shadow.size() ==
             _committed_reader->size());

  _flush_context.store(_flush_contexts.data());

  // setup round-robin chain
  auto* ctx = _flush_contexts.data();
  for (auto* last = ctx + _flush_contexts.size() - 1; ctx != last; ++ctx) {
    ctx->dir = std::make_unique<RefTrackingDirectory>(dir);
    ctx->next = ctx + 1;
  }
  ctx->dir = std::make_unique<RefTrackingDirectory>(dir);
  ctx->next = _flush_contexts.data();
}

void IndexWriter::InitMeta(IndexMeta& meta, uint64_t tick) const {
  if (_meta_payload_provider) {
    SDB_ASSERT(!meta.payload.has_value());
    auto& payload = meta.payload.emplace(bstring{});
    if (!_meta_payload_provider(tick, payload)) [[unlikely]] {
      meta.payload.reset();
    }
  }
  meta.seg_counter = CurrentSegmentId();  // Ensure counter() >= max(seg#)
  meta.gen = _last_gen;                   // Clone index metadata generation
}

void IndexWriter::Clear(uint64_t tick) {
  _commit_lock.ForgetDeadlockInfo();
  std::lock_guard commit_lock{_commit_lock};

  auto ctx = SwitchFlushContext();
  // Ensure there are no active struct update operations
  ctx->pending.Wait();
  // TODO(mbkkt) Move some pending to the next flush ctx?
  //  It's not super trivial for spliting segments
  //  It's not important now, because truncate running in exclusive mode

  SDB_ASSERT(_committed_reader);
  if (const auto& committed_meta = _committed_reader->Meta();
      !_pending_state.Valid() && committed_meta.index_meta.segments.empty() &&
      !IsInitialCommit(committed_meta)) {
    return;  // Already empty
  }

  PendingContext to_commit{PendingBase{.tick = tick}, {}, {}, {}, {}};
  InitMeta(to_commit.meta, tick);
  to_commit.ctx = std::move(ctx);

  Abort();  // iff Clear called between Begin and Commit
  ApplyFlush(std::move(to_commit));
  Finish();

  // Clear consolidating segments
  std::lock_guard lock{_consolidating.lock};
  _consolidating.segments.clear();
}

IndexWriter::ptr IndexWriter::Make(Directory& dir, Format::ptr codec,
                                   OpenMode mode,
                                   const IndexWriterOptions& options) {
  IndexLock::ptr lock;
  IndexFileRefs::ref_t lock_ref;

  if (options.lock_repository) {
    // lock the directory
    lock = dir.make_lock(kWriteLockName);
    // will be created by try_lock
    lock_ref = dir.attributes().refs().add(kWriteLockName);

    if (!lock || !lock->try_lock()) {
      throw LockObtainFailed(kWriteLockName);
    }
  }

  // read from directory or create index metadata
  DirectoryMeta meta;

  {
    auto reader = codec->get_index_meta_reader();
    const bool index_exists = reader->last_segments_file(dir, meta.filename);

    if (kOmCreate == mode ||
        ((kOmCreate | kOmAppend) == mode && !index_exists)) {
      // for OM_CREATE meta must be fully recreated, meta read only to get
      // last version
      if (index_exists) {
        // Try to read. It allows us to create writer against an index that's
        // currently opened for searching
        reader->read(dir, meta.index_meta, meta.filename);

        meta.filename.clear();  // Empty index meta -> new index
        auto& index_meta = meta.index_meta;
        index_meta.payload.reset();
        index_meta.segments.clear();
      }
    } else if (!index_exists) {
      throw FileNotFound{meta.filename};  // no segments file found
    } else {
      reader->read(dir, meta.index_meta, meta.filename);
    }
  }

  auto reader = [](Directory& dir, Format::ptr codec, DirectoryMeta&& meta,
                   const IndexReaderOptions& opts) {
    const auto& segments = meta.index_meta.segments;

    std::vector<SegmentReader> readers;
    readers.reserve(segments.size());

    for (auto& segment : segments) {
      // Segment reader holds refs to all segment files
      readers.emplace_back(SegmentReaderImpl::Open(dir, segment.meta, opts));
      SDB_ASSERT(readers.back());
    }

    return std::make_shared<const DirectoryReaderImpl>(
      dir, std::move(codec), opts, std::move(meta), std::move(readers));
  }(dir, codec, std::move(meta), options.reader_options);

  auto writer = std::make_shared<IndexWriter>(
    ConstructToken{}, std::move(lock), std::move(lock_ref), dir,
    std::move(codec), options.segment_pool_size, SegmentOptions{options},
    options.comparator,
    options.column_info ? options.column_info : kDefaultColumnInfo,
    options.features ? options.features : kDefaultFeatureInfo,
    options.meta_payload_provider, std::move(reader), options.executor,
    options.max_in_flight_flushes);

  // Remove non-index files from directory
  directory_utils::RemoveAllUnreferenced(dir);

  return writer;
}

IndexWriter::~IndexWriter() noexcept {
  // Failure may indicate a dangling 'document' instance
  SDB_ASSERT(!_segments_active.load());
  _write_lock.reset();  // Reset write lock if any
  // Reset pending state (if any) before destroying flush contexts
  _pending_state.Reset(*this);
}

uint64_t IndexWriter::BufferedDocs() const {
  uint64_t docs_in_ram = 0;
  auto ctx = GetFlushContext();
  // 'pending_used_segment_contexts_'/'pending_free_segment_contexts_'
  // may be modified
  std::lock_guard lock{ctx->pending.Mutex()};

  for (const auto& entry : ctx->pending_segments) {
    SDB_ASSERT(entry.segment != nullptr);
    // reading SegmentWriter::docs_count() is not thread safe
    docs_in_ram += entry.segment->buffered_docs.load(std::memory_order_relaxed);
  }

  return docs_in_ram;
}

uint64_t IndexWriter::NextSegmentId() noexcept {
  return _seg_counter.fetch_add(1, std::memory_order_relaxed) + 1;
}

uint64_t IndexWriter::CurrentSegmentId() const noexcept {
  return _seg_counter.load(std::memory_order_relaxed);
}

ConsolidationResult IndexWriter::Consolidate(
  const ConsolidationPolicy& policy, Format::ptr codec,
  const MergeWriter::FlushProgress& progress) {
  if (!codec) {
    // use default codec if not specified
    codec = _codec;
  }

  Consolidation candidates;
  const auto run_id = reinterpret_cast<uintptr_t>(&candidates);

  decltype(_committed_reader) committed_reader;
  // collect a list of consolidation candidates
  {
    std::lock_guard lock{_consolidating.lock};
    // hold a reference to the last committed state to prevent files from being
    // deleted by a cleaner during the upcoming consolidation
    // use atomic_load(...) since Finish() may modify the pointer
    committed_reader = GetSnapshotImpl();

    if (committed_reader->size() == 0) {
      // nothing to consolidate
      return {0, ConsolidationError::Ok};
    }

    // FIXME TODO remove from 'consolidating_segments_' any segments in
    // 'committed_state_' or 'pending_state_' to avoid data duplication
    policy(candidates, *committed_reader, _consolidating.segments);

    switch (candidates.size()) {
      case 0:  // nothing to consolidate
        return {0, ConsolidationError::Ok};
      case 1: {
        const auto* candidate = candidates.front();
        SDB_ASSERT(candidate != nullptr);
        if (!HasRemovals(candidate->Meta())) {
          // no removals, nothing to consolidate
          return {0, ConsolidationError::Ok};
        }
      }
    }

    for (const auto* candidate : candidates) {
      SDB_ASSERT(candidate != nullptr);
      // TODO(mbkkt) Make this check assert in future
      if (_consolidating.segments.contains(candidate->Meta().name)) {
        return {0, ConsolidationError::Fail};
      }
    }

    // register for consolidation
    _consolidating.segments.reserve(_consolidating.segments.size() +
                                    candidates.size());
    for (const auto* candidate : candidates) {
      _consolidating.segments.emplace(candidate->Meta().name);
    }
  }

  // unregisterer for all registered candidates
  Finally unregister_segments = [&candidates, this]() noexcept {
    if (candidates.empty()) {
      return;
    }
    std::lock_guard lock{_consolidating.lock};
    for (const auto* candidate : candidates) {
      _consolidating.segments.erase(candidate->Meta().name);
    }
  };

  // validate candidates: no duplicates and all should be from committed reader
#ifdef SDB_DEV
  absl::c_sort(candidates);
  SDB_ASSERT(std::unique(candidates.begin(), candidates.end()) ==
             candidates.end());
  {
    size_t found = 0;
    for (const auto& segment : *committed_reader) {
      found += static_cast<size_t>(absl::c_binary_search(candidates, &segment));
    }
    SDB_ASSERT(found == candidates.size());
  }
#endif

  SDB_TRACE("xxxxx", sdb::Logger::IRESEARCH, "Starting consolidation id='",
            run_id, "':\n", ToString(candidates));

  // do lock-free merge

  ConsolidationResult result{candidates.size(), ConsolidationError::Fail};

  IndexSegment consolidation_segment;
  consolidation_segment.meta.codec = codec;  // Should use new codec
  consolidation_segment.meta.version = 0;    // Reset version for new segment
  // Increment active meta
  consolidation_segment.meta.name = FileName(NextSegmentId());

  RefTrackingDirectory dir{_dir};  // Track references for new segment

  MergeWriter merger{dir, GetSegmentWriterOptions(true)};
  merger.Reset(candidates.begin(), candidates.end());

  // We do not persist segment meta since some removals may come later
  if (!merger.Flush(consolidation_segment.meta, progress)) {
    // Nothing to consolidate or consolidation failure
    return result;
  }

  auto pending_reader = SegmentReaderImpl::Open(
    _dir, consolidation_segment.meta, committed_reader->Options());
  SDB_ASSERT(pending_reader);

  // Commit merge, ensure no concurrent Commit/etc
  _commit_lock.ForgetDeadlockInfo();
  std::shared_lock lock{_commit_lock};
  const auto current_committed_reader = _committed_reader;
  SDB_ASSERT(current_committed_reader != nullptr);
  const bool pending = _pending_state.Valid();
  auto ctx = GetFlushContext();
  lock.unlock();
  // Guard against concurrent Commit/etc
  if (pending) {
    // after some transaction was started:
    if (committed_reader != current_committed_reader) {
      // If some segment already not in current reader
      // and we're waiting commit -- abort
      auto begin = current_committed_reader->begin();
      auto end = current_committed_reader->end();
      for (const auto* candidate : candidates) {
        std::string_view id{candidate->Meta().name};
        if (end == std::find_if(begin, end, [id](const SubReader& s) {
              // FIXME(gnusi): compare pointers?
              return id == s.Meta().name;
            })) {
          SDB_DEBUG("xxxxx", sdb::Logger::IRESEARCH,
                    "Failed to start consolidation for index generation '",
                    committed_reader->Meta().index_meta.gen,
                    "', not found segment ", candidate->Meta().name,
                    " in committed state");
          return result;
        }
      }
    }
    auto refs = dir.GetRefs();
    // Prevent concurrent imports modification
    std::lock_guard ctx_lock{ctx->pending.Mutex()};
    // register consolidation for the next transaction
    ctx->imports.emplace_back(
      std::move(consolidation_segment), writer_limits::kMaxTick,
      // skip removals, will accumulate removals from existing candidates
      std::move(refs),              // do not forget to track refs
      std::move(candidates),        // consolidation context candidates
      std::move(pending_reader),    // consolidated reader
      std::move(committed_reader),  // consolidation context meta
      std::move(merger));
    SDB_TRACE("xxxxx", sdb::Logger::IRESEARCH, "Consolidation id='", run_id,
              "' successfully finished: pending");
    result.error = ConsolidationError::Pending;
    return result;
  }

  // before new transaction was started:
  CandidatesMapping mappings;
  if (committed_reader != current_committed_reader) {
    // there was a Commit(s) since consolidation was started
    const auto [count, has_removals] =
      MapCandidates(mappings, candidates, *current_committed_reader);
    if (count != candidates.size()) {
      // at least one candidate is missing can't finish consolidation
      SDB_DEBUG("xxxxx", sdb::Logger::IRESEARCH,
                "Failed to finish consolidation id='", run_id,
                "' for segment '", consolidation_segment.meta.name,
                "', found only '", count, "' out of '", candidates.size(),
                "' candidates");
      return result;
    }

    // handle removals if something changed
    if (has_removals) {
      auto docs_mask =
        std::make_shared<DocumentMask>(*dir.ResourceManager().readers);
      if (!MapRemovals(mappings, merger, *docs_mask)) {
        // consolidated segment has docs missing from
        // current_committed_meta->segments()
        SDB_DEBUG("xxxxx", sdb::Logger::IRESEARCH,
                  "Failed to finish consolidation id='", run_id,
                  "' for segment '", consolidation_segment.meta.name,
                  "', due removed documents still present "
                  "the consolidation candidates");

        return result;
      }

      SDB_ASSERT(!docs_mask->empty());
      consolidation_segment.meta.docs_mask = std::move(docs_mask);
    }
  }

  index_utils::FlushIndexSegment(dir, consolidation_segment, false);

  if (consolidation_segment.meta.docs_mask) {
    pending_reader =
      pending_reader->UpdateMeta(dir, consolidation_segment.meta);
  }

  auto refs = dir.GetRefs();
  std::lock_guard ctx_lock{ctx->pending.Mutex()};
  auto& segment_mask = ctx->segment_mask;
  segment_mask.reserve(segment_mask.size() + mappings.size() +
                       candidates.size());
  const auto& pending_segment = ctx->imports.emplace_back(
    std::move(consolidation_segment), writer_limits::kMinTick,
    // removals must be applied to the consolidated segment
    std::move(refs),              // do not forget to track refs
    std::move(candidates),        // consolidation context candidates
    std::move(pending_reader),    // consolidated reader
    std::move(committed_reader),  // consolidation context meta
    *dir.ResourceManager().consolidations);
  // noexcept part: mask consolidated segments
  for (const auto* candidate : pending_segment.consolidation_ctx.candidates) {
    segment_mask.emplace(candidate->Meta().name);
  }
  if (!mappings.empty()) {
    for (const auto& segment : *current_committed_reader) {
      if (mappings.contains(segment.Meta().name)) {
        segment_mask.emplace(segment.Meta().name);
      }
    }
  }
  SDB_TRACE("xxxxx", sdb::Logger::IRESEARCH, "Consolidation id='", run_id,
            "' successfully finished: Name='",
            pending_segment.segment.meta.name,
            "', docs_count=", pending_segment.segment.meta.docs_count,
            ", live_docs_count=", pending_segment.segment.meta.live_docs_count,
            ", size=", pending_segment.segment.meta.byte_size);
  result.error = ConsolidationError::Ok;
  return result;
}

bool IndexWriter::Import(const IndexReader& reader,
                         Format::ptr codec /*= nullptr*/,
                         const MergeWriter::FlushProgress& progress /*= {}*/) {
  if (!reader.live_docs_count()) {
    return true;  // Skip empty readers since no documents to import
  }

  if (!codec) {
    codec = _codec;
  }

  const auto options = GetSnapshotImpl()->Options();

  RefTrackingDirectory dir{_dir};  // Track references

  IndexSegment segment;
  segment.meta.name = FileName(NextSegmentId());
  segment.meta.codec = codec;

  MergeWriter merger{dir, GetSegmentWriterOptions(true)};
  merger.Reset(reader.begin(), reader.end());

  if (!merger.Flush(segment.meta, progress)) {
    return false;  // Import failure (no files created, nothing to clean up)
  }

  index_utils::FlushIndexSegment(dir, segment);

  auto imported_reader = SegmentReaderImpl::Open(_dir, segment.meta, options);
  SDB_ASSERT(imported_reader);

  auto refs = dir.GetRefs();
  auto flush = GetFlushContext();

  // lock due to context modification
  std::lock_guard lock{flush->pending.Mutex()};

  // IMPORTANT NOTE!
  // Will be committed in the upcoming Commit
  // even if tick is greater than Commit tick
  // TODO(mbkkt) Can be fixed: needs to add overload with external tick and
  // moving not suited import segments to the next FlushContext in PrepareFlush
  flush->imports.emplace_back(
    std::move(segment), _tick.load(std::memory_order_relaxed), std::move(refs),
    std::move(imported_reader), *dir.ResourceManager().consolidations);

  return true;
}

IndexWriter::FlushContextPtr IndexWriter::GetFlushContext() const noexcept
  ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto* ctx = _flush_context.load(std::memory_order_relaxed);
  for (;;) {
    const bool lock = ctx->context_mutex.try_lock_shared();
    auto* curr = _flush_context.load(std::memory_order_relaxed);
    if (lock) {
      if (ctx == curr) [[likely]] {
        return {ctx, [](FlushContext* ctx)
                       ABSL_UNLOCK_FUNCTION(ctx->context_mutex) noexcept {
                         ctx->context_mutex.unlock_shared();
                       }};
      }
      ctx->context_mutex.unlock_shared();
    }
    ctx = curr;
  }
}

IndexWriter::FlushContextPtr IndexWriter::SwitchFlushContext() noexcept
  ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto* ctx = _flush_context.load(std::memory_order_relaxed);
  for (;;) {
    ctx->context_mutex.lock();
    auto* curr = _flush_context.load(std::memory_order_relaxed);
    if (ctx == curr) [[likely]] {
      _flush_context.store(ctx->next, std::memory_order_relaxed);
      return {ctx, [](FlushContext* ctx)
                     ABSL_UNLOCK_FUNCTION(ctx->context_mutex) noexcept {
                       ctx->Reset();  // reset context and make ready for reuse
                       ctx->context_mutex.unlock();
                     }};
    }
    ctx->context_mutex.unlock();
    ctx = curr;
  }
}

IndexWriter::ActiveSegmentContext IndexWriter::GetSegmentContext() try {
  // TODO(mbkkt) rewrite this when will be written parallel Commit
  //  Few ideas about rewriting:
  //  1. We should use all available memory
  //  2. Flush should be async, waiting Flush only if we don't have other choice
  //  3. segment_memory/count_max should be removed in their current state
  // increment counter to acquire reservation,
  // if another thread tries to reserve last context then it'll be over limit
  const auto segments_active =
    _segments_active.fetch_add(1, std::memory_order_relaxed) + 1;

  // no free segment_context available and maximum number of segments reached
  // must return to caller so as to unlock/relock flush_context before retrying
  // to get a new segment so as to avoid a deadlock due to a read-write-read
  // situation for FlushContext::context_mutex_ with threads trying to lock
  // FlushContext::context_mutex_ to return their segment_context
  if (_segment_limits.Count() < segments_active) {
    _segments_active.fetch_sub(1, std::memory_order_relaxed);
    return {};
  }

  {
    auto flush = GetFlushContext();
    auto* freelist_node = flush->pending_freelist.pop();
    if (freelist_node != nullptr) {
      flush->pending.Add();
      return {static_cast<PendingSegmentContext*>(freelist_node)->segment,
              _segments_active, flush.get(), freelist_node->value};
    }
  }

  const auto options = GetSegmentWriterOptions(false);

  // should allocate a new segment_context from the pool
  std::shared_ptr<SegmentContext> segment_ctx = _segment_writer_pool.emplace(
    _dir,
    [this] {
      SegmentMeta meta{.codec = _codec};
      meta.name = FileName(NextSegmentId());
      return meta;
    },
    options);

  // recreate writer if it reserved more memory than allowed by current limits
  if (_segment_limits.Memory() <
      segment_ctx->active.writer->memory_reserved()) {
    segment_ctx->active.writer =
      SegmentWriter::make(*segment_ctx->active.dir, options);
  }

  return {segment_ctx, _segments_active};
} catch (...) {
  _segments_active.fetch_sub(1, std::memory_order_relaxed);
  throw;
}

SegmentWriterOptions IndexWriter::GetSegmentWriterOptions(
  bool consolidation) const noexcept {
  return {
    .column_info = _column_info,
    .feature_info = _feature_info,
    .scorers_features = _wand_features,
    .scorer = _wand_scorer,
    .comparator = _comparator,
    .resource_manager = consolidation ? *_dir.ResourceManager().consolidations
                                      : *_dir.ResourceManager().transactions,
  };
}

void IndexWriter::ResolveDeleteStateShadow(
  std::span<const DeletePacket> packets,
  std::span<SegmentDeleteStateShadow> states,
  uint64_t barrier_delete_packet_id) {
#ifdef SDB_DEV
  uint64_t prev_id = 0;
  for (const auto& packet : packets) {
    SDB_ASSERT(prev_id < packet.id);
    SDB_ASSERT(packet.id <= barrier_delete_packet_id);
    prev_id = packet.id;
  }
#endif
  for (auto& applied_delete_packet_id : states) {
    SDB_ASSERT(applied_delete_packet_id <= barrier_delete_packet_id);
    applied_delete_packet_id = barrier_delete_packet_id;
  }
}

IndexWriter::DeleteBarrierPlan IndexWriter::BuildDeleteBarrierPlan(
  uint64_t commit_tick) {
  auto packets = _delete_packets.Snapshot();

  uint64_t floor = 0;
  if (!_committed_delete_state_shadow.empty()) {
    const auto it = std::min_element(_committed_delete_state_shadow.begin(),
                                     _committed_delete_state_shadow.end());
    floor = *it;
  }

  DeleteBarrierPlan plan{.committed_floor_id = floor, .barrier_id = floor};

  for (const auto& p : packets) {
    if (p.id <= floor) {
      continue;
    }
    if (p.tick > commit_tick) {
      continue;
    }
    plan.barrier_id = p.id;
    plan.commit_packets.push_back(p);
  }

#ifdef SDB_DEV
  SDB_ASSERT(plan.barrier_id >= plan.committed_floor_id);
  if (plan.commit_packets.empty()) {
    SDB_ASSERT(plan.barrier_id == plan.committed_floor_id);
  }
#endif

  return plan;
}

void IndexWriter::AdvanceCommittedDeleteFloor(
  uint64_t barrier_id, std::span<SegmentDeleteStateShadow> states) {
  for (auto& applied_delete_packet_id : states) {
    applied_delete_packet_id = std::max(applied_delete_packet_id, barrier_id);
  }
  _delete_packets.PruneUpTo(barrier_id);
}

IndexWriter::PendingContext IndexWriter::PrepareFlush(const CommitInfo& info) {
  SDB_ASSERT(writer_limits::kMinTick < info.tick);
  SDB_ASSERT(_committed_tick <= info.tick);
  SDB_ASSERT(info.tick <= writer_limits::kMaxTick);

  // noexcept block: I'm not sure is it really necessary or not
  PendingContext to_commit;
  to_commit.ctx = SwitchFlushContext();
  to_commit.tick = info.tick;
  auto& ctx = to_commit.ctx;
  const auto& tick = to_commit.tick;

  // ensure there are no active struct update operations
  ctx->pending.Wait();
  // Stage 0
  // wait for any outstanding segments to settle to ensure that any rollbacks
  // are properly tracked in 'modification_queries_'
  const auto flushed_tick =
    ctx->FlushPending(_committed_tick, tick, _async_flush);

  std::unique_lock cleanup_lock{_consolidating.lock, std::defer_lock};
  Finally cleanup = [&]() noexcept {
    if (ctx == nullptr) {
      return;
    }
    if (!cleanup_lock) {
      cleanup_lock.lock();
    }
    Cleanup(*ctx);
  };

  const auto& progress =
    (info.progress != nullptr ? info.progress : kNoProgress);

  IndexMeta& pending_meta = to_commit.meta;
  std::vector<PartialSync> partial_sync;
  std::vector<SegmentReader>& readers = to_commit.readers;
  auto& delete_state_shadow = to_commit.delete_state_shadow;

  auto& dir = *ctx->dir;
  const auto& committed_reader = *_committed_reader;
  const auto& committed_meta = committed_reader.Meta();
  auto reader_options = committed_reader.Options();

  // If there is no index we shall initialize it
  bool modified = IsInitialCommit(committed_meta);
  auto plan = BuildDeleteBarrierPlan(info.tick);
  to_commit.delete_barrier_id = plan.barrier_id;

  absl::flat_hash_set<uint64_t> matched_replace_ticks;

  // Stage 1
  // update document_mask for existing (i.e. sealed) segments
  auto& segment_mask = ctx->segment_mask;
  segment_mask.reserve(segment_mask.size() + ctx->cached.size());
  for (const auto& entry : ctx->cached) {
    segment_mask.emplace(entry.second->Meta().name);
  }

  size_t current_segment_index = 0;
  const size_t committed_reader_size = committed_reader.size();

  readers.reserve(committed_reader_size);
  pending_meta.segments.reserve(committed_reader_size);

  struct Out {
    SegmentReader segment_reader;
    IndexSegment index_segment;
    bool add_to_part_sync{false};
    std::optional<uint64_t> applied_delete_packet_id{std::nullopt};
  };

  auto process_futures = [&](std::span<yaclib::Future<Out>> futures) {
    yaclib::Wait(futures.begin(), futures.end());
    for (auto& future : futures) {
      auto output = std::move(future).Get().Ok();

      if (output.add_to_part_sync) {
        partial_sync.emplace_back(readers.size());
      }
      readers.emplace_back(std::move(output.segment_reader));
      pending_meta.segments.emplace_back(std::move(output.index_segment));
      if (output.applied_delete_packet_id.has_value()) {
        delete_state_shadow.emplace_back(
          output.applied_delete_packet_id.value());
      }
    }
  };

  std::vector<yaclib::Future<Out>> futures1;

  for (DocumentMask deleted_docs{{*dir.ResourceManager().transactions}};
       const auto& existing_segment : committed_reader.GetReaders()) {
    auto& index_segment =
      committed_meta.index_meta.segments[current_segment_index];
    progress("Stage 1: Apply removals to the existing segments",
             current_segment_index++, committed_reader_size);

    // skip already masked segments
    if (segment_mask.contains(existing_segment->Meta().name)) {
      continue;
    }

    // We don't want to call clear here because even for empty map it costs O(n)
    SDB_ASSERT(deleted_docs.empty());

    // mask documents matching filters from segment_contexts
    // (i.e. from new operations)
    for (const auto& packet : plan.commit_packets) {
      const bool matched = RemoveFromExistingSegmentByPacket(
        deleted_docs, packet, existing_segment);
      if (packet.kind == DeletePacketKind::KReplace && matched) {
        matched_replace_ticks.emplace(packet.tick);
      }
    }

    // Write docs_mask if masks added
    if (const size_t num_removals = deleted_docs.size(); num_removals) {
      // If all docs are masked then mask segment
      if (existing_segment.live_docs_count() == num_removals) {
        deleted_docs.clear();
        // It's important to mask empty segment to rollback
        // the affected consolidations

        segment_mask.emplace(existing_segment->Meta().name);
        modified = true;
        continue;
      }

      // Append removals
      IndexSegment segment{.meta = index_segment.meta};
      segment.meta.docs_mask = [&] {
        auto docs_mask = CopyMask(dir, existing_segment);
        docs_mask->merge(deleted_docs);
        return docs_mask;
      }();
      deleted_docs.clear();

      auto process = [&dir, segment = std::move(segment),
                      &existing_segment] mutable noexcept -> Out {
        index_utils::FlushIndexSegment(dir, segment);  // Write with new mask
        auto new_segment =
          existing_segment.GetImpl()->UpdateMeta(dir, segment.meta);

        return Out{.segment_reader{std::move(new_segment)},
                   .index_segment{std::move(segment)},
                   .add_to_part_sync = true,
                   .applied_delete_packet_id = std::nullopt};
      };

      auto [f, p] = yaclib::MakeContract<Out>();
      auto task = [process = std::move(process),
                   p = std::move(p)] mutable noexcept {
        try {
          auto result = process();
          std::move(p).Set(std::move(result));
        } catch (...) {
          std::move(p).Set(std::current_exception());
        }
      };

      _async_flush.Run(std::move(task));
      futures1.emplace_back(std::move(f));

    } else {
      auto f = yaclib::MakeFuture(
        Out{.segment_reader{std::move(existing_segment.GetImpl())},
            .index_segment{std::move(index_segment)}});
      futures1.emplace_back(std::move(f));
    }
  }
  process_futures(futures1);

  // yaclib::Wait(futures.begin(), futures.end());
  // for (auto& future : futures) {
  //   auto res = std::move(future).Get().Ok();

  //   if (res.add_to_part_sync) {
  //     partial_sync.emplace_back(readers.size());
  //   }
  //   readers.emplace_back(std::move(res.segment_reader));
  //   pending_meta.segments.emplace_back(std::move(res.index_segment));
  // }

  // Stage 2
  // Add pending complete segments registered by import or consolidation

  // Number of candidates that have been registered for pending consolidation
  size_t current_imports_index = 0;
  size_t import_candidates_count = 0;
  size_t partial_sync_threshold = readers.size();

  std::vector<yaclib::Future<Out>> futures2;
  for (auto& import : ctx->imports) {
    progress("Stage 2: Handling consolidated/imported segments",
             current_imports_index++, ctx->imports.size());

    SDB_ASSERT(import.reader);  // Ensured by Consolidation/Import
    auto& meta = import.segment.meta;
    auto& import_reader = import.reader;
    auto import_docs_mask = CopyMask(dir, *import_reader);

    bool docs_mask_modified = false;

    const ConsolidationView candidates{import.consolidation_ctx.candidates};

    const auto pending_consolidation =
      static_cast<bool>(import.consolidation_ctx.merger);

    if (pending_consolidation) {
      // Pending consolidation request
      CandidatesMapping mappings;
      const auto [count, has_removals] =
        MapCandidates(mappings, candidates, readers);

      if (count != candidates.size()) {
        // At least one candidate is missing in pending meta can't finish
        // consolidation
        SDB_DEBUG("xxxxx", sdb::Logger::IRESEARCH,
                  "Failed to finish merge for segment '", meta.name,
                  "', found only '", count, "' out of '", candidates.size(),
                  "' candidates");

        continue;  // Skip this particular consolidation
      }

      // Mask mapped candidates segments from the to-be added new segment
      for (const auto& mapping : mappings) {
        const auto* reader = mapping.second.old.segment;
        SDB_ASSERT(reader);
        segment_mask.emplace(reader->Meta().name);
      }

      // Mask mapped (matched) segments from the currently ongoing commit
      for (const auto& segment : readers) {
        if (mappings.contains(segment.Meta().name)) {
          // Important to store the address of implementation
          segment_mask.emplace(segment.Meta().name);
        }
      }

      // Have some changes, apply removals
      if (has_removals) {
        const auto success = MapRemovals(
          mappings, import.consolidation_ctx.merger, *import_docs_mask);

        if (!success) {
          // Consolidated segment has docs missing from 'segments'
          SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
                   "Failed to finish merge for segment '", meta.name,
                   "', due to removed documents still present "
                   "the consolidation candidates");

          continue;  // Skip this particular consolidation
        }

        // We're done with removals for pending consolidation
        // they have been already applied to candidates above
        // and successfully remapped to consolidated segment
        docs_mask_modified |= true;
      }

      // We've seen at least 1 successfully applied
      // pending consolidation request
      import_candidates_count += candidates.size();
    } else {
      // During consolidation doc_mask could be already populated even for just
      // merged segment. Pending already imported/consolidated segment, apply
      // removals mask documents matching filters from segment_contexts
      // (i.e. from new operations)

      for (const auto& packet : plan.commit_packets) {
        if (import.tick <= packet.tick) {
          const bool matched = RemoveFromImportedSegmentByPacket(
            *import_docs_mask, packet, *import_reader);
          docs_mask_modified |= matched;
          if (packet.kind == DeletePacketKind::KReplace && matched) {
            matched_replace_ticks.emplace(packet.tick);
          }
        }
      }
    }

    // Skip empty segments
    if (meta.docs_count <= import_docs_mask->size()) {
      SDB_ASSERT(meta.docs_count == import_docs_mask->size());
      modified = true;  // FIXME(gnusi): looks strange
      continue;
    }

    if (docs_mask_modified) {
      meta.docs_mask = std::move(import_docs_mask);
    }

    if (docs_mask_modified || pending_consolidation) {
      auto process = [&dir, &import, pending_consolidation,
                      docs_mask_modified] mutable -> Out {
        index_utils::FlushIndexSegment(dir, import.segment,
                                       !pending_consolidation);

        if (docs_mask_modified) {
          import.reader = import.reader->UpdateMeta(dir, import.segment.meta);
        }

        return Out{.segment_reader{std::move(import.reader)},
                   .index_segment{std::move(import.segment)}};
      };

      auto [f, p] = yaclib::MakeContract<Out>();
      auto task = [process = std::move(process),
                   p = std::move(p)] mutable noexcept {
        try {
          std::move(p).Set(process());
        } catch (...) {
          std::move(p).Set(std::current_exception());
        }
      };
      _async_flush.Run(std::move(task));
      futures2.emplace_back(std::move(f));

    } else {
      if (docs_mask_modified) {
        import_reader = import_reader->UpdateMeta(dir, meta);
      }
      auto f =
        yaclib::MakeFuture(Out{.segment_reader{std::move(import_reader)},
                               .index_segment{std::move(import.segment)}});
      futures2.emplace_back(std::move(f));
    }

    // readers.emplace_back(std::move(import_reader));
    // pending_meta.segments.emplace_back(std::move(import.segment));
  }
  process_futures(futures2);

  // For pending consolidation we need to filter out consolidation
  // candidates after applying them
  if (import_candidates_count != 0) {
    SDB_ASSERT(import_candidates_count <= readers.size());
    const size_t count = readers.size() - import_candidates_count;
    std::vector<SegmentReader> tmp_readers;
    tmp_readers.reserve(count);
    IndexMeta tmp_meta;
    tmp_meta.segments.reserve(count);
    std::vector<PartialSync> tmp_partial_sync;

    auto partial_sync_begin = partial_sync.begin();
    for (size_t i = 0; i < partial_sync_threshold; ++i) {
      if (auto& segment = readers[i];
          !segment_mask.contains(segment->Meta().name)) {
        partial_sync_begin =
          std::find_if(partial_sync_begin, partial_sync.end(),
                       [i](const auto& v) { return i == v.segment_index; });
        if (partial_sync_begin != partial_sync.end()) {
          tmp_partial_sync.emplace_back(tmp_readers.size());
        }
        tmp_readers.emplace_back(std::move(segment));
        tmp_meta.segments.emplace_back(std::move(pending_meta.segments[i]));
      }
    }
    const auto tmp_partial_sync_threshold = tmp_readers.size();

    tmp_readers.insert(
      tmp_readers.end(),
      std::make_move_iterator(readers.begin() + partial_sync_threshold),
      std::make_move_iterator(readers.end()));
    tmp_meta.segments.insert(
      tmp_meta.segments.end(),
      std::make_move_iterator(pending_meta.segments.begin() +
                              partial_sync_threshold),
      std::make_move_iterator(pending_meta.segments.end()));

    partial_sync_threshold = tmp_partial_sync_threshold;
    partial_sync = std::move(tmp_partial_sync);
    readers = std::move(tmp_readers);
    pending_meta = std::move(tmp_meta);
  }

  auto& curr_cached = ctx->cached;
  auto& next_cached = ctx->next->cached;

  SDB_ASSERT(pending_meta.segments.size() == readers.size());
  if (info.reopen_columnstore) {
    auto it = pending_meta.segments.begin();
    for (auto& reader : readers) {
      auto impl =
        reader.GetImpl()->ReopenColumnStore(dir, it->meta, reader_options);
      reader = SegmentReader{std::move(impl)};
      ++it;
    }
    curr_cached.clear();
    next_cached.clear();
  }

  delete_state_shadow.clear();
  delete_state_shadow.reserve(to_commit.meta.segments.size());

  for (size_t i = 0; i < to_commit.meta.segments.size(); ++i) {
    delete_state_shadow.emplace_back(plan.barrier_id);
  }

  for (const auto& segment : ctx->segments) {
    SDB_ASSERT(segment != nullptr);
    segment->DrainPendingFlush();
  }

  // Stage 3
  // create new segments
  {
    // count total number of segments once
    size_t total_flushed_segments = 0;
    size_t current_flushed_segments = 0;
    for (const auto& segment : ctx->segments) {
      SDB_ASSERT(segment != nullptr);
      // TODO(mbkkt) precise count?
      total_flushed_segments += segment->flushed.size();
    }

    std::vector<FlushedSegmentContext> segment_ctxs;
    // TODO(mbkkt) reserve
    // process all segments that have been seen by the current flush_context
    for (const auto& segment : ctx->segments) {
      SDB_ASSERT(segment);

      // was updated after flush
      SDB_ASSERT(segment->active.committed_buffered_docs == 0);
      SDB_ASSERT(segment->committed_flushed_docs ==
                 segment->flushed_docs.size());
      // process individually each flushed SegmentMeta from the SegmentContext
      for (auto& flushed : segment->flushed) {
        SDB_ASSERT(flushed.GetDocsBegin() < flushed.GetDocsEnd());
        const auto flushed_first_tick =
          segment->flushed_docs[flushed.GetDocsBegin()].tick;
        const auto flushed_last_tick =
          segment->flushed_docs[flushed.GetDocsEnd() - 1].tick;
        SDB_ASSERT(flushed_first_tick <= flushed_last_tick);

        if (flushed_last_tick <= _committed_tick) {
          continue;  // skip flushed from previous Commit
        }
        if (tick < flushed_first_tick) {
          break;  // skip flushed from next Commit
        }
        progress("Stage 3: Creating new/reopen old segments",
                 current_flushed_segments++, total_flushed_segments);

        SDB_ASSERT(flushed.meta.live_docs_count != 0);
        SDB_ASSERT(flushed.meta.live_docs_count <= flushed.meta.docs_count);
        SDB_ASSERT(!flushed.meta.docs_mask);

        auto reader = [&] {
          if (auto it = curr_cached.find(&flushed); it != curr_cached.end()) {
            SDB_ASSERT(it->second != nullptr);
            // We don't support case when segment is committed partially more
            // than one time. Because it's useless and ineffective.
            SDB_ASSERT(flushed_last_tick <= tick);
            // reuse existing reader with initial meta and docs_mask
            return it->second->UpdateMeta(dir, flushed.meta);
          } else {
            return SegmentReaderImpl::Open(dir, flushed.meta, reader_options);
          }
        }();
        SDB_ASSERT(reader);

        if (tick < flushed_last_tick) {
          next_cached[&flushed] = reader;
        }

        auto& segment_ctx = segment_ctxs.emplace_back(
          std::move(reader), *segment, flushed, dir.ResourceManager(),
          matched_replace_ticks);

        // mask documents matching filters from all flushed segment_contexts
        // (i.e. from new operations)

        for (const auto& packet : plan.commit_packets) {
          if (flushed_first_tick <= packet.tick) {
            segment_ctx.RemoveByPacket(packet);
          }
        }
      }
    }

    // write docs_mask if !empty(), if all docs are masked then remove segment
    // altogether
    size_t current_segment_ctxs = 0;
    std::vector<yaclib::Future<Out>> futures3;
    for (auto& segment_ctx : segment_ctxs) {
      // note: from the code, we are still a part of 'Stage 3',
      // but we need to report something different here, i.e. 'Stage 4'
      progress("Stage 4: Applying removals for new segments",
               current_segment_ctxs++, segment_ctxs.size());

      if (segment_ctx.segment.has_replace) {
        segment_ctx.MaskUnusedReplace(_committed_tick, tick);
      }
      DocumentMask document_mask{{*dir.ResourceManager().readers}};
      IndexSegment new_segment;
      if (segment_ctx.MakeDocumentMask(tick, document_mask, new_segment)) {
        modified |= segment_ctx.flushed.was_flush;
        continue;
      }
      SDB_ASSERT(segment_ctx.flushed.meta.version == new_segment.meta.version);
      const bool need_flush =
        segment_ctx.flushed.was_flush || !document_mask.empty();
      segment_ctx.flushed.was_flush = true;
      if (need_flush) {
        ++new_segment.meta.version;
        ++segment_ctx.flushed.meta.version;
      }
      if (!document_mask.empty()) {
        new_segment.meta.docs_mask =
          std::make_shared<DocumentMask>(std::move(document_mask));
      }

      auto process = [&segment_ctx, &dir, new_segment = std::move(new_segment),
                      need_flush,
                      barrier_id = plan.barrier_id]() mutable -> Out {
        index_utils::FlushIndexSegment(dir, new_segment, false);
        if (need_flush) {
          segment_ctx.reader =
            segment_ctx.reader->UpdateMeta(dir, new_segment.meta);
        }
        SDB_ASSERT(segment_ctx.flushed.applied_delete_packet_id <= barrier_id);
        segment_ctx.flushed.applied_delete_packet_id = barrier_id;

        return Out{.segment_reader{std::move(segment_ctx.reader)},
                   .index_segment = std::move(new_segment),
                   .applied_delete_packet_id =
                     segment_ctx.flushed.applied_delete_packet_id};
      };

      auto [f, p] = yaclib::MakeContract<Out>();
      auto task = [process = std::move(process),
                   p = std::move(p)]() mutable noexcept {
        try {
          std::move(p).Set(process());
        } catch (...) {
          std::move(p).Set(std::current_exception());
        }
      };

      _async_flush.Run(std::move(task));
      futures3.emplace_back(std::move(f));
    }
    process_futures(futures3);
  }

  if (!plan.commit_packets.empty()) {
    ResolveDeleteStateShadow(plan.commit_packets, delete_state_shadow,
                             plan.barrier_id);
  }

  SDB_ASSERT(readers.size() == pending_meta.segments.size());
  SDB_ASSERT(delete_state_shadow.size() == pending_meta.segments.size());

#ifdef SDB_DEV
  {
    std::vector<std::string_view> filenames;
    filenames.reserve(pending_meta.segments.size());
    for (const auto& meta : pending_meta.segments) {
      filenames.emplace_back(meta.filename);
    }
    absl::c_sort(filenames);
    auto it = std::unique(filenames.begin(), filenames.end());
    SDB_ASSERT(it == filenames.end());
  }
#endif

  // TODO(mbkkt) In general looks useful to iterate here over all segments which
  //  partially committed, and free query memory which already was applied.
  //  But when I start thinking about rollback stuff it looks almost impossible

  auto files_to_sync =
    GetFilesToSync(pending_meta.segments, partial_sync, partial_sync_threshold);

  modified |= !files_to_sync.empty();

  // only flush a new index version upon a new index or a metadata change
  if (!modified) {
    SDB_ASSERT(readers.size() == committed_reader_size);
    if (info.reopen_columnstore) {
      auto new_reader = std::make_shared<const DirectoryReaderImpl>(
        committed_reader.Dir(), committed_reader.Codec(),
        committed_reader.Options(), DirectoryMeta{committed_reader.Meta()},
        std::move(readers));
      std::atomic_store_explicit(&_committed_reader, std::move(new_reader),
                                 std::memory_order_release);
    }
    if (!plan.commit_packets.empty()) {
      ResolveDeleteStateShadow(plan.commit_packets,
                               _committed_delete_state_shadow, plan.barrier_id);
      AdvanceCommittedDeleteFloor(plan.barrier_id,
                                  _committed_delete_state_shadow);
    }
    return {};
  }

  InitMeta(pending_meta, LimitTick(tick, flushed_tick));

  if (!next_cached.empty()) {
    cleanup_lock.lock();
    _consolidating.segments.reserve(_consolidating.segments.size() +
                                    next_cached.size());
    for (const auto& entry : next_cached) {
      _consolidating.segments.emplace(entry.second->Meta().name);
    }
  }

  to_commit.tick = LimitTick(tick, _committed_tick);
  to_commit.files_to_sync = std::move(files_to_sync);
  return to_commit;
}

void IndexWriter::ApplyFlush(PendingContext&& context) {
  SDB_ASSERT(!_pending_state.Valid());
  SDB_ASSERT(context.ctx);
  SDB_ASSERT(context.ctx->dir != nullptr);

  RefTrackingDirectory& dir = *context.ctx->dir;

  std::string index_meta_file;
  DirectoryMeta to_commit{.index_meta = std::move(context.meta)};

  // Execute 1st phase of index meta transaction
  if (!_writer->prepare(dir, to_commit.index_meta, to_commit.filename,
                        index_meta_file)) {
    throw IllegalState{absl::StrCat(
      "Failed to write index metadata for index: ", index_meta_file)};
  }

  // The 1st phase of the transaction successfully finished here,
  // ensure we rollback changes if something goes wrong afterwards
  const auto new_gen = to_commit.index_meta.gen;
  Finally update_generation = [&]() noexcept {
    if (!_pending_state.Valid()) [[unlikely]] {
      _writer->rollback();  // Rollback failed transaction
    }

    // Ensure writer's generation is updated
    _last_gen = new_gen;
  };

  context.files_to_sync.emplace_back(to_commit.filename);

  if (!dir.sync(context.files_to_sync)) {
    throw IoError{
      absl::StrCat("Failed to sync files for index: ", index_meta_file)};
  }

  // Update file name so that directory reader holds a reference
  to_commit.filename = std::move(index_meta_file);
  // Assemble directory reader
  _pending_state.delete_state_shadow = std::move(context.delete_state_shadow);
  SDB_ASSERT(_pending_state.delete_state_shadow.size() ==
             to_commit.index_meta.segments.size());
  _pending_state.commit = std::make_shared<const DirectoryReaderImpl>(
    dir, _codec, _committed_reader->Options(), std::move(to_commit),
    std::move(context.readers));
  SDB_ASSERT(context.ctx);
  static_cast<PendingBase&>(_pending_state) = std::move(context);
  SDB_ASSERT(_pending_state.Valid());
}

bool IndexWriter::Start(const CommitInfo& info) {
  if (_pending_state.Valid()) {
    // Begin has been already called without corresponding call to commit
    return false;
  }

  auto to_commit = PrepareFlush(info);

  if (to_commit.Empty()) {
    // Nothing to commit, no transaction started
    _committed_tick = LimitTick(info.tick, _committed_tick);
    return false;
  }
  Finally cleanup = [&]() noexcept {
    if (!to_commit.Empty()) {
      to_commit.StartReset(*this);
    }
  };

  // TODO(mbkkt) error here means we don't remove cached from consolidating
  ApplyFlush(std::move(to_commit));

  return true;
}

void IndexWriter::Finish() {
  if (!_pending_state.Valid()) {
    return;
  }

  Finally cleanup = [&]() noexcept {
    Abort();  // after FinishReset it's noop
  };

  if (!_writer->commit()) [[unlikely]] {
    throw IllegalState{"Failed to commit index metadata"};
  }

  auto& delete_state_shadow = _pending_state.delete_state_shadow;
  try {
    AdvanceCommittedDeleteFloor(_pending_state.delete_barrier_id,
                                delete_state_shadow);
  } catch (...) {
    // cleanup-only path, commit already successful
  }
  // noexcept part!
  _pending_state.StartReset(*this, true);
  SDB_ASSERT(_pending_state.tick != writer_limits::kMaxTick);
  _committed_tick = _pending_state.tick;

  _committed_delete_state_shadow =
    std::move(_pending_state.delete_state_shadow);
  SDB_ASSERT(_committed_delete_state_shadow.size() ==
             _pending_state.commit->size());

  // after this line transaction is successful (only noexcept operations below)
  std::atomic_store_explicit(&_committed_reader,
                             std::move(_pending_state.commit),
                             std::memory_order_release);
  _pending_state.FinishReset();
}

void IndexWriter::Abort() noexcept {
  if (!_pending_state.Valid()) {
    return;  // There is no open transaction
  }

  _writer->rollback();
  _pending_state.Reset(*this);
}

}  // namespace irs
