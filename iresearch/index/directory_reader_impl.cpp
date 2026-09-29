
////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "directory_reader_impl.hpp"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/types/hyperloglog.hpp>
#include <ranges>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/index_meta_reader.hpp"
#include "iresearch/index/segment_reader_impl.hpp"
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {

struct DirectoryReaderImpl::Init {
  Init(const Directory& dir, const DirectoryMeta& meta,
       const ReadersType& readers);

  FileRefs file_refs;
  uint64_t docs_count{};
  uint64_t live_docs_count{};
  irs::containers::FlatHashMap<field_id,
                               duckdb::unique_ptr<duckdb::BaseStatistics>>
    column_stats;
};

DirectoryReaderImpl::Init::Init(const Directory& dir, const DirectoryMeta& meta,
                                const ReadersType& readers) {
  const bool has_segments_file = !meta.filename.empty();
  const auto& segments = meta.index_meta.segments;
  SDB_ASSERT(segments.size() == readers.size());

  file_refs.reserve(segments.size() + size_t{has_segments_file});

  struct Accumulator {
    duckdb::unique_ptr<duckdb::BaseStatistics> stats;
    duckdb::unique_ptr<duckdb::HyperLogLog> hyperloglog;
  };
  irs::containers::FlatHashMap<field_id, Accumulator> col2stats;

  auto& refs = dir.attributes().refs();
  for (const auto& [index_segment, reader] :
       std::views::zip(segments, readers)) {
    const auto& [filename, segment] = index_segment;
    file_refs.emplace_back(refs.add(filename));
    docs_count += segment.docs_count;
    live_docs_count += segment.live_docs_count;

    const auto* col_reader = reader.GetColReader();
    if (!col_reader) {
      continue;
    }
    for (const auto& column : col_reader->Columns()) {
      auto& [stats, hyperloglog] = col2stats[column->Id()];
      if (!stats) {
        stats = column->MergedStatistics().ToUnique();
      } else {
        stats->Merge(column->MergedStatistics());
      }
      if (const auto* src_hyperloglog = column->HyperLogLog()) {
        if (!hyperloglog) {
          hyperloglog = src_hyperloglog->Copy();
        } else {
          hyperloglog->Merge(*src_hyperloglog);
        }
      }
    }
  }

  if (has_segments_file) {
    file_refs.emplace_back(refs.add(meta.filename));
  }

  for (auto& [field, merged] : col2stats) {
    auto& [stats, hyperloglog] = merged;
    if (hyperloglog) {
      const auto cnt =
        std::min<uint64_t>(hyperloglog->Count(), live_docs_count);
      stats->SetDistinctCount(cnt);
    }
    column_stats[field] = std::move(stats);
  }
}

namespace {

IndexFileRefs::ref_t LoadNewestIndexMeta(IndexMeta& meta, const Directory& dir,
                                         MetaPayloadReader payload) noexcept {
  try {
    IndexFileRefs::ref_t ref;
    std::string filename;

    // ensure have a valid ref to a filename
    while (!ref) {
      const bool index_exists = index_meta::LastFile(dir, filename);

      if (!index_exists) {
        return nullptr;
      }

      ref = directory_utils::Reference(const_cast<Directory&>(dir), filename);
    }

    SDB_ASSERT(ref);
    index_meta::Read(dir, meta, *ref, std::move(payload));

    return ref;
  } catch (const std::exception& e) {
    SDB_ERROR(IRESEARCH,
              absl::StrCat(
                "Caught exception while loading the newest index meta, error '",
                e.what(), "'"));
  } catch (...) {
    SDB_ERROR(IRESEARCH,
              "Caught exception while loading the newest index meta");
  }

  return nullptr;
}

}  // namespace

DirectoryReaderImpl::DirectoryReaderImpl(const Directory& dir,
                                         const IndexReaderOptions& opts,
                                         DirectoryMeta&& meta,
                                         ReadersType&& readers)
  : DirectoryReaderImpl{Init{dir, meta, readers}, dir, opts, std::move(meta),
                        std::move(readers)} {}

DirectoryReaderImpl::DirectoryReaderImpl(Init&& init, const Directory& dir,
                                         const IndexReaderOptions& opts,
                                         DirectoryMeta&& meta,
                                         ReadersType&& readers)
  : CompositeReaderImpl{std::move(readers), init.live_docs_count,
                        init.docs_count},
    _dir{dir},
    _file_refs{std::move(init.file_refs)},
    _meta{std::move(meta)},
    _opts{opts},
    _column_stats{std::move(init.column_stats)} {}

std::shared_ptr<const DirectoryReaderImpl> DirectoryReaderImpl::Open(
  const Directory& dir, const IndexReaderOptions& opts,
  const std::shared_ptr<const DirectoryReaderImpl>& cached,
  MetaPayloadReader payload) {
  IndexMeta index_meta;
  auto meta_file_ref = LoadNewestIndexMeta(index_meta, dir, std::move(payload));

  if (!meta_file_ref) {
    throw IndexNotFound{};
  }

  if (cached && cached->_meta.index_meta == index_meta) {
    return cached;  // no changes to refresh
  }

  static constexpr size_t kInvalidCandidate{std::numeric_limits<size_t>::max()};
  absl::flat_hash_map<std::string_view, size_t> reuse_candidates;

  if (cached) {
    const auto& segments = cached->Meta().index_meta.segments;
    reuse_candidates.reserve(segments.size());

    for (size_t i = 0; const auto& segment : segments) {
      SDB_ASSERT(cached);  // ensured by loop condition above
      auto it = reuse_candidates.emplace(segment.meta.name, i++);

      if (!it.second) [[unlikely]] {
        it.first->second = kInvalidCandidate;  // treat collisions as invalid
      }
    }
  }

  const auto& segments = index_meta.segments;

  ReadersType readers(segments.size());
  auto reader = readers.begin();

  for (const auto& [filename, meta] : segments) {
    const auto it = reuse_candidates.find(meta.name);

    if (it != reuse_candidates.end() && it->second != kInvalidCandidate &&
        meta == cached->_meta.index_meta.segments[it->second].meta) {
      auto tmp = (*cached)[it->second];
      SDB_ASSERT(tmp != nullptr);
      SDB_ASSERT(tmp->Meta() ==
                 cached->_meta.index_meta.segments[it->second].meta);
      *reader = std::move(tmp);
      reuse_candidates.erase(it);
    } else {
      *reader = SegmentReader{SegmentReaderImpl::Open(dir, meta, opts)};
    }

    if (!*reader) {
      throw IndexError{absl::StrCat("While opening reader for segment '",
                                    meta.name,
                                    "', error: failed to open reader")};
    }

    ++reader;
  }

  return std::make_shared<DirectoryReaderImpl>(
    dir, opts,
    DirectoryMeta{.filename = *meta_file_ref,
                  .index_meta = std::move(index_meta)},
    std::move(readers));
}

}  // namespace irs
