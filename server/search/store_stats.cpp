////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "search/store_stats.h"

#include <duckdb/common/error_data.hpp>
#include <exception>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
#include <utility>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

namespace sdb::search {

StoreStats StoreStats::FromReader(const irs::DirectoryReader& reader) {
  SDB_ASSERT(reader);
  StoreStats stats;
  auto& segments = reader->Meta().index_meta.segments;
  stats.numSegments = segments.size();
  stats.numDocs = reader->docs_count();
  stats.numLiveDocs = reader->live_docs_count();
  stats.numFiles = 1 + stats.numSegments;
  for (const auto& segment : segments) {
    const auto& meta = segment.meta;
    stats.indexSize += meta.byte_size;
    stats.numFiles += meta.files.size();
  }
  return stats;
}

void MaintainedStore::SetWriterOptions(irs::IndexWriterOptions& options) const {
  options.sort = _sort;
}

yaclib::Future<MergeResult> MaintainedStore::RunMergeAsync(
  irs::IndexWriter::MergeTicket ticket, const irs::MergeProgress& progress,
  const irs::IndexFieldOptions* field_options, const irs::AnnBuildEnv* env,
  const irs::PipelineMergeExecutor& executor) {
  bool empty_compaction = false;
  bool failed = false;
  MergeResult result;
  try {
    co_await _writer->RunMerge(std::move(ticket), field_options, progress, env,
                               executor, result.outcome);
    if (!result.outcome) {
      SDB_DEBUG(SEARCH, "merge of relation ", _merge_relation->id,
                " produced nothing: cancelled, or every document was removed");
      empty_compaction = true;
    } else {
      empty_compaction = result.outcome.size == 0;
    }
  } catch (const std::exception& e) {
    failed = true;
    result.outcome.error = irs::CompactionError::Fail;
    result.error = duckdb::ErrorData{e}.RawMessage();
  }
  _maintenance.RecordCompaction(
    failed, empty_compaction,
    result.outcome.stats.profile.TotalNanos() / 1'000'000);
  co_return result;
}

}  // namespace sdb::search
