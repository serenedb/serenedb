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

#include <absl/strings/str_cat.h>

#include <duckdb/common/error_data.hpp>
#include <exception>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
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

yaclib::Future<MergeResult> MaintainedStore::AwaitMerge(
  std::chrono::steady_clock::time_point begin,
  yaclib::Future<irs::CompactionResult> merge) {
  bool empty_compaction = false;
  MergeResult result;
  try {
    result.outcome = co_await std::move(merge);
    if (!result.outcome) {
      SDB_DEBUG(SEARCH, "merge of relation ", _merge_relation->id,
                " produced nothing: cancelled, or every document was removed");
      empty_compaction = true;
    } else {
      empty_compaction = result.outcome.size == 0;
    }
  } catch (const std::exception& e) {
    result.outcome.error = irs::CompactionError::Fail;
    result.error = duckdb::ErrorData{e}.RawMessage();
    result.res = absl::InternalError(absl::StrCat(
      "merge of relation ", _merge_relation->id, " failed: ", result.error));
  }
  result.time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count();
  _maintenance.RecordCompaction(result.res, empty_compaction, result.time_ms);
  co_return result;
}

}  // namespace sdb::search
