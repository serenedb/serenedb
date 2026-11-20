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

#include "iresearch/index/segment_list.hpp"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include "iresearch/index/file_names.hpp"
#include "iresearch/index/segment_reader_impl.hpp"
#include "iresearch/utils/pg/errcodes.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

std::vector<SegmentMeta> ReadSegmentList(std::string_view text) {
  std::vector<SegmentMeta> segments;
  for (const std::string_view line :
       absl::StrSplit(text, '\n', absl::SkipEmpty())) {
    const std::vector<std::string_view> parts = absl::StrSplit(line, ' ');
    uint64_t id = 0;
    uint32_t kinds = 0;
    auto& meta = segments.emplace_back();
    if (parts.size() != 3 || !absl::SimpleAtoi(parts[0], &id) ||
        !absl::SimpleAtoi(parts[1], &kinds) ||
        !absl::SimpleAtoi(parts[2], &meta.docs_count)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                      ERR_MSG("malformed segment list line: ", line));
    }
    meta.name = FileName(id);
    meta.kinds = static_cast<uint8_t>(kinds);
    meta.live_docs_count = meta.docs_count;
  }
  return segments;
}

}  // namespace

void AppendSegmentList(std::string& out, uint64_t id, uint8_t kinds,
                       doc_id_t docs) {
  absl::StrAppend(&out, id, " ", static_cast<uint32_t>(kinds), " ", docs, "\n");
}

CompositeReader OpenSegmentList(duckdb::DatabaseInstance& db,
                                duckdb::FileSystem& fs, std::string_view root,
                                std::string_view text,
                                const IndexReaderOptions& options) {
  std::vector<SegmentReader> readers;
  for (const auto& meta : ReadSegmentList(text)) {
    readers.emplace_back(
      SegmentReaderImpl::Open(db, fs, root, meta, options, nullptr));
  }
  return {std::move(readers), /*stats=*/true};
}

}  // namespace irs
