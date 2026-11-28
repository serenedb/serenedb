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

#include <bit>

#include "iresearch/index/file_names.hpp"
#include "iresearch/index/segment_reader_impl.hpp"
#include "iresearch/utils/pg/errcodes.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

[[noreturn]] void ThrowMalformed(std::string_view line) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("malformed segment list line: ", line));
}

std::vector<SegmentMeta> ReadSegmentList(std::string_view text) {
  std::vector<SegmentMeta> segments;
  for (const std::string_view line :
       absl::StrSplit(text, '\n', absl::SkipEmpty())) {
    const std::vector<std::string_view> parts = absl::StrSplit(line, ' ');
    uint64_t id = 0;
    uint32_t kinds = 0;
    auto& meta = segments.emplace_back();
    if (parts.size() < 3 || !absl::SimpleAtoi(parts[0], &id) ||
        !absl::SimpleAtoi(parts[1], &kinds) || kinds >= (1U << kFileKinds) ||
        !absl::SimpleAtoi(parts[2], &meta.docs_count) ||
        parts.size() != 3 + 2 * static_cast<size_t>(std::popcount(kinds))) {
      ThrowMalformed(line);
    }
    meta.name = FileName(id);
    meta.kinds = static_cast<uint8_t>(kinds);
    meta.live_docs_count = meta.docs_count;
    size_t at = 3;
    for (size_t kind = 0; kind != kFileKinds; ++kind) {
      if ((kinds & (1U << kind)) == 0) {
        continue;
      }
      auto& sizes = meta.files[kind];
      if (!absl::SimpleAtoi(parts[at], &sizes.file) ||
          !absl::SimpleAtoi(parts[at + 1], &sizes.footer)) {
        ThrowMalformed(line);
      }
      at += 2;
    }
  }
  return segments;
}

}  // namespace

void AppendSegmentList(std::string& out, uint64_t id, const SegmentMeta& meta,
                       doc_id_t docs) {
  absl::StrAppend(&out, id, " ", static_cast<uint32_t>(meta.kinds), " ", docs);
  for (size_t kind = 0; kind != kFileKinds; ++kind) {
    if ((meta.kinds & (1U << kind)) != 0) {
      absl::StrAppend(&out, " ", meta.files[kind].file, " ",
                      meta.files[kind].footer);
    }
  }
  absl::StrAppend(&out, "\n");
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
