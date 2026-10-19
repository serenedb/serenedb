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
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/formats/segment_meta_reader.hpp"

#include <absl/strings/str_cat.h>

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <vector>

#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/segment_meta_writer.hpp"
#include "iresearch/index/document_mask.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::segment_meta {

void Read(const Directory& dir, SegmentMeta& meta, std::string_view filename) {
  SDB_ASSERT(!IsNull(filename));

  std::string_view segment_name;
  uint64_t segment_version = 0;

  if (!ParseFileName(filename, kExt, segment_name, segment_version))
    [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                    ERR_MSG("Malformed segment meta file name: ", filename));
  }

  std::string name{segment_name};

  auto in = dir.open(filename, IOAdvice::SEQUENTIAL | IOAdvice::READONCE);

  std::vector<std::string> files;
  bool has_files = false;
  uint32_t docs_count = 0;
  uint64_t size = 0;
  const auto footer = format_utils::ReadFooter(
    *in, filename, [&](duckdb::BinaryDeserializer& meta_in, uint64_t) {
      has_files = meta_in.ReadOptionalList(
        kFieldFiles, "files",
        [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
          files.emplace_back(list.ReadElement<std::string>());
        });
      docs_count =
        meta_in.ReadProperty<uint32_t>(kFieldDocsCount, "docs_count");
      size = meta_in.ReadProperty<uint64_t>(kFieldByteSize, "byte_size");
    });
  const auto docs_mask_size = footer.data_len;

  std::shared_ptr<const DocumentMask> docs_mask;
  if (docs_mask_size != 0) {
    docs_mask = std::make_shared<const DocumentMask>(
      DocumentMask::ReadFrozen(*in, docs_mask_size));
  }

  if (!has_files) [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                    ERR_MSG("While reading segment meta '", name,
                            "', error: the segment file list is missing"));
  }

  const auto mask_count = docs_mask ? docs_mask->Count() : 0;

  if (docs_count >= doc_limits::eof() || mask_count > docs_count) [[unlikely]] {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INDEX_CORRUPTED),
      ERR_MSG("While reading segment meta '", name, "', error: docs_count(",
              docs_count, ") is out of range for ", mask_count,
              " masked document(s)"));
  }

  // all operations below are noexcept
  meta.name = std::move(name);
  meta.version = segment_version;
  meta.docs_count = docs_count;
  meta.live_docs_count = static_cast<uint32_t>(docs_count - mask_count);
  meta.docs_mask = std::move(docs_mask);
  meta.docs_mask_size = docs_mask_size;
  meta.byte_size = size + docs_mask_size;
  meta.files = std::move(files);
}

}  // namespace irs::segment_meta
