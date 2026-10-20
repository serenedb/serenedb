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

#include "iresearch/formats/segment_meta_writer.hpp"

#include <absl/strings/str_cat.h>

#include <duckdb/common/serializer/binary_serializer.hpp>

#include "iresearch/formats/format_utils.hpp"
#include "iresearch/index/document_mask.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/string_utils.hpp"

namespace irs::segment_meta {
namespace {

void WriteDocumentMask(IndexOutput& out, const DocumentMask& mask,
                       uint64_t size) {
  if (auto* buf = out.Reserve(size); buf != nullptr) {
    mask.WriteFrozen(reinterpret_cast<char*>(buf));
    return;
  }
  bstring blob;
  irs::utils::StrResize(blob, size);
  mask.WriteFrozen(reinterpret_cast<char*>(blob.data()));
  out.WriteData(blob.data(), size);
}

}  // namespace

std::string FileName(const SegmentMeta& meta) {
  return irs::FileName(meta.name, meta.version, kExt);
}

void Write(Directory& dir, std::string& meta_file, SegmentMeta& meta) {
  SDB_ASSERT(meta.live_docs_count <= meta.docs_count);
  SDB_ASSERT(meta.docs_count - meta.live_docs_count == RemovalCount(meta));
  SDB_ASSERT(RemovalCount(meta) < doc_limits::eof());
  SDB_ASSERT(meta.docs_mask_size <= meta.byte_size);
  const auto size_without_mask = meta.byte_size - meta.docs_mask_size;

  const auto* docs_mask = meta.docs_mask.get();
  const bool has_mask = docs_mask && !docs_mask->Empty();
  const uint64_t mask_size = has_mask ? docs_mask->FrozenSize() : 0;

  meta_file = FileName(meta);
  auto out = dir.create(meta_file);

  if (has_mask) {
    WriteDocumentMask(*out, *docs_mask, mask_size);
  }

  format_utils::WriteFooter(*out, [&](duckdb::BinarySerializer& meta_out) {
    meta_out.WriteList(
      kFieldFiles, "files", meta.files.size(),
      [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
        list.WriteElement<std::string>(meta.files[i]);
      });
    meta_out.WriteProperty<uint32_t>(kFieldDocsCount, "docs_count",
                                     meta.docs_count);
    meta_out.WriteProperty<uint64_t>(kFieldByteSize, "byte_size",
                                     size_without_mask);
  });

  meta.docs_mask_size = mask_size;
  meta.byte_size = size_without_mask + mask_size;
}

}  // namespace irs::segment_meta
