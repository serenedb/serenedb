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
#include <roaring/roaring.hh>
#include <vector>

#include "iresearch/error/error.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/index/document_mask.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/string_utils.hpp"

namespace irs::segment_meta {
namespace {

void WriteDocumentMask(IndexOutput& out, const roaring::Roaring& compressed,
                       uint64_t size) {
  if (auto* buf = out.Reserve(size); buf != nullptr) {
    compressed.write(reinterpret_cast<char*>(buf));
    return;
  }
  bstring blob;
  irs::utils::StrResize(blob, size);
  compressed.write(reinterpret_cast<char*>(blob.data()));
  out.WriteData(blob.data(), size);
}

}  // namespace

std::string FileName(const SegmentMeta& meta) {
  return irs::FileName(meta.name, meta.version, kExt);
}

void Write(Directory& dir, std::string& meta_file, SegmentMeta& meta,
           const DocumentMaskBuilder* patch, uint64_t parent) {
  SDB_ASSERT(meta.live_docs_count <= meta.docs_count);
  SDB_ASSERT(meta.docs_count - meta.live_docs_count == RemovalCount(meta));
  SDB_ASSERT(RemovalCount(meta) < doc_limits::eof());
  SDB_ASSERT(meta.docs_mask_size <= meta.byte_size);
  const auto size_without_mask = meta.byte_size - meta.docs_mask_size;

  const auto& docs_mask = meta.docs_mask;
  const bool has_mask = docs_mask && !docs_mask->Empty();

  const bool append =
    has_mask && patch != nullptr && meta.docs_mask_size > kMinChainBytes;

  roaring::Roaring compressed;
  if (append) {
    compressed = patch->Compress();
  } else if (has_mask) {
    compressed = docs_mask->Compress();
  }
  const uint64_t mask_size = has_mask ? compressed.getSizeInBytes() : 0;

  std::vector<std::string> files;
  std::vector<uint64_t> parents;
  for (const auto& file : meta.files) {
    std::string_view name;
    uint64_t link = 0;
    const bool is_link = ParseFileName(file, kExt, name, link);
    SDB_ASSERT(!is_link || name == meta.name);
    if (is_link && append) {
      parents.push_back(link);
    }
    if (!is_link || append) {
      files.push_back(file);
    }
  }
  uint64_t chain_bytes = 0;
  if (append) {
    SDB_ASSERT(parent < meta.version);
    parents.push_back(parent);
    files.emplace_back(irs::FileName(meta.name, parent, kExt));
    chain_bytes = meta.docs_mask_size;
  }

  meta_file = FileName(meta);
  auto out = dir.create(meta_file);

  if (!out) [[unlikely]] {
    throw IoError{absl::StrCat("failed to create file, path: ", meta_file)};
  }

  if (has_mask) {
    WriteDocumentMask(*out, compressed, mask_size);
  }

  format_utils::WriteFooter(*out, [&](duckdb::BinarySerializer& meta_out) {
    if (!parents.empty()) {
      meta_out.WriteList(
        kFieldParents, "parents", parents.size(),
        [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
          list.WriteElement<uint64_t>(parents[i]);
        });
    }
    if (!append) {
      meta_out.WriteList(
        kFieldFiles, "files", files.size(),
        [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
          list.WriteElement<std::string>(files[i]);
        });
    }
    meta_out.WriteProperty<uint32_t>(kFieldDocsCount, "docs_count",
                                     meta.docs_count);
    meta_out.WriteProperty<uint64_t>(kFieldByteSize, "byte_size",
                                     size_without_mask);
  });

  meta.files = std::move(files);
  meta.docs_mask_size = chain_bytes + mask_size;
  meta.byte_size = size_without_mask + meta.docs_mask_size;
}

}  // namespace irs::segment_meta
