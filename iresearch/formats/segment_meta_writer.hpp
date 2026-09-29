
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

#pragma once

#include <duckdb/common/serializer/binary_serializer.hpp>
#include <roaring/roaring.hh>

#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/utils/serialization.hpp"
#include "iresearch/utils/string_utils.hpp"

namespace irs {

struct SegmentMetaWriterImpl : public SegmentMetaWriter {
  static constexpr std::string_view kFormatExt = "sm";

  static constexpr size_t kMinChainBytes = 4096;

  static constexpr duckdb::field_id_t kFieldMaskSize = 0;
  static constexpr duckdb::field_id_t kFieldParents = 1;
  static constexpr duckdb::field_id_t kFieldFiles = 2;
  static constexpr duckdb::field_id_t kFieldDocsCount = 3;
  static constexpr duckdb::field_id_t kFieldByteSize = 4;

  void Write(Directory& dir, std::string& filename, SegmentMeta& meta,
             const DocumentMask* patch = nullptr, uint64_t parent = 0) final;
};

template<>
inline std::string FileName<SegmentMetaWriter, SegmentMeta>(
  const SegmentMeta& meta) {
  return irs::FileName(meta.name, meta.version,
                       SegmentMetaWriterImpl::kFormatExt);
}

inline void WriteDocumentMask(IndexOutput& out,
                              const roaring::Roaring& compressed,
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

inline void SegmentMetaWriterImpl::Write(Directory& dir, std::string& meta_file,
                                         SegmentMeta& meta,
                                         const DocumentMask* patch,
                                         uint64_t parent) {
  SDB_ASSERT(meta.live_docs_count <= meta.docs_count);
  SDB_ASSERT(meta.docs_count - meta.live_docs_count == RemovalCount(meta));
  SDB_ASSERT(RemovalCount(meta) < doc_limits::eof());
  SDB_ASSERT(meta.docs_mask_size <= meta.byte_size);
  const auto size_without_mask = meta.byte_size - meta.docs_mask_size;

  const auto& docs_mask = meta.docs_mask;
  const bool has_mask =
    (docs_mask && !docs_mask->Empty()) || HasInvisible(meta);

  const bool append =
    has_mask && patch != nullptr && meta.docs_mask_size > kMinChainBytes;

  roaring::Roaring compressed;
  if (append) {
    compressed = patch->Compress();
  } else if (has_mask) {
    if (docs_mask) {
      compressed = docs_mask->Compress();
    }
    if (HasInvisible(meta)) {
      compressed.addRange(meta.visible_end,
                          doc_limits::min() + meta.docs_count);
      compressed.runOptimize();
    }
  }
  const uint64_t mask_size = has_mask ? compressed.getSizeInBytes() : 0;

  std::vector<std::string> files;
  std::vector<uint64_t> parents;
  for (const auto& file : meta.files) {
    std::string_view name;
    uint64_t link = 0;
    const bool is_link = ParseFileName(file, kFormatExt, name, link);
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
    files.emplace_back(irs::FileName(meta.name, parent, kFormatExt));
    chain_bytes = meta.docs_mask_size;
  }

  meta_file = FileName<SegmentMetaWriter>(meta);
  auto out = dir.create(meta_file);

  if (!out) [[unlikely]] {
    throw IoError{absl::StrCat("failed to create file, path: ", meta_file)};
  }

  duckdb::BinarySerializer meta_out{*out, duckdb::VersionStorageOptions()};
  meta_out.Begin();
  meta_out.WritePropertyWithDefault<uint64_t>(kFieldMaskSize, "mask_size",
                                              mask_size, 0);
  if (!parents.empty()) {
    meta_out.WriteList(kFieldParents, "parents", parents.size(),
                       [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
                         list.WriteElement<uint64_t>(parents[i]);
                       });
  }
  if (!append) {
    meta_out.WriteList(kFieldFiles, "files", files.size(),
                       [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
                         list.WriteElement<std::string>(files[i]);
                       });
  }
  meta_out.WriteProperty<uint32_t>(kFieldDocsCount, "docs_count",
                                   meta.docs_count);
  meta_out.WriteProperty<uint64_t>(kFieldByteSize, "byte_size",
                                   size_without_mask);
  meta_out.End();

  if (has_mask) {
    WriteDocumentMask(*out, compressed, mask_size);
  }

  meta.files = std::move(files);
  meta.docs_mask_size = chain_bytes + mask_size;
  meta.byte_size = size_without_mask + meta.docs_mask_size;
}

}  // namespace irs
