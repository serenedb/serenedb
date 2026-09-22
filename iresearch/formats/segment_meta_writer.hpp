
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
#include <limits>
#include <span>

#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/utils/serialization.hpp"

namespace irs {

struct SegmentMetaWriterImpl : public SegmentMetaWriter {
  static constexpr std::string_view kFormatExt = "sm";

  static constexpr uint32_t kMaxMaskFiles = 8;

  static constexpr size_t kMinChainBytes = 4096;

  static constexpr uint64_t kNoParent = std::numeric_limits<uint64_t>::max();

  static constexpr duckdb::field_id_t kFieldParent = 0;
  static constexpr duckdb::field_id_t kFieldName = 1;
  static constexpr duckdb::field_id_t kFieldVersion = 2;
  static constexpr duckdb::field_id_t kFieldLiveDocsCount = 3;
  static constexpr duckdb::field_id_t kFieldRemovalCount = 4;
  static constexpr duckdb::field_id_t kFieldUncommittedCount = 5;
  static constexpr duckdb::field_id_t kFieldByteSize = 6;
  static constexpr duckdb::field_id_t kFieldFiles = 7;

  void write(Directory& dir, std::string& filename, SegmentMeta& meta) final {
    Write(dir, filename, meta, nullptr, kNoParent);
  }

  void WritePatch(Directory& dir, std::string& filename, SegmentMeta& meta,
                  const DocumentMask& patch, uint64_t parent) final {
    Write(dir, filename, meta, &patch, parent);
  }

 private:
  static void Write(Directory& dir, std::string& filename, SegmentMeta& meta,
                    const DocumentMask* patch, uint64_t parent);
};

template<>
inline std::string FileName<SegmentMetaWriter, SegmentMeta>(
  const SegmentMeta& meta) {
  return irs::FileName(meta.name, meta.version,
                       SegmentMetaWriterImpl::kFormatExt);
}

inline uint64_t WriteDocumentMask(IndexOutput& out,
                                  const roaring::Roaring& compressed) {
  const auto size = compressed.getSizeInBytes();
  if (auto* buf = out.Reserve(size); buf != nullptr) {
    compressed.write(reinterpret_cast<char*>(buf));
    return size;
  }
  bstring blob(size, 0);
  compressed.write(reinterpret_cast<char*>(blob.data()));
  out.WriteData(blob.data(), size);
  return size;
}

inline void SegmentMetaWriterImpl::Write(Directory& dir, std::string& meta_file,
                                         SegmentMeta& meta,
                                         const DocumentMask* patch,
                                         uint64_t parent) {
  if (meta.docs_count < meta.live_docs_count ||
      meta.docs_count - meta.live_docs_count != RemovalCount(meta))
    [[unlikely]] {
    throw IndexError{absl::StrCat("Invalid segment meta '", meta.name,
                                  "' detected : docs_count=", meta.docs_count,
                                  ", live_docs_count=", meta.live_docs_count)};
  }

  SDB_ASSERT(RemovalCount(meta) < doc_limits::eof());
  SDB_ASSERT(meta.docs_mask_size <= meta.byte_size);
  SDB_ASSERT(meta.docs_mask_chain <= meta.files.size() + 1);
  const auto size_without_mask = meta.byte_size - meta.docs_mask_size;

  const auto& docs_mask = meta.docs_mask;
  const bool has_mask = docs_mask && !docs_mask->Empty();

  roaring::Roaring compressed;
  if (has_mask) {
    compressed = docs_mask->Compress();
  }

  const bool append = has_mask && patch != nullptr &&
                      meta.docs_mask_chain != 0 &&
                      meta.docs_mask_chain < kMaxMaskFiles &&
                      compressed.getSizeInBytes() > kMinChainBytes;

  const size_t ancestors =
    meta.docs_mask_chain != 0 ? meta.docs_mask_chain - 1 : 0;
  size_t chain_files = ancestors;
  uint64_t chain_bytes = meta.docs_mask_size;
  if (append) {
    SDB_ASSERT(parent < meta.version);
    meta.files.emplace_back(irs::FileName(meta.name, parent, kFormatExt));
    ++chain_files;
  } else {
    meta.files.resize(meta.files.size() - ancestors);
    chain_files = 0;
    chain_bytes = 0;
    parent = kNoParent;
  }

  meta_file = FileName<SegmentMetaWriter>(meta);
  auto out = dir.create(meta_file);

  if (!out) [[unlikely]] {
    throw IoError{absl::StrCat("failed to create file, path: ", meta_file)};
  }

  uint64_t mask_size = 0;
  if (append) {
    mask_size = WriteDocumentMask(*out, patch->Compress());
  } else if (has_mask) {
    mask_size = WriteDocumentMask(*out, compressed);
  }

  const auto files =
    std::span{meta.files}.first(meta.files.size() - chain_files);

  duckdb::BinarySerializer meta_out{*out, duckdb::VersionStorageOptions()};
  meta_out.Begin();
  meta_out.WritePropertyWithDefault<uint64_t>(kFieldParent, "parent", parent,
                                              kNoParent);
  meta_out.WriteProperty<std::string>(kFieldName, "name", meta.name);
  meta_out.WriteProperty<uint64_t>(kFieldVersion, "version", meta.version);
  meta_out.WriteProperty<uint32_t>(kFieldLiveDocsCount, "live_docs_count",
                                   meta.live_docs_count);
  meta_out.WritePropertyWithDefault<uint32_t>(
    kFieldRemovalCount, "removal_count", RemovalCount(meta), 0);
  meta_out.WritePropertyWithDefault<uint32_t>(
    kFieldUncommittedCount, "uncommitted_count", UncommittedCount(meta), 0);
  meta_out.WriteProperty<uint64_t>(kFieldByteSize, "byte_size",
                                   size_without_mask);
  meta_out.WriteList(kFieldFiles, "files", files.size(),
                     [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
                       list.WriteElement<std::string>(files[i]);
                     });
  meta_out.End();

  out->WriteU64(mask_size);

  meta.docs_mask_size = chain_bytes + mask_size;
  meta.docs_mask_chain = has_mask ? static_cast<uint32_t>(chain_files) + 1 : 0;
  meta.byte_size = size_without_mask + meta.docs_mask_size;
}

}  // namespace irs
