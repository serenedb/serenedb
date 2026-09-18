
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

#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/store/store_utils.hpp"

namespace irs {

struct SegmentMetaWriterImpl : public SegmentMetaWriter {
  static constexpr std::string_view kFormatExt = "sm";
  static constexpr std::string_view kFormatName = "iresearch_10_segment_meta";

  static constexpr int32_t kFormatVersion = 0;

  static constexpr uint32_t kMaxMaskFiles = 8;

  static constexpr size_t kMinChainBytes = 4096;

  static constexpr size_t kMaxInlineBytes = 1024;

  void write(Directory& dir, std::string& filename, SegmentMeta& meta) final {
    Write(dir, filename, meta, nullptr);
  }

  void WritePatch(Directory& dir, std::string& filename, SegmentMeta& meta,
                  const DocumentMask& patch) final {
    Write(dir, filename, meta, &patch);
  }

 private:
  static void Write(Directory& dir, std::string& filename, SegmentMeta& meta,
                    const DocumentMask* patch);
};

template<>
inline std::string FileName<SegmentMetaWriter, SegmentMeta>(
  const SegmentMeta& meta) {
  return irs::FileName(meta.name, meta.version,
                       SegmentMetaWriterImpl::kFormatExt);
}

struct DocsMaskWriter {
  static constexpr std::string_view kFormatExt = "dm";
};

inline void WriteDocumentMask(IndexOutput& out,
                              const roaring::Roaring& compressed) {
  const auto size = compressed.getSizeInBytes();
  SDB_ASSERT(size < std::numeric_limits<uint32_t>::max());
  out.WriteV32(static_cast<uint32_t>(size));
  if (auto* buf = out.Reserve(size); buf != nullptr) {
    compressed.write(reinterpret_cast<char*>(buf));
    return;
  }
  bstring blob(size, 0);
  compressed.write(reinterpret_cast<char*>(blob.data()));
  out.WriteData(blob.data(), size);
}

inline uint64_t WriteDocumentMask(Directory& dir, SegmentMeta& meta,
                                  const DocumentMask* patch,
                                  roaring::Roaring& compressed) {
  SDB_ASSERT(meta.docs_mask_files <= meta.files.size());

  const auto& docs_mask = meta.docs_mask;
  if (!docs_mask || docs_mask->Empty()) {
    meta.files.resize(meta.files.size() - meta.docs_mask_files);
    meta.docs_mask_files = 0;
    return 0;
  }
  compressed = docs_mask->Compress();
  const auto mask_size = compressed.getSizeInBytes();
  if (mask_size <= SegmentMetaWriterImpl::kMaxInlineBytes) {
    meta.files.resize(meta.files.size() - meta.docs_mask_files);
    meta.docs_mask_files = 0;
    return 0;
  }
  SDB_ASSERT(RemovalCount(meta) < doc_limits::eof());

  const bool append =
    patch != nullptr && meta.docs_mask_files != 0 &&
    meta.docs_mask_files < SegmentMetaWriterImpl::kMaxMaskFiles &&
    mask_size > SegmentMetaWriterImpl::kMinChainBytes;
  auto chain_size = meta.docs_mask_size;
  if (!append) {
    meta.files.resize(meta.files.size() - meta.docs_mask_files);
    meta.docs_mask_files = 0;
    chain_size = 0;
  }

  SDB_ASSERT(!append ||
             meta.files.back() == irs::FileName(meta.name, meta.version - 1,
                                                DocsMaskWriter::kFormatExt));
  auto& name = meta.files.emplace_back(
    irs::FileName(meta.name, meta.version, DocsMaskWriter::kFormatExt));
  ++meta.docs_mask_files;

  auto out = dir.create(name);

  if (!out) [[unlikely]] {
    throw IoError{absl::StrCat("failed to create file, path: ", name)};
  }

  WriteDocumentMask(*out, append ? patch->Compress() : compressed);

  return chain_size + out->Position();
}

inline void WriteStrings(IndexOutput& out, const auto& strings) {
  SDB_ASSERT(strings.size() < std::numeric_limits<uint32_t>::max());

  out.WriteV32(static_cast<uint32_t>(strings.size()));
  for (const auto& s : strings) {
    WriteStr(out, s);
  }
}

inline void SegmentMetaWriterImpl::Write(Directory& dir, std::string& meta_file,
                                         SegmentMeta& meta,
                                         const DocumentMask* patch) {
  if (meta.docs_count < meta.live_docs_count ||
      meta.docs_count - meta.live_docs_count != RemovalCount(meta))
    [[unlikely]] {
    throw IndexError{absl::StrCat("Invalid segment meta '", meta.name,
                                  "' detected : docs_count=", meta.docs_count,
                                  ", live_docs_count=", meta.live_docs_count)};
  }

  SDB_ASSERT(meta.docs_mask_size <= meta.byte_size);
  const auto size_without_mask = meta.byte_size - meta.docs_mask_size;

  roaring::Roaring compressed;
  const auto docs_mask_size = WriteDocumentMask(dir, meta, patch, compressed);

  meta_file = FileName<SegmentMetaWriter>(meta);
  auto out = dir.create(meta_file);

  if (!out) [[unlikely]] {
    throw IoError{absl::StrCat("failed to create file, path: ", meta_file)};
  }

  format_utils::WriteHeader(*out, kFormatName, kFormatVersion);
  WriteStr(*out, meta.name);
  out->WriteV64(meta.version);
  out->WriteV32(meta.live_docs_count);
  const auto removal_count = RemovalCount(meta);
  out->WriteV32(removal_count);
  if (removal_count != 0) {
    const auto uncommitted_count = UncommittedCount(meta);
    const auto scattered_count = removal_count - uncommitted_count;
    out->WriteV32(uncommitted_count);
    out->WriteV32(meta.docs_mask_files);
    if (meta.docs_mask_files == 0 && scattered_count != 0) {
      WriteDocumentMask(*out, compressed);
    }
  }
  out->WriteV64(size_without_mask);
  WriteStrings(*out, std::span{meta.files}.first(meta.files.size() -
                                                 meta.docs_mask_files));
  format_utils::WriteFooter(*out);

  meta.byte_size = size_without_mask + docs_mask_size;
  meta.docs_mask_size = docs_mask_size;
}

}  // namespace irs
