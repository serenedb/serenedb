
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
#include "iresearch/utils/bytes_utils.hpp"

namespace irs {

enum DocumentMaskOnDiskFormat {
  DeletedVarintList = 0,
  DeletedDenseBitset = 1,
  AliveVarintList = 2,
};

struct SegmentMetaWriterImpl : public SegmentMetaWriter {
  static constexpr std::string_view kFormatExt = "sm";
  static constexpr std::string_view kFormatName = "iresearch_10_segment_meta";

  static constexpr int32_t kFormatMin = 0;
  static constexpr int32_t kFormatMax = 1;

  explicit SegmentMetaWriterImpl(int32_t version) noexcept : _version(version) {
    SDB_ASSERT(_version >= kFormatMin && version <= kFormatMax);
  }

  void write(Directory& dir, std::string& filename, SegmentMeta& meta) final;

 private:
  int32_t _version;
};

template<>
inline std::string FileName<SegmentMetaWriter, SegmentMeta>(
  const SegmentMeta& meta) {
  return irs::FileName(meta.name, meta.version,
                       SegmentMetaWriterImpl::kFormatExt);
}

// The old format, not used anymore. ReadDocumentMask should support old
// versions, so keeping serialization algorithm for visibility and benchmarking
// purposes.
[[deprecated]]
inline uint64_t WriteDocumentMaskV0(IndexOutput& out,
                                    const DocumentMask* docs_mask) {
  // TODO(gnusi): better format
  uint32_t mask_size =
    docs_mask ? static_cast<uint32_t>(docs_mask->DeletedDocCount()) : 0;
  SDB_ASSERT(mask_size < doc_limits::eof());

  if (!mask_size) {
    out.WriteV32(0);
    return 0;
  }

  out.WriteV32(mask_size);
  const auto pos = out.Position();
  docs_mask->ForEachDeleted([&out](doc_id_t doc_id) { out.WriteV32(doc_id); });
  return out.Position() - pos;
}

inline uint64_t WriteDocumentMaskDeletedVarintList(
  Directory&, IndexOutput& out, const DocumentMask& docs_mask) {
  out.WriteV32(DocumentMaskOnDiskFormat::DeletedVarintList);
  const auto pos = out.Position();
  docs_mask.ForEachDeleted([&out](doc_id_t doc_id) { out.WriteV32(doc_id); });
  return out.Position() - pos;
}
inline uint64_t WriteDocumentMaskAliveVarintList(
  Directory&, IndexOutput& out, const DocumentMask& docs_mask) {
  out.WriteV32(DocumentMaskOnDiskFormat::AliveVarintList);
  const auto pos = out.Position();
  docs_mask.ForEachAlive([&out](doc_id_t doc_id) { out.WriteV32(doc_id); });
  return out.Position() - pos;
}

inline uint64_t WriteDocumentMaskDenseBitset(Directory& dir, IndexOutput& out,
                                             const DocumentMask& docs_mask) {
  out.WriteV32(DocumentMaskOnDiskFormat::DeletedDenseBitset);
  // TODO: consider manual bytes filling, rather than using ManagedBitset
  // TODO: decide on proper resource manager for the buffering bitset
  ManagedBitset deleted_docs(docs_mask.DocCount(),
                             *dir.ResourceManager().readers);
  docs_mask.ForEachDeleted([&deleted_docs](doc_id_t doc_id) {
    deleted_docs.set(doc_id - doc_limits::min());
  });
  const auto pos = out.Position();
  out.WriteBytes(reinterpret_cast<const byte_type*>(deleted_docs.data()),
                 deleted_docs.words() * sizeof(ManagedBitset::word_t));
  return out.Position() - pos;
}

inline uint64_t WriteDocumentMask(Directory& dir, IndexOutput& out,
                                  const DocumentMask* docs_mask) {
  uint32_t doc_count = docs_mask ? docs_mask->DocCount() : 0;
  uint32_t deleted_doc_count = docs_mask ? docs_mask->DeletedDocCount() : 0;
  SDB_ASSERT(deleted_doc_count < doc_limits::eof());
  out.WriteV32(doc_count);
  out.WriteV32(deleted_doc_count);
  if (!deleted_doc_count) {
    return 0;
  }

  if (deleted_doc_count < 0.04 * doc_count) {
    return WriteDocumentMaskDeletedVarintList(dir, out, *docs_mask);
  } else if (deleted_doc_count < 0.96 * doc_count) {
    return WriteDocumentMaskDenseBitset(dir, out, *docs_mask);
  } else {
    return WriteDocumentMaskAliveVarintList(dir, out, *docs_mask);
  }
}

inline void WriteStrings(IndexOutput& out, const auto& strings) {
  SDB_ASSERT(strings.size() < std::numeric_limits<uint32_t>::max());

  out.WriteV32(static_cast<uint32_t>(strings.size()));
  for (const auto& s : strings) {
    WriteStr(out, s);
  }
}

inline void SegmentMetaWriterImpl::write(Directory& dir, std::string& meta_file,
                                         SegmentMeta& meta) {
  if (meta.docs_count < meta.live_docs_count ||
      meta.docs_count - meta.live_docs_count != RemovalCount(meta))
    [[unlikely]] {
    throw IndexError{absl::StrCat("Invalid segment meta '", meta.name,
                                  "' detected : docs_count=", meta.docs_count,
                                  ", live_docs_count=", meta.live_docs_count)};
  }

  meta_file = FileName<SegmentMetaWriter>(meta);
  auto out = dir.create(meta_file);

  if (!out) [[unlikely]] {
    throw IoError{absl::StrCat("failed to create file, path: ", meta_file)};
  }

  SDB_ASSERT(meta.docs_mask_size <= meta.byte_size);
  const auto size_without_mask = meta.byte_size - meta.docs_mask_size;

  format_utils::WriteHeader(*out, kFormatName, _version);
  WriteStr(*out, meta.name);
  out->WriteV64(meta.version);
  out->WriteV32(meta.live_docs_count);
  const auto docs_mask_size = WriteDocumentMask(dir, *out, meta.docs_mask.get());
  out->WriteV64(size_without_mask);
  WriteStrings(*out, meta.files);
  format_utils::WriteFooter(*out);

  meta.byte_size = size_without_mask + docs_mask_size;
  meta.docs_mask_size = docs_mask_size;
}

}  // namespace irs
