
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
#include "iresearch/formats/segment_meta_writer.hpp"
#include "iresearch/store/store_utils.hpp"

namespace irs {

struct SegmentMetaReaderImpl : public SegmentMetaReader {
  void read(const Directory& dir, SegmentMeta& meta,
            std::string_view filename = {}) final;  // null == use meta
};

inline std::vector<std::string> ReadStrings(DataInput& in) {
  const size_t size = in.ReadV32();

  if (size > std::numeric_limits<uint32_t>::max()) [[unlikely]] {
    throw IoError{absl::StrCat("Too many strings to read: ", size)};
  }

  std::vector<std::string> strings(size);
  for (auto& s : strings) {
    s = ReadString<std::string>(in);
  }

  return strings;
}

inline std::pair<std::shared_ptr<DocumentMask>, uint64_t>
ReadDocumentMaskV0(DataInput& in, IResourceManager& rm, size_t live_docs_count) {
  auto count = in.ReadV32();

  if (!count) {
    return {};
  }

  auto docs_mask = std::make_shared<DocumentDeletedHashMask>(rm, live_docs_count + count, count);

  const auto pos = in.Position();
  while (count--) {
    static_assert(sizeof(doc_id_t) == sizeof(decltype(in.ReadV32())));

    docs_mask->Store(in.ReadV32());
  }

  return {std::move(docs_mask), in.Position() - pos};
}

inline std::pair<std::shared_ptr<DocumentMask>, uint64_t>
ReadDocumentMaskDeletedVarintList(DataInput& in, IResourceManager& rm,
                                  size_t doc_count, size_t deleted_doc_count) {
  auto pos = in.Position();
  DocumentDeletedHashMask docs_mask(rm, doc_count, deleted_doc_count);
  while (deleted_doc_count--) {
    static_assert(sizeof(doc_id_t) == sizeof(decltype(in.ReadV32())));
    docs_mask.Store(in.ReadV32());
  }
  return {std::make_shared<DocumentDeletedHashMask>(std::move(docs_mask)),
          in.Position() - pos};
}

inline std::pair<std::shared_ptr<DocumentMask>, uint64_t>
ReadDocumentMaskAliveVarintList(DataInput& in, IResourceManager& rm,
                                size_t doc_count, size_t deleted_doc_count) {
  auto pos = in.Position();
  DocumentAliveHashMask docs_mask(rm, doc_count, deleted_doc_count);
  auto alive_doc_count = doc_count - deleted_doc_count;
  while (alive_doc_count--) {
    static_assert(sizeof(doc_id_t) == sizeof(decltype(in.ReadV32())));
    docs_mask.Store(in.ReadV32());
  }
  return {std::make_shared<DocumentAliveHashMask>(std::move(docs_mask)),
          in.Position() - pos};
}

inline std::pair<std::shared_ptr<DocumentMask>, uint64_t>
ReadDocumentMaskDenseBitset(DataInput& in, IResourceManager& rm,
                            size_t doc_count, size_t deleted_doc_count) {
  auto pos = in.Position();
  DocumentBitMask bitset(rm, doc_count, deleted_doc_count);
  auto byte_count =
    ManagedBitset::bits_to_words(doc_count) * sizeof(ManagedBitset::word_t);
  for (size_t i = 0; i < byte_count; i++) {
    auto b = in.ReadByte();
    for (size_t j = 0; j < BitsRequired<decltype(b)>(); j++) {
      if ((b >> j) & 1) {
        bitset.MarkDeleted(i * BitsRequired<decltype(b)>() + j +
                           doc_limits::min());
      }
    }
  }
  return {std::make_shared<DocumentBitMask>(std::move(bitset)),
          in.Position() - pos};
}

inline std::pair<std::shared_ptr<DocumentMask>, uint64_t> ReadDocumentMask(
  DataInput& in, IResourceManager& rm) {
  auto doc_count = in.ReadV32();
  auto deleted_doc_count = in.ReadV32();
  if (!deleted_doc_count) {
    return {};
  }
  auto format = in.ReadV32();
  SDB_ASSERT(deleted_doc_count <= doc_count);
  auto chosen_kind =
    ChooseImmutableRepresentation(doc_count, deleted_doc_count);

  std::shared_ptr<DocumentMask> docs_mask;
  uint64_t mask_size;

  switch (format) {
    case DocumentMaskOnDiskFormat::DeletedVarintList:
      std::tie(docs_mask, mask_size) =
        ReadDocumentMaskDeletedVarintList(in, rm, doc_count, deleted_doc_count);
      break;
    case DocumentMaskOnDiskFormat::DeletedDenseBitset:
      std::tie(docs_mask, mask_size) =
        ReadDocumentMaskDenseBitset(in, rm, doc_count, deleted_doc_count);
      break;
    case DocumentMaskOnDiskFormat::AliveVarintList:
      std::tie(docs_mask, mask_size) =
        ReadDocumentMaskAliveVarintList(in, rm, doc_count, deleted_doc_count);
      break;
    default:
      throw IndexError{absl::StrCat("Unknown document mask format: ", format)};
  }
  return {MakeDocumentMask(rm, chosen_kind, std::move(*docs_mask)), mask_size};
}

inline void SegmentMetaReaderImpl::read(const Directory& dir, SegmentMeta& meta,
                                        std::string_view filename) {
  const std::string meta_file = IsNull(filename)
                                  ? FileName<SegmentMetaWriter>(meta)
                                  : std::string{filename};

  auto in = dir.open(meta_file, IOAdvice::SEQUENTIAL | IOAdvice::READONCE);

  if (!in) [[unlikely]] {
    throw IoError{absl::StrCat("Failed to open file, path: ", meta_file)};
  }

  const auto checksum = format_utils::Checksum(*in);

  const auto format_version = format_utils::CheckHeader(
    *in, SegmentMetaWriterImpl::kFormatName, SegmentMetaWriterImpl::kFormatMin,
    SegmentMetaWriterImpl::kFormatMax);
  auto name = ReadString<std::string>(*in);
  const auto segment_version = in->ReadV64();
  const auto live_docs_count = in->ReadV32();
  auto [docs_mask, docs_mask_size] = [&]() {
    if (format_version == 0) {
      return ReadDocumentMaskV0(*in, *dir.ResourceManager().readers, live_docs_count);
    } else {
      return ReadDocumentMask(*in, *dir.ResourceManager().readers);
    }
  }();
  const auto docs_count =
    live_docs_count + static_cast<doc_id_t>(docs_mask ? docs_mask->DeletedDocCount() : 0);
  const auto size = in->ReadV64();
  auto files = ReadStrings(*in);
  format_utils::CheckFooter(*in, checksum);

  if (docs_count < live_docs_count) [[unlikely]] {
    throw IndexError{absl::StrCat(
      "While reading segment meta '", name, "', error: docs_count(", docs_count,
      ") > live_docs_count(", live_docs_count, ")")};
  }

  // ...........................................................................
  // all operations below are noexcept
  // ...........................................................................

  meta.name = std::move(name);
  meta.version = segment_version;
  meta.docs_count = docs_count;
  meta.live_docs_count = live_docs_count;
  meta.docs_mask = std::move(docs_mask);
  meta.docs_mask_size = docs_mask_size;
  meta.byte_size = size + docs_mask_size;
  meta.files = std::move(files);
}

}  // namespace irs
