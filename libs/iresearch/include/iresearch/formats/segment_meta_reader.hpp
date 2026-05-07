
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

inline std::pair<DocumentMaskHandle, uint64_t>
ReadDocumentMaskV0(DataInput& in, IResourceManager& rm) {
  auto count = in.ReadV32();

  if (!count) {
    return {DocumentMaskHandle{.mask = nullptr, .kind = DocumentMaskKind::None}, 0};
  }

  auto docs_mask = std::make_shared<DocumentHashMask>(rm);
  docs_mask->HintDeletedDocCount(count);

  const auto pos = in.Position();
  while (count--) {
    static_assert(sizeof(doc_id_t) == sizeof(decltype(in.ReadV32())));

    docs_mask->MarkDeleted(in.ReadV32());
  }

  return {DocumentMaskHandle{.mask = std::move(docs_mask),
                             .kind = DocumentMaskKind::DeletedHashSet},
          in.Position() - pos};
}

#define CALL_READER(ReadFunction, document_mask_kind, ...)               \
  [&]() -> std::pair<DocumentMaskHandle, uint64_t> {                     \
    switch (document_mask_kind) {                                        \
      case DocumentMaskKind::None: {                                     \
        throw IllegalState{                                              \
          absl::StrCat("Bad choice of document mask format: None")};     \
      }                                                                  \
      case DocumentMaskKind::DeletedHashSet: {                           \
        auto [mask, size] = ReadFunction<DocumentHashMask>(__VA_ARGS__); \
        return {DocumentMaskHandle{.mask = std::move(mask),              \
                                   .kind = document_mask_kind},          \
                size};                                                   \
      }                                                                  \
      case DocumentMaskKind::DenseBitset: {                              \
        auto [mask, size] = ReadFunction<DocumentBitMask>(__VA_ARGS__);  \
        return {DocumentMaskHandle{.mask = std::move(mask),              \
                                   .kind = document_mask_kind},          \
                size};                                                   \
      }                                                                  \
    }                                                                    \
  }()

template<typename T>
concept HasMarkDeleted = requires(T t) { t.MarkDeleted(doc_id_t{1}); };

template<typename TDocumentMask>
std::pair<const std::shared_ptr<DocumentMask>, uint64_t>
ReadDocumentMaskDeletedVarintList(DataInput& in, IResourceManager& rm,
                                  size_t doc_count, size_t deleted_doc_count) {
  auto pos = in.Position();
  DocumentHashMask hash_mask(rm, doc_count, deleted_doc_count);
  while (deleted_doc_count--) {
    hash_mask.MarkDeleted(in.ReadV32());
  }
  return {std::make_shared<TDocumentMask>(rm, hash_mask), in.Position() - pos};
}

template<HasMarkDeleted TDocumentMask>
std::pair<const std::shared_ptr<DocumentMask>, uint64_t>
ReadDocumentMaskDeletedVarintList(DataInput& in, IResourceManager& rm,
                                  size_t doc_count, size_t deleted_doc_count) {
  auto pos = in.Position();
  auto docs_mask =
    std::make_shared<TDocumentMask>(rm, doc_count, deleted_doc_count);
  while (deleted_doc_count--) {
    docs_mask->MarkDeleted(in.ReadV32());
  }
  return {std::move(docs_mask), in.Position() - pos};
}

template<typename TDocumentMask>
std::pair<const std::shared_ptr<DocumentMask>, uint64_t>
ReadDocumentMaskDenseBitset(DataInput& in, IResourceManager& rm,
                            size_t doc_count, size_t deleted_doc_count) {
  auto pos = in.Position();
  DocumentBitMask bitset(rm, doc_count, deleted_doc_count);
  auto byte_count =
    ManagedBitset::bits_to_words(doc_count) * sizeof(ManagedBitset::word_t);
  for (size_t i = 0; i < byte_count; i++) {
    auto b = in.ReadByte();
    for (size_t j = 0; j < BitsRequired<decltype(b)>(); j++) {
      if (b & (1 << j)) {
        bitset.MarkDeleted(i * BitsRequired<decltype(b)>() + j +
                           doc_limits::min());
      }
    }
  }
  return {std::make_shared<TDocumentMask>(rm, bitset), in.Position() - pos};
}

template<HasMarkDeleted TDocumentMask>
std::pair<std::shared_ptr<DocumentMask>, uint64_t> ReadDocumentMaskDenseBitset(
  DataInput& in, IResourceManager& rm, size_t doc_count,
  size_t deleted_doc_count) {
  auto pos = in.Position();
  auto docs_mask =
    std::make_shared<TDocumentMask>(rm, doc_count, deleted_doc_count);
  auto byte_count =
    ManagedBitset::bits_to_words(doc_count) * sizeof(ManagedBitset::word_t);
  for (size_t i = 0; i < byte_count; i++) {
    auto b = in.ReadByte();
    for (size_t j = 0; j < BitsRequired<decltype(b)>(); j++) {
      if (b & (1 << j)) {
        docs_mask->MarkDeleted(i * BitsRequired<decltype(b)>() + j +
                               doc_limits::min());
      }
    }
  }
  return {std::move(docs_mask), in.Position() - pos};
}

inline std::pair<DocumentMaskHandle, uint64_t> ReadDocumentMask(
  DataInput& in, IResourceManager& rm) {
  auto doc_count = in.ReadV32();
  auto deleted_doc_count = in.ReadV32();
  if (!deleted_doc_count) {
    return {{.mask = nullptr, .kind = DocumentMaskKind::None}, 0};
  }
  auto format = in.ReadV32();
  SDB_ASSERT(deleted_doc_count <= doc_count);
  auto chosen_kind =
    ChooseImmutableRepresentation(doc_count, deleted_doc_count);

  switch (format) {
    case DocumentMaskOnDiskFormat::DeletedVarintList:
      return CALL_READER(ReadDocumentMaskDeletedVarintList, chosen_kind, in, rm, doc_count, deleted_doc_count);
    case DocumentMaskOnDiskFormat::DeletedDenseBitset:
      return CALL_READER(ReadDocumentMaskDenseBitset, chosen_kind, in, rm, doc_count, deleted_doc_count);
    default:
      throw IndexError{absl::StrCat("Unknown document mask format: ", format)};
  }
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
      return ReadDocumentMaskV0(*in, *dir.ResourceManager().readers);
    } else {
      return ReadDocumentMask(*in, *dir.ResourceManager().readers);
    }
  }();
  const auto docs_count =
    live_docs_count + static_cast<doc_id_t>(docs_mask.mask ? docs_mask.mask->DeletedDocCount() : 0);
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
