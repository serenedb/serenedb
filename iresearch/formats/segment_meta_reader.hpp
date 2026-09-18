
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

#include <span>

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

inline std::pair<std::shared_ptr<DocumentMask>, uint64_t> ReadDocumentMask(
  const Directory& dir, std::string_view segment, uint64_t last_version,
  uint32_t files, uint32_t count) {
  if (files == 0) {
    return {};
  }

  DocumentMaskBuilder builder;
  uint64_t bytes = 0;

  for (uint32_t i = 0; i < files; ++i) {
    const auto name = irs::FileName(segment, last_version - (files - 1) + i,
                                    DocsMaskWriter::kFormatExt);
    auto in = dir.open(name, IOAdvice::SEQUENTIAL | IOAdvice::READONCE);

    if (!in) [[unlikely]] {
      throw IoError{absl::StrCat("Failed to open file, path: ", name)};
    }

    const auto blob = ReadString<std::string>(*in);

    bytes += in->Length();
    builder.Merge(DocumentMask::Read(blob.data(), blob.size()));
  }

  auto docs_mask = std::make_shared<DocumentMask>(std::move(builder).Build());

  if (docs_mask->Count() != count) [[unlikely]] {
    throw IndexError{absl::StrCat("Corrupted document mask, expected ", count,
                                  " masked documents, got ",
                                  docs_mask->Count())};
  }

  return {std::move(docs_mask), bytes};
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

  format_utils::CheckHeader(*in, SegmentMetaWriterImpl::kFormatName,
                            SegmentMetaWriterImpl::kFormatVersion);
  auto name = ReadString<std::string>(*in);
  const auto segment_version = in->ReadV64();
  const auto live_docs_count = in->ReadV32();
  const auto mask_count = in->ReadV32();
  doc_id_t uncommitted_count = 0;
  uint32_t docs_mask_files = 0;
  std::string inline_mask;
  if (mask_count != 0) {
    uncommitted_count = in->ReadV32();
    docs_mask_files = in->ReadV32();
    if (docs_mask_files == 0 && mask_count != uncommitted_count) {
      inline_mask = ReadString<std::string>(*in);
    }
  }
  const auto docs_count = live_docs_count + mask_count;
  const auto uncommitted_begin =
    uncommitted_count == 0
      ? doc_limits::eof()
      : static_cast<doc_id_t>(docs_count + doc_limits::min() -
                              uncommitted_count);
  const auto size = in->ReadV64();
  auto files = ReadStrings(*in);
  format_utils::CheckFooter(*in, checksum);

  if (docs_mask_files > segment_version + 1) [[unlikely]] {
    throw IndexError{absl::StrCat("While reading segment meta '", name,
                                  "', error: docs_mask_files(", docs_mask_files,
                                  ") > version(", segment_version, ") + 1")};
  }

  SDB_ASSERT(mask_count >= uncommitted_count);
  const auto scattered_count = mask_count - uncommitted_count;
  auto [docs_mask, docs_mask_size] =
    inline_mask.empty()
      ? ReadDocumentMask(dir, name, segment_version, docs_mask_files,
                         scattered_count)
      : std::pair{std::make_shared<DocumentMask>(
                    DocumentMask::Read(inline_mask.data(), inline_mask.size())),
                  uint64_t{0}};

  if (!inline_mask.empty() && docs_mask->Count() != scattered_count)
    [[unlikely]] {
    throw IndexError{absl::StrCat("Corrupted document mask, expected ",
                                  scattered_count, " masked documents, got ",
                                  docs_mask->Count())};
  }

  files.reserve(files.size() + docs_mask_files);
  for (uint32_t i = 0; i < docs_mask_files; ++i) {
    files.emplace_back(
      irs::FileName(name, segment_version - (docs_mask_files - 1) + i,
                    DocsMaskWriter::kFormatExt));
  }

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
  meta.uncommitted_begin = uncommitted_begin;
  meta.docs_mask = std::move(docs_mask);
  meta.docs_mask_size = docs_mask_size;
  meta.docs_mask_files = docs_mask_files;
  meta.byte_size = size + docs_mask_size;
  meta.files = std::move(files);
}

}  // namespace irs
