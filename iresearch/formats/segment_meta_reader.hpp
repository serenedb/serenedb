
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

#include <algorithm>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <vector>

#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/segment_meta_writer.hpp"

namespace irs {

struct SegmentMetaReaderImpl : public SegmentMetaReader {
  void read(const Directory& dir, SegmentMeta& meta,
            std::string_view filename) final;
};

inline uint64_t ReadMaskSize(IndexInput& in, std::string_view file) {
  const auto length = in.Length();

  if (length <= sizeof(uint64_t)) [[unlikely]] {
    throw IndexError{absl::StrCat("Truncated segment meta of ", length,
                                  " byte(s), path: ", file)};
  }

  in.Seek(length - sizeof(uint64_t));
  const auto mask_size = static_cast<uint64_t>(in.ReadI64());

  if (mask_size >= length - sizeof(uint64_t)) [[unlikely]] {
    throw IndexError{absl::StrCat(
      "Corrupted segment meta, path: ", file, ", mask size(", mask_size,
      ") leaves no metadata in a file of ", length, " byte(s)")};
  }

  return mask_size;
}

inline DocumentMask ReadDocumentMask(IndexInput& in, uint64_t mask_size) {
  in.Seek(0);
  bstring blob(mask_size, 0);
  in.ReadData(blob.data(), mask_size);
  return DocumentMask::Read(reinterpret_cast<const char*>(blob.data()),
                            blob.size());
}

inline bool ReadFiles(duckdb::BinaryDeserializer& meta_in,
                      std::vector<std::string>& files) {
  bool present = false;
  meta_in.ReadOptionalList(
    SegmentMetaWriterImpl::kFieldFiles, "files",
    [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      present = true;
      files.emplace_back(list.ReadElement<std::string>());
    });
  return present;
}

inline uint64_t ReadLink(IndexInput& in, uint64_t mask_size,
                         std::string_view file, std::vector<std::string>& files,
                         bool& has_files) {
  in.Seek(mask_size);
  duckdb::BinaryDeserializer meta_in{in};
  meta_in.Begin();
  const auto parent = meta_in.ReadPropertyWithExplicitDefault<uint64_t>(
    SegmentMetaWriterImpl::kFieldParent, "parent",
    SegmentMetaWriterImpl::kNoParent);

  if (ReadFiles(meta_in, files)) {
    if (has_files) [[unlikely]] {
      throw IndexError{
        absl::StrCat("Corrupted document mask chain, path: ", file,
                     ", a second link carries the segment file list")};
    }
    has_files = true;
  }

  return parent;
}

inline void SegmentMetaReaderImpl::read(const Directory& dir, SegmentMeta& meta,
                                        std::string_view filename) {
  SDB_ASSERT(!IsNull(filename));

  std::string_view segment_name;
  uint64_t segment_version = 0;

  if (!ParseFileName(filename, SegmentMetaWriterImpl::kFormatExt, segment_name,
                     segment_version)) [[unlikely]] {
    throw IndexError{
      absl::StrCat("Malformed segment meta file name: ", filename)};
  }

  std::string name{segment_name};

  auto in = dir.open(filename, IOAdvice::SEQUENTIAL);

  if (!in) [[unlikely]] {
    throw IoError{absl::StrCat("Failed to open file, path: ", filename)};
  }

  const auto mask_size = ReadMaskSize(*in, filename);

  in->Seek(mask_size);

  duckdb::BinaryDeserializer meta_in{*in};
  meta_in.Begin();
  const auto parent = meta_in.ReadPropertyWithExplicitDefault<uint64_t>(
    SegmentMetaWriterImpl::kFieldParent, "parent",
    SegmentMetaWriterImpl::kNoParent);
  std::vector<std::string> files;
  bool has_files = ReadFiles(meta_in, files);
  const auto docs_count = meta_in.ReadProperty<uint32_t>(
    SegmentMetaWriterImpl::kFieldDocsCount, "docs_count");
  const auto size = meta_in.ReadProperty<uint64_t>(
    SegmentMetaWriterImpl::kFieldByteSize, "byte_size");

  if (mask_size == 0 && parent != SegmentMetaWriterImpl::kNoParent)
    [[unlikely]] {
    throw IndexError{absl::StrCat("Corrupted document mask chain of '", name,
                                  "', maskless head links to ", parent)};
  }

  std::shared_ptr<DocumentMask> docs_mask;
  uint64_t docs_mask_size = 0;
  uint32_t docs_mask_chain = 0;

  if (mask_size != 0) {
    auto builder = ReadDocumentMask(*in, mask_size);
    docs_mask_size = mask_size;
    docs_mask_chain = 1;

    std::vector<std::string> links;
    auto version = segment_version;
    auto link = parent;

    while (link != SegmentMetaWriterImpl::kNoParent) {
      if (link >= version) [[unlikely]] {
        throw IndexError{absl::StrCat(
          "Corrupted document mask chain of '", name, "', ", docs_mask_chain,
          " link(s) deep at version(", version, ") links to ", link)};
      }

      auto file = irs::FileName(name, link, SegmentMetaWriterImpl::kFormatExt);

      auto mask_in = dir.open(file, IOAdvice::SEQUENTIAL);

      if (!mask_in) [[unlikely]] {
        throw IoError{absl::StrCat("Failed to open file, path: ", file)};
      }

      const auto link_size = ReadMaskSize(*mask_in, file);

      if (link_size == 0) [[unlikely]] {
        throw IndexError{absl::StrCat("Corrupted document mask chain of '",
                                      name, "', maskless link: ", file)};
      }

      const auto next = ReadLink(*mask_in, link_size, file, files, has_files);
      builder.Merge(ReadDocumentMask(*mask_in, link_size));

      docs_mask_size += link_size;
      ++docs_mask_chain;
      links.emplace_back(std::move(file));
      version = link;
      link = next;
    }

    std::reverse(links.begin(), links.end());
    files.insert(files.end(), std::make_move_iterator(links.begin()),
                 std::make_move_iterator(links.end()));
    builder.Trim();
    docs_mask = std::make_shared<DocumentMask>(std::move(builder));
  }

  const auto mask_count = docs_mask ? docs_mask->Count() : 0;

  if (docs_count >= doc_limits::eof() || mask_count > docs_count) [[unlikely]] {
    throw IndexError{absl::StrCat(
      "While reading segment meta '", name, "', error: docs_count(", docs_count,
      ") is out of range for ", mask_count, " masked document(s)")};
  }

  // ...........................................................................
  // all operations below are noexcept
  // ...........................................................................

  meta.name = std::move(name);
  meta.version = segment_version;
  meta.docs_count = docs_count;
  meta.live_docs_count = static_cast<uint32_t>(docs_count - mask_count);
  meta.visible_end = doc_limits::eof();
  meta.docs_mask = std::move(docs_mask);
  meta.docs_mask_size = docs_mask_size;
  meta.docs_mask_chain = docs_mask_chain;
  meta.byte_size = size + docs_mask_size;
  meta.files = std::move(files);
}

}  // namespace irs
