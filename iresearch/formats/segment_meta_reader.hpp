
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

inline uint64_t ReadMaskSize(duckdb::Deserializer& meta_in) {
  return meta_in.ReadPropertyWithExplicitDefault<uint64_t>(
    SegmentMetaWriterImpl::kFieldMaskSize, "mask_size", 0);
}

inline void CheckMaskSize(uint64_t mask_size, uint64_t data_size,
                          std::string_view file) {
  if (mask_size != data_size) [[unlikely]] {
    throw IndexError{
      absl::StrCat("Corrupted segment meta, path: ", file, ", mask size(",
                   mask_size, ") differs from the data size(", data_size, ")")};
  }
}

inline DocumentMask ReadDocumentMask(IndexInput& in, uint64_t mask_size) {
  if (const auto* data = in.ReadVolatile(0, mask_size)) {
    return DocumentMask::Read(reinterpret_cast<const char*>(data), mask_size);
  }
  bstring blob(mask_size, 0);
  in.ReadData(0, blob.data(), mask_size);
  return DocumentMask::Read(reinterpret_cast<const char*>(blob.data()),
                            blob.size());
}

inline bool ReadFiles(duckdb::Deserializer& meta_in,
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

inline std::vector<uint64_t> ReadParents(duckdb::Deserializer& meta_in) {
  std::vector<uint64_t> parents;
  meta_in.ReadOptionalList(
    SegmentMetaWriterImpl::kFieldParents, "parents",
    [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      parents.push_back(list.ReadElement<uint64_t>());
    });
  return parents;
}

inline uint64_t ReadLink(IndexInput& in, std::string_view file,
                         std::vector<std::string>& files, bool& has_files) {
  uint64_t mask_size = 0;
  const auto footer = format_utils::ReadFooter(
    in, file, [&](duckdb::Deserializer& meta_in, uint64_t) {
      mask_size = ReadMaskSize(meta_in);
      ReadParents(meta_in);

      if (ReadFiles(meta_in, files)) {
        if (has_files) [[unlikely]] {
          throw IndexError{
            absl::StrCat("Corrupted document mask chain, path: ", file,
                         ", a second link carries the segment file list")};
        }
        has_files = true;
      }

      meta_in.ReadProperty<uint32_t>(SegmentMetaWriterImpl::kFieldDocsCount,
                                     "docs_count");
      meta_in.ReadProperty<uint64_t>(SegmentMetaWriterImpl::kFieldByteSize,
                                     "byte_size");
    });
  CheckMaskSize(mask_size, footer.data_len, file);

  return mask_size;
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

  uint64_t mask_size = 0;
  std::vector<uint64_t> parents;
  std::vector<std::string> files;
  bool has_files = false;
  uint32_t docs_count = 0;
  uint64_t size = 0;
  const auto footer = format_utils::ReadFooter(
    *in, filename, [&](duckdb::Deserializer& meta_in, uint64_t) {
      mask_size = ReadMaskSize(meta_in);
      parents = ReadParents(meta_in);
      has_files = ReadFiles(meta_in, files);
      docs_count = meta_in.ReadProperty<uint32_t>(
        SegmentMetaWriterImpl::kFieldDocsCount, "docs_count");
      size = meta_in.ReadProperty<uint64_t>(
        SegmentMetaWriterImpl::kFieldByteSize, "byte_size");
    });
  CheckMaskSize(mask_size, footer.data_len, filename);

  if (mask_size == 0 && !parents.empty()) [[unlikely]] {
    throw IndexError{absl::StrCat("Corrupted document mask chain of '", name,
                                  "', maskless head derives from ",
                                  parents.size(), " link(s)")};
  }

  std::shared_ptr<DocumentMask> docs_mask;
  uint64_t docs_mask_size = 0;
  uint32_t docs_mask_chain = 0;

  if (mask_size != 0) {
    auto builder = ReadDocumentMask(*in, mask_size);
    docs_mask_size = mask_size;
    docs_mask_chain = 1;

    std::vector<std::string> links;
    links.reserve(parents.size());

    for (uint64_t floor = 0; const auto link : parents) {
      if (link < floor || link >= segment_version) [[unlikely]] {
        throw IndexError{absl::StrCat(
          "Corrupted document mask chain of '", name, "', version(",
          segment_version, ") derives from ", link, " out of order")};
      }
      floor = link + 1;

      auto file = irs::FileName(name, link, SegmentMetaWriterImpl::kFormatExt);

      auto mask_in = dir.open(file, IOAdvice::SEQUENTIAL);

      if (!mask_in) [[unlikely]] {
        throw IoError{absl::StrCat("Failed to open file, path: ", file)};
      }

      const auto link_size = ReadLink(*mask_in, file, files, has_files);

      if (link_size == 0) [[unlikely]] {
        throw IndexError{absl::StrCat("Corrupted document mask chain of '",
                                      name, "', maskless link: ", file)};
      }

      builder.Merge(ReadDocumentMask(*mask_in, link_size));

      docs_mask_size += link_size;
      ++docs_mask_chain;
      links.emplace_back(std::move(file));
    }

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
