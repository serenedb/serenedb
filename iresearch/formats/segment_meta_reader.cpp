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

#include "iresearch/formats/segment_meta_reader.hpp"

#include <absl/strings/str_cat.h>

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <vector>

#include "iresearch/error/error.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/segment_meta_writer.hpp"
#include "iresearch/index/document_mask.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/store/directory.hpp"

namespace irs::segment_meta {
namespace {

DocumentMask ReadDocumentMask(IndexInput& in, uint64_t mask_size) {
  if (const auto* data = in.ReadVolatile(0, mask_size)) {
    return DocumentMask::Read(reinterpret_cast<const char*>(data), mask_size);
  }
  bstring blob(mask_size, 0);
  in.ReadData(0, blob.data(), mask_size);
  return DocumentMask::Read(reinterpret_cast<const char*>(blob.data()),
                            blob.size());
}

bool ReadFiles(duckdb::BinaryDeserializer& meta_in,
               std::vector<std::string>& files) {
  return meta_in.ReadOptionalList(
    kFieldFiles, "files",
    [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      files.emplace_back(list.ReadElement<std::string>());
    });
}

std::vector<uint64_t> ReadParents(duckdb::BinaryDeserializer& meta_in) {
  std::vector<uint64_t> parents;
  meta_in.ReadOptionalList(
    kFieldParents, "parents",
    [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      parents.push_back(list.ReadElement<uint64_t>());
    });
  return parents;
}

format_utils::Footer ReadLink(IndexInput& in, std::string_view file,
                              std::vector<std::string>& files,
                              bool& has_files) {
  return format_utils::ReadFooter(
    in, file, [&](duckdb::BinaryDeserializer& meta_in, uint64_t) {
      ReadParents(meta_in);

      if (ReadFiles(meta_in, files)) {
        if (has_files) [[unlikely]] {
          throw IndexError{
            absl::StrCat("Corrupted document mask chain, path: ", file,
                         ", a second link carries the segment file list")};
        }
        has_files = true;
      }

      meta_in.ReadProperty<uint32_t>(kFieldDocsCount, "docs_count");
      meta_in.ReadProperty<uint64_t>(kFieldByteSize, "byte_size");
    });
}

}  // namespace

void Read(const Directory& dir, SegmentMeta& meta, std::string_view filename) {
  SDB_ASSERT(!IsNull(filename));

  std::string_view segment_name;
  uint64_t segment_version = 0;

  if (!ParseFileName(filename, kExt, segment_name, segment_version))
    [[unlikely]] {
    throw IndexError{
      absl::StrCat("Malformed segment meta file name: ", filename)};
  }

  std::string name{segment_name};

  auto in = dir.open(filename, IOAdvice::SEQUENTIAL);

  if (!in) [[unlikely]] {
    throw IoError{absl::StrCat("Failed to open file, path: ", filename)};
  }

  std::vector<uint64_t> parents;
  std::vector<std::string> files;
  bool has_files = false;
  uint32_t docs_count = 0;
  uint64_t size = 0;
  const auto footer = format_utils::ReadFooter(
    *in, filename, [&](duckdb::BinaryDeserializer& meta_in, uint64_t) {
      parents = ReadParents(meta_in);
      has_files = ReadFiles(meta_in, files);
      docs_count =
        meta_in.ReadProperty<uint32_t>(kFieldDocsCount, "docs_count");
      size = meta_in.ReadProperty<uint64_t>(kFieldByteSize, "byte_size");
    });
  const auto mask_size = footer.data_len;

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

      auto file = irs::FileName(name, link, kExt);

      auto mask_in = dir.open(file, IOAdvice::SEQUENTIAL);

      if (!mask_in) [[unlikely]] {
        throw IoError{absl::StrCat("Failed to open file, path: ", file)};
      }

      const auto link_footer = ReadLink(*mask_in, file, files, has_files);

      if (link_footer.data_len == 0) [[unlikely]] {
        throw IndexError{absl::StrCat("Corrupted document mask chain of '",
                                      name, "', maskless link: ", file)};
      }

      builder.Merge(ReadDocumentMask(*mask_in, link_footer.data_len));

      docs_mask_size += link_footer.data_len;
      ++docs_mask_chain;
      links.emplace_back(std::move(file));
    }

    files.insert(files.end(), std::make_move_iterator(links.begin()),
                 std::make_move_iterator(links.end()));
    builder.Trim();
    docs_mask = std::make_shared<DocumentMask>(std::move(builder));
  }

  if (!has_files) [[unlikely]] {
    throw IndexError{
      absl::StrCat("While reading segment meta '", name,
                   "', error: the segment file list is missing")};
  }

  const auto mask_count = docs_mask ? docs_mask->Count() : 0;

  if (docs_count >= doc_limits::eof() || mask_count > docs_count) [[unlikely]] {
    throw IndexError{absl::StrCat(
      "While reading segment meta '", name, "', error: docs_count(", docs_count,
      ") is out of range for ", mask_count, " masked document(s)")};
  }

  // all operations below are noexcept
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

}  // namespace irs::segment_meta
