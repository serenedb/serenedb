
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

inline std::pair<const std::shared_ptr<DocumentMask>, uint64_t>
ReadDocumentMask(DataInput& in, IResourceManager& rm) {
  auto count = in.ReadV32();

  if (!count) {
    return {};
  }

  auto docs_mask = std::make_shared<DocumentMask>(rm);
  docs_mask->reserve(count);

  const auto pos = in.Position();
  while (count--) {
    static_assert(sizeof(doc_id_t) == sizeof(decltype(in.ReadV32())));

    docs_mask->insert(in.ReadV32());
  }

  return {std::move(docs_mask), in.Position() - pos};
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

  std::ignore = format_utils::CheckHeader(
    *in, SegmentMetaWriterImpl::kFormatName, SegmentMetaWriterImpl::kFormatMin,
    SegmentMetaWriterImpl::kFormatMax);
  auto name = ReadString<std::string>(*in);
  const auto segment_version = in->ReadV64();
  const auto live_docs_count = in->ReadV32();
  auto [docs_mask, docs_mask_size] =
    ReadDocumentMask(*in, *dir.ResourceManager().readers);
  const auto docs_count =
    live_docs_count + static_cast<doc_id_t>(docs_mask ? docs_mask->size() : 0);
  const auto size = in->ReadV64();
  const auto flags = in->ReadByte();
  field_id sort = in->ReadV64() - 1;
  auto files = ReadStrings(*in);
  format_utils::CheckFooter(*in, checksum);

  if (docs_count < live_docs_count) [[unlikely]] {
    throw IndexError{absl::StrCat(
      "While reading segment meta '", name, "', error: docs_count(", docs_count,
      ") > live_docs_count(", live_docs_count, ")")};
  }

  if (flags & ~(SegmentMetaWriterImpl::kHasColumnStore |
                SegmentMetaWriterImpl::kSorted)) [[unlikely]] {
    throw IndexError{absl::StrCat("While reading segment meta '", name,
                                  "', error: use of unsupported flags '", flags,
                                  "'")};
  }

  const auto sorted = bool(flags & SegmentMetaWriterImpl::kSorted);

  if ((!field_limits::valid(sort)) && sorted) [[unlikely]] {
    throw IndexError{absl::StrCat("While reading segment meta '", name,
                                  "', error: incorrectly marked as sorted")};
  }

  if ((field_limits::valid(sort)) && !sorted) [[unlikely]] {
    throw IndexError{absl::StrCat("While reading segment meta '", name,
                                  "', error: incorrectly marked as unsorted")};
  }

  // ...........................................................................
  // all operations below are noexcept
  // ...........................................................................

  meta.name = std::move(name);
  meta.version = segment_version;
  meta.column_store = flags & SegmentMetaWriterImpl::kHasColumnStore;
  meta.docs_count = docs_count;
  meta.live_docs_count = live_docs_count;
  meta.docs_mask = std::move(docs_mask);
  meta.sort = sort;
  meta.docs_mask_size = docs_mask_size;
  meta.byte_size = size + docs_mask_size;
  meta.files = std::move(files);
}

}  // namespace irs
