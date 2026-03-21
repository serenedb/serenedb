
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

#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/index_meta_writer.hpp"

namespace irs {

struct IndexMetaReaderImpl : public IndexMetaReader {
  bool last_segments_file(const Directory& dir, std::string& name) const final;

  void read(const Directory& dir, IndexMeta& meta,
            std::string_view filename) final;
};

inline uint64_t ParseGeneration(std::string_view file) noexcept {
  if (file.starts_with(IndexMetaWriterImpl::kFormatPrefix)) {
    constexpr size_t kPrefixLength = IndexMetaWriterImpl::kFormatPrefix.size();

    if (uint64_t gen; absl::SimpleAtoi(file.substr(kPrefixLength), &gen)) {
      return gen;
    }
  }

  return index_gen_limits::invalid();
}

inline bool IndexMetaReaderImpl::last_segments_file(const Directory& dir,
                                                    std::string& out) const {
  uint64_t max_gen = index_gen_limits::invalid();
  Directory::visitor_f visitor = [&out, &max_gen](std::string_view name) {
    const uint64_t gen = ParseGeneration(name);

    if (gen > max_gen) {
      out = std::move(name);
      max_gen = gen;
    }
    return true;  // continue iteration
  };

  dir.visit(visitor);
  return index_gen_limits::valid(max_gen);
}

inline void IndexMetaReaderImpl::read(const Directory& dir, IndexMeta& meta,
                                      std::string_view filename) {
  std::string meta_file;
  if (IsNull(filename)) {
    meta_file = IndexMetaWriterImpl::FileName(meta.gen);
    filename = meta_file;
  }

  auto in = dir.open(filename, IOAdvice::SEQUENTIAL | IOAdvice::READONCE);

  if (!in) {
    throw IoError{absl::StrCat("Failed to open file, path: ", filename)};
  }

  const auto checksum = format_utils::Checksum(*in);

  // check header
  [[maybe_unused]] const int32_t version = format_utils::CheckHeader(
    *in, IndexMetaWriterImpl::kFormatName, IndexMetaWriterImpl::kFormatMin,
    IndexMetaWriterImpl::kFormatMax);

  // read data from segments file
  auto gen = in->ReadV64();
  auto cnt = in->ReadI64();
  auto seg_count = in->ReadV32();
  std::vector<IndexSegment> segments(seg_count);

  for (size_t i = 0, count = segments.size(); i < count; ++i) {
    auto& segment = segments[i];

    segment.filename = ReadString<std::string>(*in);
    segment.meta.codec = formats::Get(ReadString<std::string>(*in));

    auto reader = segment.meta.codec->get_segment_meta_reader();

    reader->read(dir, segment.meta, segment.filename);
  }

  bstring payload;
  const bool has_payload = in->ReadByte() & IndexMetaWriterImpl::kHasPayload;

  if (has_payload) {
    payload = ReadString<bstring>(*in);
  }

  format_utils::CheckFooter(*in, checksum);

  meta.gen = gen;
  meta.seg_counter = cnt;
  meta.segments = std::move(segments);
  if (has_payload) {
    meta.payload = std::move(payload);
  } else {
    meta.payload.reset();
  }
}

}  // namespace irs
