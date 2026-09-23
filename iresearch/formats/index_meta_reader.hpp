
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

#include <absl/strings/numbers.h>
#include <absl/strings/strip.h>

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <optional>
#include <vector>

#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/index_meta_writer.hpp"

namespace irs {

struct IndexMetaReaderImpl : public IndexMetaReader {
  bool last_segments_file(const Directory& dir, std::string& name) const final;

  void read(const Directory& dir, IndexMeta& meta,
            std::string_view filename) final;
};

inline uint64_t ParseGeneration(std::string_view file) noexcept {
  uint64_t gen;
  if (absl::ConsumePrefix(&file, IndexMetaWriterImpl::kFormatPrefix) &&
      absl::SimpleAtoi(file, &gen)) {
    return gen;
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
  SDB_ASSERT(!IsNull(filename));

  // Every caller names a file that last_segments_file already parsed.
  const auto gen = ParseGeneration(filename);
  SDB_ASSERT(index_gen_limits::valid(gen));

  auto in = dir.open(filename, IOAdvice::SEQUENTIAL | IOAdvice::READONCE);

  if (!in) {
    throw IoError{absl::StrCat("Failed to open file, path: ", filename)};
  }

  duckdb::BinaryDeserializer meta_in{*in};
  meta_in.Begin();
  const auto cnt = meta_in.ReadProperty<uint64_t>(
    IndexMetaWriterImpl::kFieldSegCounter, "seg_counter");
  std::vector<IndexSegment> segments;
  meta_in.ReadList(
    IndexMetaWriterImpl::kFieldSegments, "segments",
    [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      auto& segment = segments.emplace_back();
      list.ReadObject([&](duckdb::Deserializer& obj) {
        segment.filename = obj.ReadProperty<std::string>(
          IndexMetaWriterImpl::kSegmentFieldFilename, "filename");
        auto codec = obj.ReadProperty<std::string>(
          IndexMetaWriterImpl::kSegmentFieldCodec, "codec");
        segment.meta.codec = formats::Get(codec);

        if (!segment.meta.codec) [[unlikely]] {
          throw IndexError{absl::StrCat("Unknown codec '", codec,
                                        "' of segment '", segment.filename,
                                        "', path: ", filename)};
        }
      });
    });
  std::optional<bstring> payload;
  if (meta_in.CanDeserializeProperty(IndexMetaWriterImpl::kFieldPayload,
                                     "payload")) {
    auto& bytes = payload.emplace();
    meta_in.ReadList(
      IndexMetaWriterImpl::kFieldPayload, "payload",
      [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
        bytes.push_back(static_cast<byte_type>(list.ReadElement<char>()));
      });
  }

  for (auto& segment : segments) {
    auto reader = segment.meta.codec->get_segment_meta_reader();

    reader->read(dir, segment.meta, segment.filename);
  }

  meta.gen = gen;
  meta.seg_counter = cnt;
  meta.segments = std::move(segments);
  meta.payload = std::move(payload);
}

}  // namespace irs
