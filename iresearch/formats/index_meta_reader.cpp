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

#include "iresearch/formats/index_meta_reader.hpp"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <vector>

#include "iresearch/error/error.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/index_meta_writer.hpp"
#include "iresearch/formats/segment_meta_reader.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/serialization.hpp"

namespace irs::index_meta {

uint64_t ParseGeneration(std::string_view file) noexcept {
  uint64_t gen;
  if (absl::ConsumePrefix(&file, kPrefix) && absl::SimpleAtoi(file, &gen)) {
    return gen;
  }
  return index_gen_limits::invalid();
}

bool LastFile(const Directory& dir, std::string& out) {
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

void Read(const Directory& dir, IndexMeta& meta, std::string_view filename,
          MetaPayloadReader payload) {
  SDB_ASSERT(!IsNull(filename));

  // Every caller names a file that LastFile already parsed.
  const auto gen = ParseGeneration(filename);
  SDB_ASSERT(index_gen_limits::valid(gen));

  auto in = dir.open(filename, IOAdvice::SEQUENTIAL | IOAdvice::READONCE);

  if (!in) {
    throw IoError{absl::StrCat("Failed to open file, path: ", filename)};
  }

  uint64_t cnt = 0;
  std::vector<IndexSegment> segments;
  std::vector<uint32_t> invisible;
  format_utils::ReadFooter(
    *in, filename, [&](duckdb::BinaryDeserializer& meta_in, uint64_t) {
      const auto version =
        meta_in.ReadProperty<uint64_t>(kFieldStorageVersion, "storage_version");
      if (version != static_cast<uint64_t>(duckdb::kIResearchStorageVersion))
        [[unlikely]] {
        throw IndexError{absl::StrCat(
          "Index meta '", filename, "' has storage version ", version,
          ", this build reads storage version ",
          static_cast<uint64_t>(duckdb::kIResearchStorageVersion))};
      }
      cnt = meta_in.ReadProperty<uint64_t>(kFieldSegCounter, "seg_counter");
      meta_in.ReadList(
        kFieldSegments, "segments",
        [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
          auto& segment = segments.emplace_back();
          auto& invisible_count = invisible.emplace_back();
          list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
            segment.filename =
              obj.ReadProperty<std::string>(kSegmentFieldFilename, "filename");
            invisible_count = obj.ReadPropertyWithExplicitDefault<uint32_t>(
              kSegmentFieldInvisibleCount, "invisible_count", 0);
          });
        });
      const bool has_payload =
        meta_in.OnOptionalPropertyBegin(kFieldPayload, "payload");
      if (has_payload) {
        if (!payload) [[unlikely]] {
          throw IndexError{absl::StrCat(
            "Index meta '", filename, "' has a payload but no reader for it")};
        }
        meta_in.OnObjectBegin();
        payload(meta_in);
        meta_in.OnObjectEnd();
      }
      meta_in.OnOptionalPropertyEnd(has_payload);
    });

  for (size_t i = 0; auto& segment : segments) {
    segment_meta::Read(dir, segment.meta, segment.filename);

    if (const auto count = invisible[i++]; count != 0) {
      auto& info = segment.meta;
      if (count > info.live_docs_count) [[unlikely]] {
        throw IndexError{
          absl::StrCat("Segment '", segment.filename, "' has invisible_count(",
                       count, ") above live_docs_count(", info.live_docs_count,
                       "), path: ", filename)};
      }
      info.live_docs_count -= count;
      info.visible_end =
        static_cast<doc_id_t>(doc_limits::min() + info.docs_count - count);
    }
  }

  meta.gen = gen;
  meta.seg_counter = cnt;
  meta.segments = std::move(segments);
}

}  // namespace irs::index_meta
