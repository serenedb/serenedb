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
namespace {

uint64_t ParseGeneration(std::string_view file,
                         std::string_view prefix) noexcept {
  uint64_t gen;
  if (absl::ConsumePrefix(&file, prefix) && absl::SimpleAtoi(file, &gen)) {
    return gen;
  }
  return index_gen_limits::invalid();
}

bool LastFile(const Directory& dir, std::string_view prefix, std::string& out) {
  uint64_t max_gen = index_gen_limits::invalid();
  Directory::visitor_f visitor = [&](std::string_view name) {
    const uint64_t gen = ParseGeneration(name, prefix);

    if (gen > max_gen) {
      out = name;
      max_gen = gen;
    }
    return true;  // continue iteration
  };

  dir.visit(visitor);
  return index_gen_limits::valid(max_gen);
}

uint64_t ReadFile(const Directory& dir, std::string_view filename,
                  MetaPayloadReader& payload,
                  std::vector<IndexSegment>* segments) {
  auto in = dir.open(filename, IOAdvice::SEQUENTIAL | IOAdvice::READONCE);

  if (!in) {
    throw IoError{absl::StrCat("Failed to open file, path: ", filename)};
  }

  uint64_t cnt = 0;
  format_utils::ReadFooter(
    *in, filename, [&](duckdb::BinaryDeserializer& meta_in, uint64_t) {
      const auto version =
        meta_in.ReadProperty<uint64_t>(kFieldStorageVersion, "storage_version");
      if (const auto error = duckdb::StorageVersionError(version);
          !error.empty()) [[unlikely]] {
        throw IndexError{absl::StrCat("Index meta '", filename,
                                      "' has storage version ", version,
                                      " and cannot be read: ", error)};
      }
      cnt = meta_in.ReadProperty<uint64_t>(kFieldSegCounter, "seg_counter");
      meta_in.ReadOptionalList(
        kFieldSegments, "segments",
        [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
          list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
            auto name =
              obj.ReadProperty<std::string>(kSegmentFieldFilename, "filename");
            if (segments != nullptr) {
              segments->emplace_back().filename = std::move(name);
            }
          });
        });
      meta_in.ReadOptionalObject(
        kFieldPayload, "payload", [&](duckdb::BinaryDeserializer& obj) {
          if (!payload) [[unlikely]] {
            throw IndexError{
              absl::StrCat("Index meta '", filename,
                           "' has a payload but no reader for it")};
          }
          payload(obj);
        });
    });
  return cnt;
}

}  // namespace

uint64_t ParseGeneration(std::string_view file) noexcept {
  return ParseGeneration(file, kPrefix);
}

uint64_t ParsePendingGeneration(std::string_view file) noexcept {
  return ParseGeneration(file, kPendingPrefix);
}

bool LastFile(const Directory& dir, std::string& out) {
  return LastFile(dir, kPrefix, out);
}

bool LastPendingFile(const Directory& dir, std::string& out) {
  return LastFile(dir, kPendingPrefix, out);
}

void Read(const Directory& dir, IndexMeta& meta, std::string_view filename,
          MetaPayloadReader payload) {
  SDB_ASSERT(!IsNull(filename));

  // Every caller names a file that LastFile already parsed.
  const auto gen = ParseGeneration(filename);
  SDB_ASSERT(index_gen_limits::valid(gen));

  std::vector<IndexSegment> segments;
  const auto cnt = ReadFile(dir, filename, payload, &segments);

  for (auto& segment : segments) {
    segment_meta::Read(dir, segment.meta, segment.filename);
  }

  meta.gen = gen;
  meta.seg_counter = cnt;
  meta.segments = std::move(segments);
}

void ReadPayload(const Directory& dir, std::string_view filename,
                 MetaPayloadReader payload) {
  std::ignore = ReadFile(dir, filename, payload, nullptr);
}

}  // namespace irs::index_meta
