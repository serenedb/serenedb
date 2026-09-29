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

#include "iresearch/formats/index_meta_writer.hpp"

#include <absl/strings/str_cat.h>

#include <duckdb/common/serializer/binary_serializer.hpp>

#include "iresearch/error/error.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/log.hpp"
#include "iresearch/utils/serialization.hpp"

namespace irs {
namespace {

std::string PendingFileName(uint64_t gen) {
  SDB_ASSERT(index_gen_limits::valid(gen));
  return FileName(index_meta::kPendingPrefix, gen);
}

}  // namespace

std::string index_meta::FileName(uint64_t gen) {
  SDB_ASSERT(index_gen_limits::valid(gen));
  return irs::FileName(kPrefix, gen);
}

bool IndexMetaWriter::prepare(Directory& dir, IndexMeta& meta,
                              std::string& pending_filename,
                              std::string& filename, uint64_t tick) {
  if (index_gen_limits::valid(_pending_gen)) {
    // prepare() was already called with no corresponding call to commit()
    return false;
  }

  ++meta.gen;  // Increment generation before generating filename
  pending_filename = PendingFileName(meta.gen);
  filename = index_meta::FileName(meta.gen);

  auto out = dir.create(pending_filename);

  if (!out) {
    throw IoError{
      absl::StrCat("Failed to create file, path: ", pending_filename)};
  }

  format_utils::WriteFooter(*out, [&](duckdb::BinarySerializer& meta_out) {
    meta_out.WriteProperty<uint64_t>(
      index_meta::kFieldStorageVersion, "storage_version",
      static_cast<uint64_t>(duckdb::StorageVersion::SERENEDB_LATEST));
    meta_out.WriteProperty<uint64_t>(index_meta::kFieldSegCounter,
                                     "seg_counter", meta.seg_counter);
    meta_out.WriteList(
      index_meta::kFieldSegments, "segments", meta.segments.size(),
      [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
        const auto& segment = meta.segments[i];
        list.WriteObject([&](duckdb::BinarySerializer& obj) {
          obj.WriteProperty<std::string>(index_meta::kSegmentFieldFilename,
                                         "filename", segment.filename);
          obj.WritePropertyWithDefault<uint32_t>(
            index_meta::kSegmentFieldInvisibleCount, "invisible_count",
            InvisibleCount(segment.meta), 0);
        });
      });
    if (_payload) {
      meta_out.WriteObject(
        index_meta::kFieldPayload, "payload",
        [&](duckdb::BinarySerializer& obj) { _payload(tick, obj); });
    }
  });

  // Only noexcept operations below
  _dir = &dir;
  _pending_gen = meta.gen;

  return true;
}

bool IndexMetaWriter::commit() {
  if (!index_gen_limits::valid(_pending_gen)) {
    return false;
  }

  const auto src = PendingFileName(_pending_gen);
  const auto dst = index_meta::FileName(_pending_gen);

  if (!_dir->rename(src, dst)) {
    rollback();

    throw IoError{absl::StrCat("Failed to rename file, src path: '", src,
                               "' dst path: '", dst, "'")};
  }

  // only noexcept operations below
  // clear pending state
  _pending_gen = index_gen_limits::invalid();
  _dir = nullptr;

  return true;
}

void IndexMetaWriter::rollback() noexcept {
  if (!index_gen_limits::valid(_pending_gen)) {
    return;
  }

  std::string seg_file;

  try {
    seg_file = PendingFileName(_pending_gen);
  } catch (const std::exception& e) {
    SDB_ERROR(
      IRESEARCH,
      absl::StrCat(
        "Caught error while generating file name for index meta, reason: ",
        e.what()));
    return;
  } catch (...) {
    SDB_ERROR(IRESEARCH,
              "Caught error while generating file name for index meta");
    return;
  }

  if (!_dir->remove(seg_file)) {  // suppress all errors
    SDB_ERROR(IRESEARCH,
              absl::StrCat("Failed to remove file, path: ", seg_file));
  }

  // clear pending state
  _dir = nullptr;
  _pending_gen = index_gen_limits::invalid();
}

}  // namespace irs
