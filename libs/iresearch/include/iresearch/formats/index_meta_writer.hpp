
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
#include "iresearch/index/file_names.hpp"
#include "iresearch/store/store_utils.hpp"

namespace irs {

struct IndexMetaWriterImpl final : public IndexMetaWriter {
  static constexpr std::string_view kFormatName = "iresearch_10_index_meta";
  static constexpr std::string_view kFormatPrefix = "segments_";
  static constexpr std::string_view kFormatPrefixTmp = "pending_segments_";

  static constexpr int32_t kFormatMin = 0;
  static constexpr int32_t kFormatMax = 0;

  enum { kHasPayload = 1 };

  static std::string FileName(uint64_t gen) {
    return FileName(kFormatPrefix, gen);
  }

  explicit IndexMetaWriterImpl(int32_t version) noexcept : _version{version} {
    SDB_ASSERT(_version >= kFormatMin && version <= kFormatMax);
  }

  // FIXME(gnusi): Better to split prepare into 2 methods and pass meta by
  // const reference
  bool prepare(Directory& dir, IndexMeta& meta, std::string& pending_filename,
               std::string& filename) final;
  bool commit() final;
  void rollback() noexcept final;

 private:
  static std::string FileName(std::string_view prefix, uint64_t gen) {
    SDB_ASSERT(index_gen_limits::valid(gen));
    return irs::FileName(prefix, gen);
  }

  static std::string PendingFileName(uint64_t gen) {
    return FileName(kFormatPrefixTmp, gen);
  }

  Directory* _dir{};
  uint64_t _pending_gen{index_gen_limits::invalid()};  // Generation to commit
  int32_t _version;
};

inline bool IndexMetaWriterImpl::prepare(Directory& dir, IndexMeta& meta,
                                         std::string& pending_filename,
                                         std::string& filename) {
  if (index_gen_limits::valid(_pending_gen)) {
    // prepare() was already called with no corresponding call to commit()
    return false;
  }

  ++meta.gen;  // Increment generation before generating filename
  pending_filename = PendingFileName(meta.gen);
  filename = FileName(meta.gen);

  auto out = dir.create(pending_filename);

  if (!out) {
    throw IoError{
      absl::StrCat("Failed to create file, path: ", pending_filename)};
  }

  {
    format_utils::WriteHeader(*out, kFormatName, _version);
    out->WriteV64(meta.gen);
    out->WriteU64(meta.seg_counter);
    SDB_ASSERT(meta.segments.size() <= std::numeric_limits<uint32_t>::max());
    out->WriteV32(static_cast<uint32_t>(meta.segments.size()));

    for (const auto& segment : meta.segments) {
      WriteStr(*out, segment.filename);
      WriteStr(*out, segment.meta.codec->type()().name());
    }

    const auto payload = GetPayload(meta);
    const uint8_t flags = IsNull(payload) ? 0 : kHasPayload;
    out->WriteByte(flags);

    if (flags == kHasPayload) {
      WriteStr(*out, payload);
    }

    format_utils::WriteFooter(*out);
  }  // Important to close output here

  // Only noexcept operations below
  _dir = &dir;
  _pending_gen = meta.gen;

  return true;
}

inline bool IndexMetaWriterImpl::commit() {
  if (!index_gen_limits::valid(_pending_gen)) {
    return false;
  }

  const auto src = PendingFileName(_pending_gen);
  const auto dst = FileName(_pending_gen);

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

inline void IndexMetaWriterImpl::rollback() noexcept {
  if (!index_gen_limits::valid(_pending_gen)) {
    return;
  }

  std::string seg_file;

  try {
    seg_file = PendingFileName(_pending_gen);
  } catch (const std::exception& e) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat(
        "Caught error while generating file name for index meta, reason: ",
        e.what()));
    return;
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while generating file name for index meta");
    return;
  }

  if (!_dir->remove(seg_file)) {  // suppress all errors
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to remove file, path: ", seg_file));
  }

  // clear pending state
  _dir = nullptr;
  _pending_gen = index_gen_limits::invalid();
}

}  // namespace irs
