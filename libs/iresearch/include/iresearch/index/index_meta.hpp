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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/container/flat_hash_set.h>

#include <optional>
#include <span>
#include <vector>

#include "basics/resource_manager.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/index/document_mask.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class Format;
class IndexWriter;

struct SegmentInfo {
  bool operator==(const SegmentInfo&) const = default;

  std::string name;              // TODO(gnusi): move to SegmentMeta
  uint32_t docs_count = 0;       // Total number of documents in a segment
  uint32_t live_docs_count = 0;  // Total number of live documents in a segment
  uint64_t byte_size = 0;        // Size of a segment in bytes
  uint64_t version = 0;
};

static_assert(std::is_nothrow_move_constructible_v<SegmentInfo>);
static_assert(std::is_nothrow_move_assignable_v<SegmentInfo>);

struct SegmentMeta : SegmentInfo {
  bool operator==(const SegmentMeta& rhs) const {
    return SegmentInfo::operator==(rhs) && files == rhs.files &&
           codec == rhs.codec &&
           (docs_mask == rhs.docs_mask ||
            (docs_mask && rhs.docs_mask && *docs_mask == *rhs.docs_mask)) &&
           docs_mask_size == rhs.docs_mask_size;
  }

  std::vector<std::string> files;
  std::shared_ptr<const Format> codec;
  std::shared_ptr<const DocumentMask> docs_mask;
  uint64_t docs_mask_size = 0;
};

inline doc_id_t RemovalCount(const SegmentMeta& meta) noexcept {
  return meta.docs_mask ? static_cast<doc_id_t>(meta.docs_mask->DeletedDocCount()) : 0;
}

inline bool HasRemovals(const SegmentInfo& meta) noexcept {
  return meta.live_docs_count != meta.docs_count;
}

static_assert(std::is_nothrow_move_constructible_v<SegmentMeta>);
static_assert(std::is_nothrow_move_assignable_v<SegmentMeta>);

struct IndexSegment {
  bool operator==(const IndexSegment&) const = default;

  std::string filename;
  SegmentMeta meta;
};

static_assert(std::is_nothrow_move_constructible_v<IndexSegment>);
static_assert(std::is_nothrow_move_assignable_v<IndexSegment>);

struct IndexMeta {
  bool operator==(const IndexMeta&) const = default;

  uint64_t gen{index_gen_limits::invalid()};
  uint64_t seg_counter{0};
  std::vector<IndexSegment> segments;
  std::optional<bstring> payload;
};

inline bytes_view GetPayload(const IndexMeta& meta) noexcept {
  return meta.payload ? *meta.payload : bytes_view{};
}

struct DirectoryMeta {
  bool operator==(const DirectoryMeta&) const = default;

  std::string filename;
  IndexMeta index_meta;
};

static_assert(std::is_nothrow_move_constructible_v<DirectoryMeta>);
static_assert(std::is_nothrow_move_assignable_v<DirectoryMeta>);

}  // namespace irs
