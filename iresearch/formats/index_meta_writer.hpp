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

#include <absl/functional/any_invocable.h>

#include <duckdb/common/constants.hpp>
#include <duckdb/common/serializer/serialization_traits.hpp>
#include <string>
#include <string_view>

#include "iresearch/index/index_meta.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct Directory;

namespace index_meta {

inline constexpr std::string_view kPrefix = "segments_";
inline constexpr std::string_view kPendingPrefix = "pending_segments_";

inline constexpr duckdb::field_id_t kFieldStorageVersion = 0;
inline constexpr duckdb::field_id_t kFieldSegCounter = 1;
inline constexpr duckdb::field_id_t kFieldSegments = 2;
inline constexpr duckdb::field_id_t kFieldPayload = 3;

inline constexpr duckdb::field_id_t kSegmentFieldFilename = 0;
inline constexpr duckdb::field_id_t kSegmentFieldInvisibleCount = 1;

std::string FileName(uint64_t gen);

}  // namespace index_meta

using MetaPayloadWriter =
  absl::AnyInvocable<void(uint64_t tick, duckdb::BinarySerializer&)>;

class IndexMetaWriter final {
 public:
  explicit IndexMetaWriter(MetaPayloadWriter payload = {}) noexcept
    : _payload{std::move(payload)} {}

  // FIXME(gnusi): Better to split prepare into 2 methods and pass meta by
  // const reference
  bool prepare(Directory& dir, IndexMeta& meta, std::string& pending_filename,
               std::string& filename, uint64_t tick);
  bool commit();
  void rollback() noexcept;

 private:
  MetaPayloadWriter _payload;
  Directory* _dir{};
  uint64_t _pending_gen{index_gen_limits::invalid()};  // Generation to commit
};

}  // namespace irs
