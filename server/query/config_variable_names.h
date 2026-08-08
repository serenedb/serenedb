////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string_view>

namespace sdb {

inline constexpr std::string_view kRowGroupSizeSetting = "row_group_size";
inline constexpr std::string_view kNormRowGroupSizeSetting =
  "norm_row_group_size";
inline constexpr std::string_view kRefreshIntervalSetting = "refresh_interval";
inline constexpr std::string_view kCompactionIntervalSetting =
  "compaction_interval";
inline constexpr std::string_view kCleanupIntervalStepSetting =
  "cleanup_interval_step";
inline constexpr std::string_view kSegmentMemoryMaxSetting =
  "segment_memory_max";
inline constexpr std::string_view kSegmentDocsMaxSetting = "segment_docs_max";
inline constexpr std::string_view kCompactionMaxSegmentsSetting =
  "compaction_max_segments";
inline constexpr std::string_view kCompactionMaxSegmentsBytesSetting =
  "compaction_max_segments_bytes";
inline constexpr std::string_view kCompactionFloorSegmentBytesSetting =
  "compaction_floor_segment_bytes";
inline constexpr std::string_view kRecoveryReplayDepthSetting =
  "recovery_replay_depth";

}  // namespace sdb
