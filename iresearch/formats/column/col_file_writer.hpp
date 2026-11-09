////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "iresearch/formats/format_utils.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/types.hpp"

namespace irs {

class ColFileWriter final {
 public:
  static constexpr size_t kChunkSize = size_t{64} << 10;
  static constexpr size_t kMaxQueued = size_t{64} << 20;

  explicit ColFileWriter(IndexOutput::ptr out);

  uint64_t Position();

  uint64_t Append(std::string_view bytes);

  uint64_t AppendAligned(std::span<const std::string_view> parts);

  uint64_t AppendString(std::string_view string);

  void Finish(format_utils::FooterWriter footer);

 private:
  struct Chunk {
    std::unique_ptr<byte_type[]> data;
    size_t size = 0;
  };

  uint64_t Write(uint64_t alignment, std::span<const std::string_view> parts);
  void Stage(std::string_view bytes) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void TakeFull(std::vector<Chunk>& batch)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void Drain();
  bool HasRoom() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  bool Idle() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);

  IndexOutput::ptr _out;
  absl::Mutex _mutex;
  uint64_t _end ABSL_GUARDED_BY(_mutex);
  size_t _queued ABSL_GUARDED_BY(_mutex) = 0;
  bool _writing ABSL_GUARDED_BY(_mutex) = false;
  Chunk _tail ABSL_GUARDED_BY(_mutex);
  std::vector<Chunk> _full ABSL_GUARDED_BY(_mutex);
};

}  // namespace irs
