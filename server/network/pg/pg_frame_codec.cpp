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

#include "network/pg/pg_frame_codec.h"

#include <absl/base/internal/endian.h>

namespace sdb::network::pg {

FrameResult ParseFrame(std::string_view input, FrameKind kind,
                       uint32_t max_len) noexcept {
  const size_t offset = kind == FrameKind::Typed ? 1 : 0;
  if (input.size() < offset + 4) {
    return {.status = FrameStatus::NeedMore};
  }
  const uint32_t length = absl::big_endian::Load32(input.data() + offset);
  if (length < 4) {
    return {.status = FrameStatus::Malformed};
  }
  if (length > max_len) {
    return {.status = FrameStatus::TooLarge};
  }
  const size_t total = offset + length;
  if (input.size() < total) {
    return {.status = FrameStatus::NeedMore};
  }
  return {
    .payload = input.substr(offset + 4, length - 4),
    .consumed = total,
    .type = kind == FrameKind::Typed ? input[0] : '\0',
    .status = FrameStatus::Ok,
  };
}

uint32_t StartupCode(std::string_view payload) noexcept {
  if (payload.size() < 4) {
    return 0;
  }
  return absl::big_endian::Load32(payload.data());
}

}  // namespace sdb::network::pg
