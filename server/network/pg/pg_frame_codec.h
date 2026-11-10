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

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace sdb::network::pg {

enum class FrameStatus : uint8_t {
  Ok,
  NeedMore,
  TooLarge,
  Malformed,
};

// Startup-phase frames carry no type byte (just a length-prefixed body);
// command-phase frames are a type byte + length + body.
enum class FrameKind : uint8_t {
  Startup,
  Typed,
};

// A parsed frame plus how many input bytes it consumed. Fields ordered
// widest-first so the struct packs tight (no interior padding).
struct FrameResult {
  std::string_view payload;
  size_t consumed = 0;
  char type = 0;
  FrameStatus status = FrameStatus::NeedMore;
};

// Parse one frame from the head of `input`; status == NeedMore when `input`
// does not yet hold a whole frame.
FrameResult ParseFrame(std::string_view input, FrameKind kind,
                       uint32_t max_len) noexcept;

// The 4-byte protocol/request code at the head of a startup-phase payload.
uint32_t StartupCode(std::string_view payload) noexcept;

}  // namespace sdb::network::pg
