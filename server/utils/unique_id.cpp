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

#include "server/utils/unique_id.h"

#include <absl/algorithm/container.h>
#include <absl/random/random.h>
#include <absl/strings/escaping.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

namespace sdb {
namespace {

constexpr size_t kTimeBytes = 6;

}  // namespace

UniqueId UniqueId::New() {
  UniqueId id;
  const auto millis = static_cast<uint64_t>(absl::ToUnixMillis(absl::Now()));
  for (size_t i = 0; i != kTimeBytes; ++i) {
    id._bytes[i] =
      static_cast<uint8_t>(millis >> (8 * (kTimeBytes - 1 - i)));
  }
  absl::BitGen gen;
  for (size_t i = kTimeBytes; i != kBytes; ++i) {
    id._bytes[i] = absl::Uniform<uint8_t>(gen);
  }
  return id;
}

std::optional<UniqueId> UniqueId::FromBytes(std::string_view bytes) {
  if (bytes.size() != kBytes) {
    return std::nullopt;
  }
  UniqueId id;
  absl::c_copy(bytes, id._bytes.begin());
  return id;
}

std::optional<UniqueId> UniqueId::FromHex(std::string_view hex) {
  std::string bytes;
  if (hex.size() != 2 * kBytes || !absl::HexStringToBytes(hex, &bytes)) {
    return std::nullopt;
  }
  return FromBytes(bytes);
}

UniqueId::operator bool() const noexcept {
  return absl::c_any_of(_bytes, [](uint8_t b) { return b != 0; });
}

uint64_t UniqueId::High() const noexcept {
  uint64_t high = 0;
  for (size_t i = 0; i != sizeof(high); ++i) {
    high = (high << 8) | _bytes[i];
  }
  return high;
}

std::string UniqueId::Hex() const { return absl::BytesToHexString(Bytes()); }

}  // namespace sdb
