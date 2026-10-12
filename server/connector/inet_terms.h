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

#include <algorithm>
#include <array>
#include <cstdint>
#include <iresearch/utils/string.hpp>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {

class Value;

}  // namespace duckdb
namespace sdb::connector::inet_terms {

inline constexpr size_t kMaxTermSize = 1 + 16 + 2 + 16;
inline constexpr size_t kMaxTermsPerValue = 9;

using Bytes = std::array<uint8_t, 16>;

struct Inet {
  bool v6 = false;
  uint16_t mask = 0;
  Bytes address{};

  uint32_t Width() const noexcept { return v6 ? 16 : 4; }
  uint32_t MaxBits() const noexcept { return v6 ? 128 : 32; }
  uint32_t NetworkBits() const noexcept {
    return std::min<uint32_t>(mask, MaxBits());
  }
};

std::optional<Inet> FromAddress(uint8_t ip_type, uint64_t upper, uint64_t lower,
                                uint16_t mask);
std::optional<Inet> FromValue(const duckdb::Value& value);

Inet FamilyMin(bool v6);
Inet FamilyMax(bool v6);

Bytes Network(const Inet& value, uint32_t bits);
Bytes Broadcast(const Inet& value, uint32_t bits);

uint32_t LevelCount(bool v6);
uint32_t LevelBits(bool v6, uint32_t level);

size_t ExactTerm(const Inet& value, uint8_t* out);
size_t LevelTerm(bool v6, uint32_t level, const Bytes& network, uint8_t* out);
size_t MaskTerm(bool v6, uint8_t mask, uint8_t* out);

irs::bstring ExactTermOf(const Inet& value);
std::vector<irs::bstring> GranularTermsOf(const Inet& value);
irs::bstring LevelTermOf(bool v6, uint32_t level, const Bytes& network);
irs::bstring MaskTermOf(bool v6, uint8_t mask);
irs::bstring ExactPrefixOf(bool v6, const Bytes& network);
irs::bstring ExactPrefixOf(bool v6, const Bytes& network, uint16_t mask);
irs::bstring ExactUpperOf(bool v6, const Bytes& network);
irs::bstring CoarsestLevelPrefixOf(bool v6);

std::string Describe(irs::bytes_view term);

template<typename Emit>
void ForEachTerm(const Inet& value, Emit&& emit) {
  uint8_t buf[kMaxTermSize];
  emit(buf, ExactTerm(value, buf));
  const auto network = Network(value, value.NetworkBits());
  for (uint32_t level = 1; level <= LevelCount(value.v6); ++level) {
    emit(buf, LevelTerm(value.v6, level, network, buf));
  }
  if (value.mask < value.MaxBits()) {
    emit(buf, MaskTerm(value.v6, static_cast<uint8_t>(value.mask), buf));
  }
}

}  // namespace sdb::connector::inet_terms
