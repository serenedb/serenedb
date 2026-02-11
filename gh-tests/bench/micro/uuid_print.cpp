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

#include <absl/strings/internal/str_format/output.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <fmt/compile.h>
#include <fmt/format.h>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <limits>
#include <random>

#include "basics/shared.hpp"

namespace {

auto MakeNumbers(const boost::uuids::uuid& u) {
  auto uuid = *reinterpret_cast<const __int128_t*>(u.begin());
  // this byteswap isn't needed in our production code
  // but boost::uuid use opposite byte order
  uuid = std::byteswap(uuid);

  const uint64_t high = (uuid >> 64);
  const uint64_t low = (uuid & std::numeric_limits<uint64_t>::max());

  const uint32_t a = high >> 32;
  const uint16_t b = (high >> 16) & 0xFFFF;
  const uint16_t c = high & 0xFFFF;
  const uint16_t d = low >> 48;
  const uint64_t e = low & 0xFFFF'FFFF'FFFF;
  return std::tuple{a, b, c, d, e};
}

IRS_NO_INLINE char* FormatStd(char* buf, const boost::uuids::uuid& u) {
  const auto [a, b, c, d, e] = MakeNumbers(u);
  return std::format_to(buf, "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}", a, b, c, d,
                        e);
}

IRS_NO_INLINE char* FormatAbseilSNPrintf(char* buf,
                                         const boost::uuids::uuid& u) {
  const auto [a, b, c, d, e] = MakeNumbers(u);
  return buf +
         absl::SNPrintF(buf, 39, "%08x-%04x-%04x-%04x-%012x", a, b, c, d, e);
}

IRS_NO_INLINE char* FormatFmt(char* buf, const boost::uuids::uuid& u) {
  const auto [a, b, c, d, e] = MakeNumbers(u);
  return fmt::format_to(buf, "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}", a, b, c, d,
                        e);
}

IRS_NO_INLINE char* FormatFmtCompile(char* buf, const boost::uuids::uuid& u) {
  const auto [a, b, c, d, e] = MakeNumbers(u);
  return fmt::format_to(buf, FMT_COMPILE("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}"),
                        a, b, c, d, e);
}

IRS_NO_INLINE char* FormatAbseilHexStringify(char* buf,
                                             const boost::uuids::uuid& u) {
  const auto [a, b, c, d, e] = MakeNumbers(u);
  struct {
    char* buf;

    void Append(std::string_view x) {
      std::memcpy(buf, x.data(), x.size());
      buf += x.size();
    }
  } sink{buf};

  AbslStringify(sink, absl::Hex{a, absl::kZeroPad8});
  *sink.buf++ = '-';
  AbslStringify(sink, absl::Hex{b, absl::kZeroPad4});
  *sink.buf++ = '-';
  AbslStringify(sink, absl::Hex{c, absl::kZeroPad4});
  *sink.buf++ = '-';
  AbslStringify(sink, absl::Hex{d, absl::kZeroPad4});
  *sink.buf++ = '-';
  AbslStringify(sink, absl::Hex{e, absl::kZeroPad12});

  return sink.buf;
}

IRS_NO_INLINE char* FormatOwn(char* buf, const boost::uuids::uuid& u) {
  const auto [a, b, c, d, e] = MakeNumbers(u);
  static constexpr size_t kMaxHexSize = 16;
  char hex_buf[kMaxHexSize];
  char* const hex_buf_end = hex_buf + kMaxHexSize;
  struct Hex {
    uint64_t value;
    uint8_t pad;
  };
  auto write_hex = [&](Hex hex) {
    absl::numbers_internal::FastHexToBufferZeroPad16(hex.value, hex_buf);
    std::memcpy(buf, hex_buf_end - hex.pad, hex.pad);
    buf += hex.pad;
  };

  write_hex(Hex{a, 8});
  *buf++ = '-';
  write_hex(Hex{b, 4});
  *buf++ = '-';
  write_hex(Hex{c, 4});
  *buf++ = '-';
  write_hex(Hex{d, 4});
  *buf++ = '-';
  write_hex(Hex{e, 12});

  return buf;
}

IRS_NO_INLINE char* FormatBoostUUID(char* buf, const boost::uuids::uuid& u) {
  return boost::uuids::detail::to_chars(u, buf);
}

using FormatFunc = char*(char* buf, const boost::uuids::uuid& u);

void Do(std::string_view name, FormatFunc format, size_t n, uint64_t seed) {
  std::vector<std::array<char, 40>> buffers(n);

  std::mt19937_64 number_gen{seed};
  boost::uuids::basic_random_generator uuid_gen{number_gen};
  std::vector<boost::uuids::uuid> kits(n);
  for (auto& kit : kits) {
    kit = uuid_gen();
  }

  const auto start = std::chrono::high_resolution_clock::now();

  for (size_t i = 0; i < n; ++i) {
    char* end = format(buffers[i].data(), kits[i]);
    *end = '\0';
  }

  const auto end = std::chrono::high_resolution_clock::now();
  const auto duration = end - start;

  size_t hash = 0;
  for (const auto& buf : buffers) {
    size_t i = 0;
    for (; i < buf.size(); ++i) {
      char c = buf[i];
      if (c == '\0') {
        break;
      }
      hash = hash * 31 + static_cast<unsigned char>(c);
    }
    if (i != 36) {
      std::cout << "Wrong size in " << name << ": " << i << " != 36"
                << std::endl;
      std::abort();
    }
  }

  std::cout << "Name: " << name << "\n"
            << "Time: " << static_cast<double>(duration.count()) / n << " ns\n"
            << "Hash: " << hash << "\n"
            << "First: " << buffers[0].data() << "\n"
            << std::endl;
}

}  // namespace

int main() {
  static constexpr uint64_t kSeed = 1234;
  static constexpr size_t kSize = 1'000'000;

  Do("std", FormatStd, kSize, kSeed);
  Do("abseil snprintf", FormatAbseilSNPrintf, kSize, kSeed);
  Do("fmt", FormatFmt, kSize, kSeed);
  Do("fmt compile", FormatFmtCompile, kSize, kSeed);
  Do("boost uuids detail", FormatBoostUUID, kSize, kSeed);
  Do("abseil hex stringify", FormatAbseilHexStringify, kSize, kSeed);
  Do("own", FormatOwn, kSize, kSeed);
}
