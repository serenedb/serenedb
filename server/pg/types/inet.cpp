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

#include "pg/types/inet.h"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <fast_float/fast_float.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/function/cast/default_casts.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_error.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <span>
#include <string>

#include "pg/types/cast.h"
#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

std::string Unabbreviate(std::string_view text) {
  const auto slash = text.find('/');
  const auto address = text.substr(0, slash);
  const auto octets = static_cast<int>(absl::c_count(address, '.')) + 1;
  if (address.empty() || octets >= 4 || absl::StrContains(text, ':')) {
    return std::string{text};
  }
  std::string out{address};
  for (auto i = octets; i < 4; ++i) {
    absl::StrAppend(&out, ".0");
  }
  if (slash != std::string_view::npos) {
    absl::StrAppend(&out, text.substr(slash));
    return out;
  }
  uint32_t first = 0;
  if (fast_float::from_chars(address.data(), address.data() + address.size(),
                             first)
        .ec != std::errc{}) {
    return std::string{text};
  }
  auto bits = first >= 240   ? 32
              : first >= 224 ? 8
              : first >= 192 ? 24
              : first >= 128 ? 16
                             : 8;
  bits = std::max(bits, octets * 8);
  if (bits == 8 && first == 224) {
    bits = 4;
  }
  absl::StrAppend(&out, "/", bits);
  return out;
}

std::optional<irs::pg::SqlErrorData> TryParseCidr(std::string_view text,
                                                  INET_IPAddress& inet) {
  const auto input = Unabbreviate(text);
  try {
    inet = ipaddress_from_string(input.data(), input.size());
  } catch (const std::exception&) {
    inet.type = INET_IP_ADDRESS_INVALID;
  }
  if (inet.type == INET_IP_ADDRESS_INVALID) {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("invalid input syntax for type cidr: \"", text, "\""));
  }
  if (!IsNetwork(inet)) {
    return SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                          ERR_MSG("invalid cidr value: \"", text, "\""),
                          ERR_DETAIL("Value has bits set to right of mask."));
  }
  return std::nullopt;
}

bool TextToCidr(duckdb::Vector& source, duckdb::Vector& result,
                duckdb::idx_t count, duckdb::CastParameters& parameters) {
  auto src = source.Values<duckdb::string_t>();
  bool converted = true;
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto value = src[i];
    if (!value.IsValid()) {
      duckdb::FlatVector::SetNull(result, i, true);
      continue;
    }
    const auto& text = value.GetValue();
    INET_IPAddress inet{};
    if (auto error = TryParseCidr({text.GetData(), text.GetSize()}, inet)) {
      InputError(std::move(*error), parameters);
      converted = false;
      duckdb::FlatVector::SetNull(result, i, true);
      continue;
    }
    StoreInet(result, i, inet);
  }
  return converted;
}

template<typename Apply>
void ForEachInet(const duckdb::Vector& source, duckdb::idx_t count,
                 duckdb::Vector& result, Apply&& apply) {
  duckdb::RecursiveUnifiedVectorFormat vdata;
  duckdb::Vector::RecursiveToUnifiedFormat(source, vdata);
  for (duckdb::idx_t i = 0; i < count; i++) {
    const auto idx = vdata.unified.sel->get_index(i);
    if (!vdata.unified.validity.RowIsValid(idx)) {
      duckdb::FlatVector::SetNull(result, i, true);
      continue;
    }
    INET_IPAddress inet{};
    ReadInet(vdata, idx, inet);
    apply(i, inet);
  }
}

template<bool kToText>
bool CidrCast(duckdb::Vector& source, duckdb::Vector& result,
              duckdb::idx_t count, duckdb::CastParameters&) {
  ForEachInet(
    source, count, result, [&](duckdb::idx_t row, const INET_IPAddress& inet) {
      if constexpr (kToText) {
        char buffer[64];
        const auto size = CidrToString(inet, buffer, sizeof(buffer));
        duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result)[row] =
          duckdb::StringVector::AddString(result, buffer, size);
      } else {
        StoreInet(result, row, ipaddress_network(&inet));
      }
    });
  return true;
}

template<typename Apply>
void ForEachInetPair(const duckdb::Vector& lhs, const duckdb::Vector& rhs,
                     duckdb::idx_t count, duckdb::Vector& result,
                     Apply&& apply) {
  duckdb::RecursiveUnifiedVectorFormat left;
  duckdb::RecursiveUnifiedVectorFormat right;
  duckdb::Vector::RecursiveToUnifiedFormat(lhs, left);
  duckdb::Vector::RecursiveToUnifiedFormat(rhs, right);
  for (duckdb::idx_t i = 0; i < count; i++) {
    const auto l = left.unified.sel->get_index(i);
    const auto r = right.unified.sel->get_index(i);
    if (!left.unified.validity.RowIsValid(l) ||
        !right.unified.validity.RowIsValid(r)) {
      duckdb::FlatVector::SetNull(result, i, true);
      continue;
    }
    INET_IPAddress a{};
    INET_IPAddress b{};
    ReadInet(left, l, a);
    ReadInet(right, r, b);
    apply(i, a, b);
  }
}

constexpr uint64_t kIpv4Max = 0xFFFF'FFFF;

uint16_t MaxBits(const INET_IPAddress& inet) {
  return inet.type == INET_IP_ADDRESS_V6 ? 128 : 32;
}

INET_IPAddress Complement(INET_IPAddress inet) {
  inet.address.lower = ~inet.address.lower;
  inet.address.upper = ~inet.address.upper;
  if (inet.type != INET_IP_ADDRESS_V6) {
    inet.address.lower &= kIpv4Max;
    inet.address.upper = 0;
  }
  return inet;
}

bool SamePrefix(INET_IPAddress lhs, INET_IPAddress rhs, uint16_t bits) {
  lhs.mask = bits;
  rhs.mask = bits;
  const auto left = ipaddress_network(&lhs);
  const auto right = ipaddress_network(&rhs);
  return left.address.lower == right.address.lower &&
         left.address.upper == right.address.upper;
}

template<bool kOrEqual>
bool ContainedBy(const INET_IPAddress& inner, const INET_IPAddress& outer) {
  return inner.type == outer.type &&
         (kOrEqual ? inner.mask >= outer.mask : inner.mask > outer.mask) &&
         SamePrefix(inner, outer, outer.mask);
}

template<bool kOrEqual>
bool Contains(const INET_IPAddress& outer, const INET_IPAddress& inner) {
  return ContainedBy<kOrEqual>(inner, outer);
}

bool Overlaps(const INET_IPAddress& lhs, const INET_IPAddress& rhs) {
  return lhs.type == rhs.type &&
         SamePrefix(lhs, rhs, std::min(lhs.mask, rhs.mask));
}

bool SameFamily(const INET_IPAddress& lhs, const INET_IPAddress& rhs) {
  return lhs.type == rhs.type;
}

void CheckSameFamily(const INET_IPAddress& lhs, const INET_IPAddress& rhs,
                     std::string_view message) {
  if (lhs.type != rhs.type) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(message));
  }
}

[[noreturn]] void ThrowOutOfRange() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                  ERR_MSG("result is out of range"));
}

template<bool kOr>
INET_IPAddress Bitwise(const INET_IPAddress& lhs, const INET_IPAddress& rhs) {
  CheckSameFamily(lhs, rhs,
                  kOr ? "cannot OR inet values of different sizes"
                      : "cannot AND inet values of different sizes");
  auto result = lhs;
  if constexpr (kOr) {
    result.address.lower |= rhs.address.lower;
    result.address.upper |= rhs.address.upper;
  } else {
    result.address.lower &= rhs.address.lower;
    result.address.upper &= rhs.address.upper;
  }
  result.mask = std::max(lhs.mask, rhs.mask);
  return result;
}

int64_t Difference(const INET_IPAddress& lhs, const INET_IPAddress& rhs) {
  CheckSameFamily(lhs, rhs, "cannot subtract inet values of different sizes");
  if (lhs.type != INET_IP_ADDRESS_V6) {
    return static_cast<int64_t>(lhs.address.lower) -
           static_cast<int64_t>(rhs.address.lower);
  }
  const uint64_t borrow = lhs.address.lower < rhs.address.lower ? 1 : 0;
  const auto lower =
    static_cast<int64_t>(lhs.address.lower - rhs.address.lower);
  const auto upper = lhs.address.upper - rhs.address.upper - borrow;
  if (upper != (lower < 0 ? ~uint64_t{0} : uint64_t{0})) {
    ThrowOutOfRange();
  }
  return lower;
}

INET_IPAddress Offset(INET_IPAddress inet, bool negative, uint64_t magnitude) {
  if (inet.type != INET_IP_ADDRESS_V6) {
    if (negative ? magnitude > inet.address.lower
                 : magnitude > kIpv4Max - inet.address.lower) {
      ThrowOutOfRange();
    }
    inet.address.lower = negative ? inet.address.lower - magnitude
                                  : inet.address.lower + magnitude;
    return inet;
  }
  if (negative) {
    const uint64_t borrow = inet.address.lower < magnitude ? 1 : 0;
    if (borrow > inet.address.upper) {
      ThrowOutOfRange();
    }
    inet.address.lower -= magnitude;
    inet.address.upper -= borrow;
    return inet;
  }
  const auto lower = inet.address.lower + magnitude;
  const uint64_t carry = lower < magnitude ? 1 : 0;
  if (carry > ~inet.address.upper) {
    ThrowOutOfRange();
  }
  inet.address.lower = lower;
  inet.address.upper += carry;
  return inet;
}

uint64_t Magnitude(int64_t value) {
  return value < 0 ? uint64_t{0} - static_cast<uint64_t>(value)
                   : static_cast<uint64_t>(value);
}

INET_IPAddress Merge(const INET_IPAddress& lhs, const INET_IPAddress& rhs) {
  CheckSameFamily(lhs, rhs, "cannot merge addresses from different families");
  int common = 0;
  if (lhs.type != INET_IP_ADDRESS_V6) {
    common = std::countl_zero(
      static_cast<uint32_t>(lhs.address.lower ^ rhs.address.lower));
  } else if (const auto upper = lhs.address.upper ^ rhs.address.upper;
             upper != 0) {
    common = std::countl_zero(upper);
  } else {
    common = 64 + std::countl_zero(lhs.address.lower ^ rhs.address.lower);
  }
  auto result = lhs;
  result.mask =
    static_cast<uint16_t>(std::min(common, int{std::min(lhs.mask, rhs.mask)}));
  return ipaddress_network(&result);
}

template<auto Test>
void InetPredicateFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                           duckdb::Vector& result) {
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  ForEachInetPair(
    args.data[0], args.data[1], args.size(), result,
    [&](duckdb::idx_t row, const INET_IPAddress& lhs,
        const INET_IPAddress& rhs) { out[row] = Test(lhs, rhs); });
}

template<auto Combine>
void InetCombineFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  ForEachInetPair(args.data[0], args.data[1], args.size(), result,
                  [&](duckdb::idx_t row, const INET_IPAddress& lhs,
                      const INET_IPAddress& rhs) {
                    StoreInet(result, row, Combine(lhs, rhs));
                  });
}

void InetDifferenceFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                            duckdb::Vector& result) {
  auto* out = duckdb::FlatVector::GetDataMutable<int64_t>(result);
  ForEachInetPair(
    args.data[0], args.data[1], args.size(), result,
    [&](duckdb::idx_t row, const INET_IPAddress& lhs,
        const INET_IPAddress& rhs) { out[row] = Difference(lhs, rhs); });
}

template<bool kInetFirst, bool kSubtract>
void InetOffsetFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                        duckdb::Vector& result) {
  duckdb::UnifiedVectorFormat offsets;
  args.data[kInetFirst ? 1 : 0].ToUnifiedFormat(offsets);
  const auto* values = duckdb::UnifiedVectorFormat::GetData<int64_t>(offsets);
  ForEachInet(args.data[kInetFirst ? 0 : 1], args.size(), result,
              [&](duckdb::idx_t row, const INET_IPAddress& inet) {
                const auto idx = offsets.sel->get_index(row);
                if (!offsets.validity.RowIsValid(idx)) {
                  duckdb::FlatVector::SetNull(result, row, true);
                  return;
                }
                const auto offset = values[idx];
                StoreInet(result, row,
                          Offset(inet, kSubtract ? offset > 0 : offset < 0,
                                 Magnitude(offset)));
              });
}

void InetNotFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  ForEachInet(args.data[0], args.size(), result,
              [&](duckdb::idx_t row, const INET_IPAddress& inet) {
                StoreInet(result, row, Complement(inet));
              });
}

void NetmaskFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  ForEachInet(args.data[0], args.size(), result,
              [&](duckdb::idx_t row, const INET_IPAddress& inet) {
                auto mask = ipaddress_netmask(&inet);
                mask.mask = MaxBits(inet);
                StoreInet(result, row, mask);
              });
}

std::array<uint8_t, 16> AddressBytes(const INET_IPAddress& inet) {
  std::array<uint8_t, 16> bytes{};
  if (inet.type == INET_IP_ADDRESS_V6) {
    absl::big_endian::Store64(bytes.data(), inet.address.upper);
    absl::big_endian::Store64(bytes.data() + 8, inet.address.lower);
  } else {
    absl::big_endian::Store32(bytes.data(),
                              static_cast<uint32_t>(inet.address.lower));
  }
  return bytes;
}

void AppendCidrAbbrevV4(std::span<const uint8_t, 16> src, int bits,
                        std::string& out) {
  if (bits == 0) {
    out.push_back('0');
  }
  size_t at = 0;
  for (int b = bits / 8; b > 0; --b) {
    absl::StrAppend(&out, uint32_t{src[at++]});
    if (b > 1) {
      out.push_back('.');
    }
  }
  if (const int b = bits % 8; b > 0) {
    if (!out.empty()) {
      out.push_back('.');
    }
    absl::StrAppend(&out, src[at] & (((1U << b) - 1) << (8 - b)));
  }
  absl::StrAppend(&out, "/", bits);
}

void AppendCidrAbbrevV6(std::span<const uint8_t, 16> src, int bits,
                        std::string& out) {
  if (bits == 0) {
    out.append("::");
    absl::StrAppend(&out, "/", bits);
    return;
  }
  std::array<uint8_t, 16> s{};
  const int bytes = (bits + 7) / 8;
  std::copy_n(src.begin(), bytes, s.begin());
  if (const int b = bits % 8; b != 0) {
    s[bytes - 1] &= static_cast<uint8_t>(0xFF << (8 - b));
  }
  const int words = std::max((bits + 15) / 16, 2);
  int zero_s = 0;
  int zero_l = 0;
  int tmp_zero_s = 0;
  int tmp_zero_l = 0;
  for (int i = 0; i < words * 2; i += 2) {
    if ((s[i] | s[i + 1]) == 0) {
      if (tmp_zero_l == 0) {
        tmp_zero_s = i / 2;
      }
      ++tmp_zero_l;
    } else if (tmp_zero_l != 0 && zero_l < tmp_zero_l) {
      zero_s = tmp_zero_s;
      zero_l = tmp_zero_l;
      tmp_zero_l = 0;
    }
  }
  if (tmp_zero_l != 0 && zero_l < tmp_zero_l) {
    zero_s = tmp_zero_s;
    zero_l = tmp_zero_l;
  }
  const bool ipv4 =
    zero_l != words && zero_s == 0 &&
    (zero_l == 6 || (zero_l == 5 && s[10] == 0xFF && s[11] == 0xFF) ||
     (zero_l == 7 && s[14] != 0 && s[15] != 1));
  size_t at = 0;
  for (int word = 0; word < words; ++word) {
    if (zero_l != 0 && word >= zero_s && word < zero_s + zero_l) {
      if (word == zero_s) {
        out.push_back(':');
      }
      if (word == words - 1) {
        out.push_back(':');
      }
      at += 2;
      continue;
    }
    if (ipv4 && word > 5) {
      out.push_back(word == 6 ? ':' : '.');
      absl::StrAppend(&out, uint32_t{s[at++]});
      if (word != 7 || bits > 120) {
        out.push_back('.');
        absl::StrAppend(&out, uint32_t{s[at++]});
      }
    } else {
      if (!out.empty()) {
        out.push_back(':');
      }
      absl::StrAppend(&out, absl::Hex(s[at] * 256 + s[at + 1]));
      at += 2;
    }
  }
  absl::StrAppend(&out, "/", bits);
}

void MasklenFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  auto* masks = duckdb::FlatVector::GetDataMutable<int32_t>(result);
  ForEachInet(args.data[0], args.size(), result,
              [&](duckdb::idx_t row, const INET_IPAddress& inet) {
                masks[row] = inet.mask;
              });
}

void HostmaskFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  ForEachInet(args.data[0], args.size(), result,
              [&](duckdb::idx_t row, const INET_IPAddress& inet) {
                auto host = Complement(ipaddress_netmask(&inet));
                host.mask = MaxBits(inet);
                StoreInet(result, row, host);
              });
}

template<bool kCidr>
void SetMasklenFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                        duckdb::Vector& result) {
  duckdb::UnifiedVectorFormat lengths;
  args.data[1].ToUnifiedFormat(lengths);
  const auto* data = duckdb::UnifiedVectorFormat::GetData<int32_t>(lengths);
  ForEachInet(args.data[0], args.size(), result,
              [&](duckdb::idx_t row, INET_IPAddress inet) {
                const auto idx = lengths.sel->get_index(row);
                if (!lengths.validity.RowIsValid(idx)) {
                  duckdb::FlatVector::SetNull(result, row, true);
                  return;
                }
                auto bits = data[idx];
                if (bits == -1) {
                  bits = MaxBits(inet);
                }
                if (bits < 0 || bits > MaxBits(inet)) {
                  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                                  ERR_MSG("invalid mask length: ", bits));
                }
                inet.mask = static_cast<uint16_t>(bits);
                StoreInet(result, row, kCidr ? ipaddress_network(&inet) : inet);
              });
}

template<bool kCidr>
void AbbrevFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  auto* texts = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  std::string text;
  ForEachInet(args.data[0], args.size(), result,
              [&](duckdb::idx_t row, const INET_IPAddress& inet) {
                text.clear();
                if constexpr (kCidr) {
                  const auto bytes = AddressBytes(inet);
                  if (inet.type == INET_IP_ADDRESS_V6) {
                    AppendCidrAbbrevV6(bytes, inet.mask, text);
                  } else {
                    AppendCidrAbbrevV4(bytes, inet.mask, text);
                  }
                } else {
                  char buffer[64];
                  text.append(
                    buffer, ipaddress_to_string(&inet, buffer, sizeof(buffer)));
                }
                texts[row] = duckdb::StringVector::AddString(result, text);
              });
}

void RegisterCasts(duckdb::CastFunctionSet& casts) {
  RegisterTextInput(casts, CIDR(), TextToCidr, InitInput);
  casts.RegisterCastFunction(CIDR(), duckdb::LogicalType::VARCHAR,
                             CidrCast<true>, 50);
  casts.RegisterCastFunction(INET(), CIDR(), CidrCast<false>);
  casts.RegisterCastFunction(
    CIDR(), INET(),
    duckdb::BoundCastInfo(duckdb::DefaultCasts::ReinterpretCast), 1);
}

void RegisterFunctions(Functions& functions) {
  const auto inet = INET();
  const auto cidr = CIDR();
  const auto boolean = duckdb::LogicalType::BOOLEAN;
  const auto bigint = duckdb::LogicalType::BIGINT;
  Add(functions, "masklen",
      {{inet}, duckdb::LogicalType::INTEGER, MasklenFunction});
  Add(functions, "hostmask", {{inet}, inet, HostmaskFunction});
  Add(
    functions, "set_masklen",
    Fallible(
      {{inet, duckdb::LogicalType::INTEGER}, inet, SetMasklenFunction<false>}));
  Add(
    functions, "set_masklen",
    Fallible(
      {{cidr, duckdb::LogicalType::INTEGER}, cidr, SetMasklenFunction<true>}));
  Add(functions, "abbrev",
      {{inet}, duckdb::LogicalType::VARCHAR, AbbrevFunction<false>});
  Add(functions, "abbrev",
      {{cidr}, duckdb::LogicalType::VARCHAR, AbbrevFunction<true>});
  Add(functions, "<<",
      {{inet, inet}, boolean, InetPredicateFunction<ContainedBy<false>>});
  Add(functions, ">>",
      {{inet, inet}, boolean, InetPredicateFunction<Contains<false>>});
  Add(functions, "&&",
      {{inet, inet}, boolean, InetPredicateFunction<Overlaps>});
  Add(functions, "~", {{inet}, inet, InetNotFunction});
  Add(functions, "&",
      Fallible({{inet, inet}, inet, InetCombineFunction<Bitwise<false>>}));
  Add(functions, "|",
      Fallible({{inet, inet}, inet, InetCombineFunction<Bitwise<true>>}));
  Add(functions, "-", Fallible({{inet, inet}, bigint, InetDifferenceFunction}));
  Add(functions, "+",
      Fallible({{inet, bigint}, inet, InetOffsetFunction<true, false>}));
  Add(functions, "+",
      Fallible({{bigint, inet}, inet, InetOffsetFunction<false, false>}));
  Add(functions, "-",
      Fallible({{inet, bigint}, inet, InetOffsetFunction<true, true>}));
  Add(functions, "inet_same_family",
      {{inet, inet}, boolean, InetPredicateFunction<SameFamily>});
  Add(functions, "inet_merge",
      Fallible({{inet, inet}, cidr, InetCombineFunction<Merge>}));
  Replace(functions, "<<=",
          {{inet, inet}, boolean, InetPredicateFunction<ContainedBy<true>>});
  Replace(functions, ">>=",
          {{inet, inet}, boolean, InetPredicateFunction<Contains<true>>});
  Replace(functions, "netmask", {{inet}, inet, NetmaskFunction});
}

}  // namespace

void StoreInet(duckdb::Vector& vec, duckdb::idx_t row,
               const INET_IPAddress& inet) {
  auto upper = inet.address.upper;
  if (inet.type == INET_IP_ADDRESS_V6) {
    upper ^= (uint64_t{1} << 63);  // re-apply IPv6 sort-order flip
  }
  auto& entries = duckdb::StructVector::GetEntries(vec);
  duckdb::FlatVector::GetDataMutable<uint8_t>(entries[0])[row] =
    static_cast<uint8_t>(inet.type);
  duckdb::hugeint_t stored;
  stored.lower = inet.address.lower;
  stored.upper = static_cast<int64_t>(upper);
  duckdb::FlatVector::GetDataMutable<duckdb::hugeint_t>(entries[1])[row] =
    stored;
  duckdb::FlatVector::GetDataMutable<uint16_t>(entries[2])[row] = inet.mask;
}

bool IsNetwork(const INET_IPAddress& inet) {
  const auto network = ipaddress_network(&inet);
  return network.address.upper == inet.address.upper &&
         network.address.lower == inet.address.lower;
}

INET_IPAddress ParseCidr(std::string_view text) {
  INET_IPAddress inet{};
  if (auto error = TryParseCidr(text, inet)) {
    THROW_SQL_ERROR_FROM_DATA(std::move(*error));
  }
  return inet;
}

size_t CidrToString(const INET_IPAddress& inet, char* buffer, size_t size) {
  auto written = ipaddress_to_string(&inet, buffer, size);
  const std::string_view full =
    inet.type == INET_IP_ADDRESS_V6 ? "/128" : "/32";
  if (!std::string_view{buffer, written}.contains('/') &&
      written + full.size() <= size) {
    std::memcpy(buffer + written, full.data(), full.size());
    written += full.size();
  }
  return written;
}

void RegisterInet(duckdb::CastFunctionSet& casts, Functions& functions) {
  RegisterCasts(casts);
  RegisterFunctions(functions);
}

}  // namespace sdb::pg
