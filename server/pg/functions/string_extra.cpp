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

#include "pg/functions/string_extra.h"

#include <absl/strings/escaping.h>
#include <absl/strings/numbers.h>
#include <openssl/evp.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/type/SimpleFunctionApi.h>

#include <iresearch/utils/utf8_utils.hpp>

#include "basics/fwd.h"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg::functions {
namespace {

// Returns MD5 hash as a 32-character hex string.
template<typename T>
struct PgMd5 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>& input) {
    unsigned char digest[MD5_DIGEST_LENGTH];
    MD5(reinterpret_cast<const unsigned char*>(input.data()), input.size(),
        digest);
    static constexpr char kHexDigits[] = "0123456789abcdef";
    result.resize(MD5_DIGEST_LENGTH * 2);
    auto* out = result.data();
    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
      out[i * 2] = kHexDigits[digest[i] >> 4];
      out[i * 2 + 1] = kHexDigits[digest[i] & 0x0f];
    }
  }
};

// to_hex(integer/bigint) -> text
template<typename T>
struct PgToHex {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const int32_t& value) {
    toHex(result, static_cast<uint32_t>(value));
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const int64_t& value) {
    toHex(result, static_cast<uint64_t>(value));
  }

 private:
  template<typename U>
  void toHex(out_type<velox::Varchar>& result, U uval) {
    char buf[16];
    auto digits = absl::numbers_internal::FastHexToBufferZeroPad16(uval, buf);
    result.resize(digits);
    std::memcpy(result.data(), buf + 16 - digits, digits);
  }
};

// to_bin(integer/bigint) -> text
template<typename T>
struct PgToBin {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const int32_t& value) {
    toBin(result, static_cast<uint32_t>(value));
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const int64_t& value) {
    toBin(result, static_cast<uint64_t>(value));
  }

 private:
  template<typename U>
  void toBin(out_type<velox::Varchar>& result, U uval) {
    char buf[sizeof(U) * 8];
    int pos = sizeof(buf);
    if (uval == 0) {
      result.resize(1);
      result.data()[0] = '0';
      return;
    }
    while (uval > 0) {
      buf[--pos] = '0' + (uval & 1);
      uval >>= 1;
    }
    int len = sizeof(buf) - pos;
    result.resize(len);
    std::memcpy(result.data(), buf + pos, len);
  }
};

// to_oct(integer/bigint) -> text
template<typename T>
struct PgToOct {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const int32_t& value) {
    toOct(result, static_cast<uint32_t>(value));
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const int64_t& value) {
    toOct(result, static_cast<uint64_t>(value));
  }

 private:
  template<typename U>
  void toOct(out_type<velox::Varchar>& result, U uval) {
    char buf[sizeof(U) * 3];  // max octal digits
    int pos = sizeof(buf);
    if (uval == 0) {
      result.resize(1);
      result.data()[0] = '0';
      return;
    }
    while (uval > 0) {
      buf[--pos] = '0' + (uval & 7);
      uval >>= 3;
    }
    int len = sizeof(buf) - pos;
    result.resize(len);
    std::memcpy(result.data(), buf + pos, len);
  }
};

// right(text, n) -> text
// Returns last n characters, or all but first |n| if n is negative.
template<typename T>
struct PgRight {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr int32_t reuse_strings_from_arg = 0;

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>& input,
                                const int32_t& n) {
    int32_t len = 0;
    // Count UTF-8 characters.
    const char* data = input.data();
    size_t size = input.size();
    for (size_t i = 0; i < size;) {
      ++len;
      unsigned char c = data[i];
      if (c < 0x80) {
        ++i;
      } else if (c < 0xE0) {
        i += 2;
      } else if (c < 0xF0) {
        i += 3;
      } else {
        i += 4;
      }
    }

    int32_t skip;
    if (n >= 0) {
      skip = std::max(0, len - n);
    } else {
      skip = std::min(len, -n);
    }

    // Advance `skip` characters.
    size_t byte_offset = 0;
    for (int32_t i = 0; i < skip && byte_offset < size; ++i) {
      unsigned char c = data[byte_offset];
      if (c < 0x80) {
        ++byte_offset;
      } else if (c < 0xE0) {
        byte_offset += 2;
      } else if (c < 0xF0) {
        byte_offset += 3;
      } else {
        byte_offset += 4;
      }
    }

    result.setNoCopy(velox::StringView(data + byte_offset, size - byte_offset));
  }
};

// string_to_array(text, delimiter [, null_string]) -> text[]
// Splits text by delimiter into an array.
template<typename T>
struct PgStringToArray {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_default_null_behavior = false;
  static constexpr int32_t reuse_strings_from_arg = 0;

  FOLLY_ALWAYS_INLINE bool callNullable(
    out_type<velox::Array<velox::Varchar>>& result,
    const arg_type<velox::Varchar>* input,
    const arg_type<velox::Varchar>* delimiter) {
    // NULL input -> NULL result
    if (!input) {
      return false;
    }

    const char* data = input->data();
    size_t size = input->size();

    // NULL delimiter: split into individual UTF-8 characters
    if (!delimiter) {
      auto* it = reinterpret_cast<const irs::byte_type*>(data);
      auto* end = it + size;
      while (it < end) {
        auto* next = irs::utf8_utils::Next(it, end);
        auto char_len = static_cast<size_t>(next - it);
        result.add_item().setNoCopy(
          velox::StringView(reinterpret_cast<const char*>(it), char_len));
        it = next;
      }
      return true;
    }

    // Empty delimiter: return single-element array (or empty for empty input)
    if (delimiter->size() == 0) {
      if (size > 0) {
        result.add_item().setNoCopy(velox::StringView(data, size));
      }
      return true;
    }

    size_t dlen = delimiter->size();
    size_t pos = 0;

    while (pos <= size) {
      // Find next occurrence of delimiter.
      const char* found = nullptr;
      if (pos + dlen <= size) {
        for (size_t i = pos; i + dlen <= size; ++i) {
          if (std::memcmp(data + i, delimiter->data(), dlen) == 0) {
            found = data + i;
            break;
          }
        }
      }

      if (found) {
        size_t elem_len = found - (data + pos);
        result.add_item().setNoCopy(velox::StringView(data + pos, elem_len));
        pos = (found - data) + dlen;
      } else {
        result.add_item().setNoCopy(velox::StringView(data + pos, size - pos));
        break;
      }
    }
    return true;
  }
};

// string_to_array(text, delimiter, null_string) -> text[]
// Splits text by delimiter; elements equal to null_string become NULL.
template<typename T>
struct PgStringToArray3 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_default_null_behavior = false;
  static constexpr int32_t reuse_strings_from_arg = 0;

  FOLLY_ALWAYS_INLINE bool callNullable(
    out_type<velox::Array<velox::Varchar>>& result,
    const arg_type<velox::Varchar>* input,
    const arg_type<velox::Varchar>* delimiter,
    const arg_type<velox::Varchar>* null_string) {
    // NULL input -> NULL result
    if (!input) {
      return false;
    }

    const char* data = input->data();
    size_t size = input->size();

    auto add_element = [&](const char* elem_data, size_t elem_len) {
      if (null_string && elem_len == null_string->size() &&
          (elem_len == 0 ||
           std::memcmp(elem_data, null_string->data(), elem_len) == 0)) {
        result.add_null();
      } else {
        result.add_item().setNoCopy(velox::StringView(elem_data, elem_len));
      }
    };

    // NULL delimiter: split into individual UTF-8 characters
    if (!delimiter) {
      auto* it = reinterpret_cast<const irs::byte_type*>(data);
      auto* end = it + size;
      while (it < end) {
        auto* next = irs::utf8_utils::Next(it, end);
        auto char_len = static_cast<size_t>(next - it);
        add_element(reinterpret_cast<const char*>(it), char_len);
        it = next;
      }
      return true;
    }

    // Empty delimiter: return single-element array (or empty for empty input)
    if (delimiter->size() == 0) {
      if (size > 0) {
        add_element(data, size);
      }
      return true;
    }

    size_t dlen = delimiter->size();
    size_t pos = 0;

    while (pos <= size) {
      const char* found = nullptr;
      if (pos + dlen <= size) {
        for (size_t i = pos; i + dlen <= size; ++i) {
          if (std::memcmp(data + i, delimiter->data(), dlen) == 0) {
            found = data + i;
            break;
          }
        }
      }

      if (found) {
        size_t elem_len = found - (data + pos);
        add_element(data + pos, elem_len);
        pos = (found - data) + dlen;
      } else {
        add_element(data + pos, size - pos);
        break;
      }
    }
    return true;
  }
};

// encode(bytea, format) -> text
// Supported formats: 'base64', 'hex', 'escape'
template<typename T>
struct PgEncode {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varbinary>& data,
                                const arg_type<velox::Varchar>& format) {
    std::string_view fmt(format.data(), format.size());
    if (fmt == "hex") {
      auto encoded =
        absl::BytesToHexString(absl::string_view(data.data(), data.size()));
      result.resize(encoded.size());
      std::memcpy(result.data(), encoded.data(), encoded.size());
    } else if (fmt == "base64") {
      auto encoded =
        absl::Base64Escape(absl::string_view(data.data(), data.size()));
      result.resize(encoded.size());
      std::memcpy(result.data(), encoded.data(), encoded.size());
    } else if (fmt == "escape") {
      std::string out;
      out.reserve(data.size());
      for (size_t i = 0; i < data.size(); ++i) {
        auto c = static_cast<uint8_t>(data.data()[i]);
        if (c == '\\') {
          out += "\\\\";
        } else if (c < 32 || c > 126) {
          out += '\\';
          out += ('0' + (c >> 6));
          out += ('0' + ((c >> 3) & 7));
          out += ('0' + (c & 7));
        } else {
          out += static_cast<char>(c);
        }
      }
      result.resize(out.size());
      std::memcpy(result.data(), out.data(), out.size());
    } else {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("unrecognized encoding: \"", fmt, "\""));
    }
  }
};

// decode(text, format) -> bytea
template<typename T>
struct PgDecode {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varbinary>& result,
                                const arg_type<velox::Varchar>& data,
                                const arg_type<velox::Varchar>& format) {
    std::string_view fmt(format.data(), format.size());
    if (fmt == "hex") {
      std::string decoded;
      if (!absl::HexStringToBytes(absl::string_view(data.data(), data.size()),
                                  &decoded)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("invalid hexadecimal data"));
      }
      result.resize(decoded.size());
      std::memcpy(result.data(), decoded.data(), decoded.size());
    } else if (fmt == "base64") {
      std::string decoded;
      if (!absl::Base64Unescape(absl::string_view(data.data(), data.size()),
                                &decoded)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("invalid input for base64 decoding"));
      }
      result.resize(decoded.size());
      std::memcpy(result.data(), decoded.data(), decoded.size());
    } else if (fmt == "escape") {
      std::string out;
      out.reserve(data.size());
      for (size_t i = 0; i < data.size(); ++i) {
        if (data.data()[i] == '\\' && i + 1 < data.size()) {
          if (data.data()[i + 1] == '\\') {
            out += '\\';
            ++i;
          } else if (i + 3 < data.size() && data.data()[i + 1] >= '0' &&
                     data.data()[i + 1] <= '7' && data.data()[i + 2] >= '0' &&
                     data.data()[i + 2] <= '7' && data.data()[i + 3] >= '0' &&
                     data.data()[i + 3] <= '7') {
            auto c = static_cast<uint8_t>((data.data()[i + 1] - '0') * 64 +
                                          (data.data()[i + 2] - '0') * 8 +
                                          (data.data()[i + 3] - '0'));
            out += static_cast<char>(c);
            i += 3;
          } else {
            out += data.data()[i];
          }
        } else {
          out += data.data()[i];
        }
      }
      result.resize(out.size());
      std::memcpy(result.data(), out.data(), out.size());
    } else {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("unrecognized encoding: \"", fmt, "\""));
    }
  }
};

// get_byte(bytea, offset) -> integer
template<typename T>
struct PgGetByte {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(int32_t& result,
                                const arg_type<velox::Varbinary>& data,
                                int32_t offset) {
    if (offset < 0 || offset >= static_cast<int32_t>(data.size())) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                      ERR_MSG("index ", offset, " out of valid range, 0..",
                              static_cast<int64_t>(data.size()) - 1));
    }
    result = static_cast<uint8_t>(data.data()[offset]);
  }
};

// set_byte(bytea, offset, value) -> bytea
template<typename T>
struct PgSetByte {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varbinary>& result,
                                const arg_type<velox::Varbinary>& data,
                                int32_t offset, int32_t value) {
    if (offset < 0 || offset >= static_cast<int32_t>(data.size())) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                      ERR_MSG("index ", offset, " out of valid range, 0..",
                              static_cast<int64_t>(data.size()) - 1));
    }
    result.resize(data.size());
    std::memcpy(result.data(), data.data(), data.size());
    result.data()[offset] = static_cast<char>(value & 0xff);
  }
};

// get_bit(bytea, offset) -> integer
template<typename T>
struct PgGetBit {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(int32_t& result,
                                const arg_type<velox::Varbinary>& data,
                                int64_t bit_offset) {
    if (bit_offset < 0 || bit_offset >= static_cast<int64_t>(data.size()) * 8) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                      ERR_MSG("index ", bit_offset, " out of valid range, 0..",
                              static_cast<int64_t>(data.size()) * 8 - 1));
    }
    int64_t byte_idx = bit_offset / 8;
    int bit_idx = static_cast<int>(bit_offset % 8);
    result = (static_cast<uint8_t>(data.data()[byte_idx]) >> bit_idx) & 1;
  }
};

// set_bit(bytea, offset, value) -> bytea
template<typename T>
struct PgSetBit {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varbinary>& result,
                                const arg_type<velox::Varbinary>& data,
                                int64_t bit_offset, int32_t value) {
    if (bit_offset < 0 || bit_offset >= static_cast<int64_t>(data.size()) * 8) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                      ERR_MSG("index ", bit_offset, " out of valid range, 0..",
                              static_cast<int64_t>(data.size()) * 8 - 1));
    }
    int64_t byte_idx = bit_offset / 8;
    int bit_idx = static_cast<int>(bit_offset % 8);
    result.resize(data.size());
    std::memcpy(result.data(), data.data(), data.size());
    auto& byte = result.data()[byte_idx];
    if (value) {
      byte |= (1 << bit_idx);
    } else {
      byte &= ~(1 << bit_idx);
    }
  }
};

// sha224(bytea) -> bytea
template<typename T>
struct PgSha224 {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varbinary>& result,
                                const arg_type<velox::Varbinary>& data) {
    unsigned char digest[SHA224_DIGEST_LENGTH];
    SHA224(reinterpret_cast<const unsigned char*>(data.data()), data.size(),
           digest);
    result.resize(SHA224_DIGEST_LENGTH);
    std::memcpy(result.data(), digest, SHA224_DIGEST_LENGTH);
  }
};

// sha384(bytea) -> bytea
template<typename T>
struct PgSha384 {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varbinary>& result,
                                const arg_type<velox::Varbinary>& data) {
    unsigned char digest[SHA384_DIGEST_LENGTH];
    SHA384(reinterpret_cast<const unsigned char*>(data.data()), data.size(),
           digest);
    result.resize(SHA384_DIGEST_LENGTH);
    std::memcpy(result.data(), digest, SHA384_DIGEST_LENGTH);
  }
};

// convert_from(bytea, encoding) -> text
// For now only supports UTF8.
template<typename T>
struct PgConvertFrom {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varbinary>& data,
                                const arg_type<velox::Varchar>& encoding) {
    std::string_view enc(encoding.data(), encoding.size());
    if (enc != "UTF8" && enc != "UTF-8" && enc != "utf8" && enc != "utf-8") {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("conversion from ", enc, " is not supported"));
    }
    result.resize(data.size());
    std::memcpy(result.data(), data.data(), data.size());
  }
};

// convert_to(text, encoding) -> bytea
template<typename T>
struct PgConvertTo {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varbinary>& result,
                                const arg_type<velox::Varchar>& data,
                                const arg_type<velox::Varchar>& encoding) {
    std::string_view enc(encoding.data(), encoding.size());
    if (enc != "UTF8" && enc != "UTF-8" && enc != "utf8" && enc != "utf-8") {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("conversion to ", enc, " is not supported"));
    }
    result.resize(data.size());
    std::memcpy(result.data(), data.data(), data.size());
  }
};

// pg_client_encoding() -> name
template<typename T>
struct PgClientEncoding {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result) {
    result.copy_from("UTF8");
  }
};

}  // namespace

void registerStringExtraFunctions(const std::string& prefix) {
  velox::registerFunction<PgMd5, velox::Varchar, velox::Varchar>(
    {prefix + "md5"});
  velox::registerFunction<PgToHex, velox::Varchar, int32_t>(
    {prefix + "to_hex"});
  velox::registerFunction<PgToHex, velox::Varchar, int64_t>(
    {prefix + "to_hex"});
  velox::registerFunction<PgToBin, velox::Varchar, int32_t>(
    {prefix + "to_bin"});
  velox::registerFunction<PgToBin, velox::Varchar, int64_t>(
    {prefix + "to_bin"});
  velox::registerFunction<PgToOct, velox::Varchar, int32_t>(
    {prefix + "to_oct"});
  velox::registerFunction<PgToOct, velox::Varchar, int64_t>(
    {prefix + "to_oct"});
  velox::registerFunction<PgRight, velox::Varchar, velox::Varchar, int32_t>(
    {prefix + "right"});
  velox::registerFunction<PgStringToArray, velox::Array<velox::Varchar>,
                          velox::Varchar, velox::Varchar>(
    {prefix + "string_to_array"});
  velox::registerFunction<PgStringToArray3, velox::Array<velox::Varchar>,
                          velox::Varchar, velox::Varchar, velox::Varchar>(
    {prefix + "string_to_array"});
  velox::registerFunction<PgEncode, velox::Varchar, velox::Varbinary,
                          velox::Varchar>({prefix + "encode"});
  velox::registerFunction<PgDecode, velox::Varbinary, velox::Varchar,
                          velox::Varchar>({prefix + "decode"});
  velox::registerFunction<PgGetByte, int32_t, velox::Varbinary, int32_t>(
    {prefix + "get_byte"});
  velox::registerFunction<PgSetByte, velox::Varbinary, velox::Varbinary,
                          int32_t, int32_t>({prefix + "set_byte"});
  velox::registerFunction<PgGetBit, int32_t, velox::Varbinary, int64_t>(
    {prefix + "get_bit"});
  velox::registerFunction<PgSetBit, velox::Varbinary, velox::Varbinary, int64_t,
                          int32_t>({prefix + "set_bit"});
  velox::registerFunction<PgSha224, velox::Varbinary, velox::Varbinary>(
    {prefix + "sha224"});
  velox::registerFunction<PgSha384, velox::Varbinary, velox::Varbinary>(
    {prefix + "sha384"});
  velox::registerFunction<PgConvertFrom, velox::Varchar, velox::Varbinary,
                          velox::Varchar>({prefix + "convert_from"});
  velox::registerFunction<PgConvertTo, velox::Varbinary, velox::Varchar,
                          velox::Varchar>({prefix + "convert_to"});
  velox::registerFunction<PgClientEncoding, velox::Varchar>(
    {prefix + "client_encoding"});
}

}  // namespace sdb::pg::functions
