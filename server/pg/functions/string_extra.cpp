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

#include <openssl/evp.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <velox/common/encode/Base64.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/type/SimpleFunctionApi.h>

#include "basics/fwd.h"

namespace sdb::pg::functions {
namespace {

using namespace velox;

// md5(text) -> text
// Returns MD5 hash as a 32-character hex string.
template<typename T>
struct PgMd5 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Varchar>& result,
                                const arg_type<Varchar>& input) {
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

// to_hex(integer) -> text
// to_hex(bigint) -> text
template<typename T>
struct PgToHex {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Varchar>& result,
                                const int32_t& value) {
    toHex(result, static_cast<uint32_t>(value));
  }

  FOLLY_ALWAYS_INLINE void call(out_type<Varchar>& result,
                                const int64_t& value) {
    toHex(result, static_cast<uint64_t>(value));
  }

 private:
  template<typename U>
  void toHex(out_type<Varchar>& result, U uval) {
    static constexpr char kHexDigits[] = "0123456789abcdef";
    // Max hex digits: 8 for uint32, 16 for uint64.
    char buf[sizeof(U) * 2];
    int pos = sizeof(buf);
    if (uval == 0) {
      result.resize(1);
      result.data()[0] = '0';
      return;
    }
    while (uval > 0) {
      buf[--pos] = kHexDigits[uval & 0xf];
      uval >>= 4;
    }
    int len = sizeof(buf) - pos;
    result.resize(len);
    std::memcpy(result.data(), buf + pos, len);
  }
};

// to_bin(integer/bigint) -> text
template<typename T>
struct PgToBin {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Varchar>& result,
                                const int32_t& value) {
    toBin(result, static_cast<uint32_t>(value));
  }

  FOLLY_ALWAYS_INLINE void call(out_type<Varchar>& result,
                                const int64_t& value) {
    toBin(result, static_cast<uint64_t>(value));
  }

 private:
  template<typename U>
  void toBin(out_type<Varchar>& result, U uval) {
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

  FOLLY_ALWAYS_INLINE void call(out_type<Varchar>& result,
                                const int32_t& value) {
    toOct(result, static_cast<uint32_t>(value));
  }

  FOLLY_ALWAYS_INLINE void call(out_type<Varchar>& result,
                                const int64_t& value) {
    toOct(result, static_cast<uint64_t>(value));
  }

 private:
  template<typename U>
  void toOct(out_type<Varchar>& result, U uval) {
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

  FOLLY_ALWAYS_INLINE void call(out_type<Varchar>& result,
                                const arg_type<Varchar>& input,
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

    result.setNoCopy(StringView(data + byte_offset, size - byte_offset));
  }
};

// string_to_array(text, delimiter [, null_string]) -> text[]
// Splits text by delimiter into an array.
template<typename T>
struct PgStringToArray {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<Array<Varchar>>& result,
                                const arg_type<Varchar>& input,
                                const arg_type<Varchar>& delimiter) {
    if (delimiter.size() == 0) {
      // Empty delimiter: return array of individual characters is PG behavior
      // when delimiter is empty string. Actually PG returns NULL for empty
      // delimiter. Let's return NULL.
      return false;
    }

    const char* data = input.data();
    size_t size = input.size();
    size_t dlen = delimiter.size();
    size_t pos = 0;

    while (pos <= size) {
      // Find next occurrence of delimiter.
      const char* found = nullptr;
      if (dlen > 0 && pos + dlen <= size) {
        for (size_t i = pos; i + dlen <= size; ++i) {
          if (std::memcmp(data + i, delimiter.data(), dlen) == 0) {
            found = data + i;
            break;
          }
        }
      }

      if (found) {
        size_t elem_len = found - (data + pos);
        result.add_item().setNoCopy(StringView(data + pos, elem_len));
        pos = (found - data) + dlen;
      } else {
        result.add_item().setNoCopy(StringView(data + pos, size - pos));
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

  FOLLY_ALWAYS_INLINE void call(out_type<Varchar>& result,
                                const arg_type<Varbinary>& data,
                                const arg_type<Varchar>& format) {
    std::string_view fmt(format.data(), format.size());
    if (fmt == "hex") {
      static constexpr char kHex[] = "0123456789abcdef";
      result.resize(data.size() * 2);
      auto* out = result.data();
      for (size_t i = 0; i < data.size(); ++i) {
        auto byte = static_cast<uint8_t>(data.data()[i]);
        out[i * 2] = kHex[byte >> 4];
        out[i * 2 + 1] = kHex[byte & 0xf];
      }
    } else if (fmt == "base64") {
      auto encoded = velox::encoding::Base64::encode(
        folly::StringPiece(data.data(), data.size()));
      result.resize(encoded.size());
      std::memcpy(result.data(), encoded.data(), encoded.size());
    } else if (fmt == "escape") {
      // Escape encoding: non-printable bytes as octal, backslash doubled.
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
      VELOX_USER_CHECK(false, "unrecognized encoding: {}", fmt);
    }
  }
};

// decode(text, format) -> bytea
template<typename T>
struct PgDecode {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Varbinary>& result,
                                const arg_type<Varchar>& data,
                                const arg_type<Varchar>& format) {
    std::string_view fmt(format.data(), format.size());
    if (fmt == "hex") {
      VELOX_USER_CHECK(data.size() % 2 == 0, "invalid hexadecimal data");
      result.resize(data.size() / 2);
      auto* out = result.data();
      for (size_t i = 0; i < data.size(); i += 2) {
        auto hi = hexVal(data.data()[i]);
        auto lo = hexVal(data.data()[i + 1]);
        out[i / 2] = static_cast<char>((hi << 4) | lo);
      }
    } else if (fmt == "base64") {
      auto decoded = velox::encoding::Base64::decode(
        folly::StringPiece(data.data(), data.size()));
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
          } else if (i + 3 < data.size()) {
            auto c = static_cast<uint8_t>((data.data()[i + 1] - '0') * 64 +
                                          (data.data()[i + 2] - '0') * 8 +
                                          (data.data()[i + 3] - '0'));
            out += static_cast<char>(c);
            i += 3;
          }
        } else {
          out += data.data()[i];
        }
      }
      result.resize(out.size());
      std::memcpy(result.data(), out.data(), out.size());
    } else {
      VELOX_USER_CHECK(false, "unrecognized encoding: {}", fmt);
    }
  }

 private:
  static uint8_t hexVal(char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
      return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
      return c - 'A' + 10;
    }
    VELOX_USER_CHECK(false, "invalid hexadecimal digit: {}", c);
    return 0;
  }
};

// get_byte(bytea, offset) -> integer
template<typename T>
struct PgGetByte {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(int32_t& result,
                                const arg_type<Varbinary>& data,
                                int32_t offset) {
    VELOX_USER_CHECK(offset >= 0 && offset < static_cast<int32_t>(data.size()),
                     "index {} out of valid range, 0..{}", offset,
                     data.size() - 1);
    result = static_cast<uint8_t>(data.data()[offset]);
  }
};

// set_byte(bytea, offset, value) -> bytea
template<typename T>
struct PgSetByte {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(out_type<Varbinary>& result,
                                const arg_type<Varbinary>& data, int32_t offset,
                                int32_t value) {
    VELOX_USER_CHECK(offset >= 0 && offset < static_cast<int32_t>(data.size()),
                     "index {} out of valid range, 0..{}", offset,
                     data.size() - 1);
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
                                const arg_type<Varbinary>& data,
                                int64_t bit_offset) {
    int64_t byte_idx = bit_offset / 8;
    int bit_idx = bit_offset % 8;
    VELOX_USER_CHECK(
      byte_idx >= 0 && byte_idx < static_cast<int64_t>(data.size()),
      "index {} out of valid range", bit_offset);
    result = (static_cast<uint8_t>(data.data()[byte_idx]) >> (7 - bit_idx)) & 1;
  }
};

// set_bit(bytea, offset, value) -> bytea
template<typename T>
struct PgSetBit {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(out_type<Varbinary>& result,
                                const arg_type<Varbinary>& data,
                                int64_t bit_offset, int32_t value) {
    int64_t byte_idx = bit_offset / 8;
    int bit_idx = bit_offset % 8;
    VELOX_USER_CHECK(
      byte_idx >= 0 && byte_idx < static_cast<int64_t>(data.size()),
      "index {} out of valid range", bit_offset);
    result.resize(data.size());
    std::memcpy(result.data(), data.data(), data.size());
    auto& byte = result.data()[byte_idx];
    if (value) {
      byte |= (1 << (7 - bit_idx));
    } else {
      byte &= ~(1 << (7 - bit_idx));
    }
  }
};

// sha224(bytea) -> bytea
template<typename T>
struct PgSha224 {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(out_type<Varbinary>& result,
                                const arg_type<Varbinary>& data) {
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
  FOLLY_ALWAYS_INLINE void call(out_type<Varbinary>& result,
                                const arg_type<Varbinary>& data) {
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
  FOLLY_ALWAYS_INLINE void call(out_type<Varchar>& result,
                                const arg_type<Varbinary>& data,
                                const arg_type<Varchar>& encoding) {
    std::string_view enc(encoding.data(), encoding.size());
    VELOX_USER_CHECK(
      enc == "UTF8" || enc == "UTF-8" || enc == "utf8" || enc == "utf-8",
      "conversion from {} is not supported", enc);
    result.resize(data.size());
    std::memcpy(result.data(), data.data(), data.size());
  }
};

// convert_to(text, encoding) -> bytea
template<typename T>
struct PgConvertTo {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(out_type<Varbinary>& result,
                                const arg_type<Varchar>& data,
                                const arg_type<Varchar>& encoding) {
    std::string_view enc(encoding.data(), encoding.size());
    VELOX_USER_CHECK(
      enc == "UTF8" || enc == "UTF-8" || enc == "utf8" || enc == "utf-8",
      "conversion to {} is not supported", enc);
    result.resize(data.size());
    std::memcpy(result.data(), data.data(), data.size());
  }
};

// pg_client_encoding() -> name
template<typename T>
struct PgClientEncoding {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(out_type<Varchar>& result) {
    result.copy_from("UTF8");
  }
};

}  // namespace

void registerStringExtraFunctions(const std::string& prefix) {
  registerFunction<PgMd5, Varchar, Varchar>({prefix + "md5"});
  registerFunction<PgToHex, Varchar, int32_t>({prefix + "to_hex"});
  registerFunction<PgToHex, Varchar, int64_t>({prefix + "to_hex"});
  registerFunction<PgToBin, Varchar, int32_t>({prefix + "to_bin"});
  registerFunction<PgToBin, Varchar, int64_t>({prefix + "to_bin"});
  registerFunction<PgToOct, Varchar, int32_t>({prefix + "to_oct"});
  registerFunction<PgToOct, Varchar, int64_t>({prefix + "to_oct"});
  registerFunction<PgRight, Varchar, Varchar, int32_t>({prefix + "right"});
  registerFunction<PgStringToArray, Array<Varchar>, Varchar, Varchar>(
    {prefix + "string_to_array"});
  registerFunction<PgEncode, Varchar, Varbinary, Varchar>({prefix + "encode"});
  registerFunction<PgDecode, Varbinary, Varchar, Varchar>({prefix + "decode"});
  registerFunction<PgGetByte, int32_t, Varbinary, int32_t>(
    {prefix + "get_byte"});
  registerFunction<PgSetByte, Varbinary, Varbinary, int32_t, int32_t>(
    {prefix + "set_byte"});
  registerFunction<PgGetBit, int32_t, Varbinary, int64_t>({prefix + "get_bit"});
  registerFunction<PgSetBit, Varbinary, Varbinary, int64_t, int32_t>(
    {prefix + "set_bit"});
  registerFunction<PgSha224, Varbinary, Varbinary>({prefix + "sha224"});
  registerFunction<PgSha384, Varbinary, Varbinary>({prefix + "sha384"});
  registerFunction<PgConvertFrom, Varchar, Varbinary, Varchar>(
    {prefix + "convert_from"});
  registerFunction<PgConvertTo, Varbinary, Varchar, Varchar>(
    {prefix + "convert_to"});
  registerFunction<PgClientEncoding, Varchar>({prefix + "client_encoding"});
}

}  // namespace sdb::pg::functions
