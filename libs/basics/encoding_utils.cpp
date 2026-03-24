////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "encoding_utils.h"

#include <zconf.h>
#include <zlib.h>

#include "basics/buffer.h"
#include "basics/debugging.h"
#include "basics/endian.h"
#include "basics/errors.h"
#include "basics/string_buffer.h"
#include "basics/string_utils.h"

#define LZ4_STATIC_LINKING_ONLY
#include <lz4.h>

#include <string>

namespace sdb::encoding {
namespace {

constexpr size_t kMaxUncompressedSize = 512 * 1024 * 1024;
constexpr size_t kLz4HeaderLen = 1 + sizeof(uint32_t);

template<typename T>
int UncompressStream(z_stream& strm, T& uncompressed) noexcept {
  SDB_ASSERT(strm.opaque == Z_NULL);

  // check that uncompressed size will be in the allowed range
  if (strm.total_out > kMaxUncompressedSize) {
    return Z_MEM_ERROR;
  }

  char outbuffer[32768];
  int ret = Z_DATA_ERROR;

  try {
    uncompressed.reserve(static_cast<size_t>(strm.total_out));
    do {
      strm.next_out = reinterpret_cast<Bytef*>(outbuffer);
      strm.avail_out = sizeof(outbuffer);

      // ret will change from Z_OK to Z_STREAM_END if we have read all data
      ret = inflate(&strm, Z_NO_FLUSH);

      if (uncompressed.size() < strm.total_out) {
        uncompressed.append({outbuffer, strm.total_out - uncompressed.size()});
      }
      if (uncompressed.size() > kMaxUncompressedSize) {
        // should not happen
        uncompressed.clear();
        return Z_DATA_ERROR;
      }
    } while (ret == Z_OK);
  } catch (...) {
    // we can get here only if uncompressed.reserve() or uncompressed.append()
    // throw
    uncompressed.clear();
    ret = Z_MEM_ERROR;
  }

  return ret;
}

template<typename T>
int CompressStream(z_stream& strm, T& compressed) noexcept {
  SDB_ASSERT(strm.opaque == Z_NULL);

  char outbuffer[32768];
  int ret = Z_DATA_ERROR;

  Bytef* p = reinterpret_cast<Bytef*>(strm.next_in);
  Bytef* e = p + strm.avail_in;

  try {
    while (p < e) {
      int flush;
      strm.next_in = p;

      if (e - p > static_cast<ptrdiff_t>(sizeof(outbuffer))) {
        strm.avail_in = static_cast<uInt>(sizeof(outbuffer));
        flush = Z_NO_FLUSH;
      } else {
        strm.avail_in = static_cast<uInt>(e - p);
        flush = Z_FINISH;
      }
      p += strm.avail_in;

      do {
        strm.next_out = reinterpret_cast<Bytef*>(outbuffer);
        strm.avail_out = sizeof(outbuffer);

        // ret will change from Z_OK to Z_STREAM_END if we have read all data
        ret = deflate(&strm, flush);

        if (ret == Z_STREAM_ERROR) {
          break;
        }
        compressed.append({&outbuffer[0], sizeof(outbuffer) - strm.avail_out});

      } while (strm.avail_out == 0);
    }
  } catch (...) {
    // we can get here only if compressed.append() throws
    compressed.clear();
    ret = Z_MEM_ERROR;
  }

  return ret;
}

template<typename T>
ErrorCode UncompressWrapper(
  const uint8_t* compressed, size_t compressed_len, T& uncompressed,
  const std::function<ErrorCode(z_stream& strm)>& init_cb) {
  uncompressed.clear();

  z_stream strm;
  memset(&strm, 0, sizeof(strm));
  strm.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(compressed));
  strm.avail_in = static_cast<uInt>(compressed_len);

  ErrorCode res = init_cb(strm);
  if (res != ERROR_OK) {
    return res;
  }

  int ret = UncompressStream(strm, uncompressed);
  inflateEnd(&strm);

  switch (ret) {
    case Z_STREAM_END:
      return ERROR_OK;
    case Z_MEM_ERROR:
      return ERROR_OUT_OF_MEMORY;
    default:
      return ERROR_INTERNAL;
  }
}

}  // namespace

LZ4_stream_t* MakeLZ4Stream() {
  // TODO(mbkkt) we should use it in iresearch
  static_assert(sizeof(LZ4_stream_t) >= 1024);
  static thread_local auto gStream = [] {
    LZ4_stream_t stream;
    LZ4_initStream(&stream, sizeof(stream));
    return stream;
  }();
  return &gStream;
}

template<typename T>
ErrorCode GZipUncompress(const uint8_t* compressed, size_t compressed_len,
                         T& uncompressed) {
  if (compressed_len == 0) {
    // empty input
    return ERROR_OK;
  }

  return UncompressWrapper<T>(
    compressed, compressed_len, uncompressed, [](z_stream& strm) -> ErrorCode {
      if (inflateInit2(&strm, (16 + MAX_WBITS)) != Z_OK) {
        return ERROR_OUT_OF_MEMORY;
      }
      return ERROR_OK;
    });
}

template<typename T>
ErrorCode ZLibInflate(const uint8_t* compressed, size_t compressed_len,
                      T& uncompressed) {
  return UncompressWrapper<T>(compressed, compressed_len, uncompressed,
                              [](z_stream& strm) -> ErrorCode {
                                if (inflateInit(&strm) != Z_OK) {
                                  return ERROR_OUT_OF_MEMORY;
                                }
                                return ERROR_OK;
                              });
}

template<typename T>
ErrorCode Lz4Uncompress(const uint8_t* compressed, size_t compressed_len,
                        T& uncompressed) {
  if (compressed_len <= kLz4HeaderLen) {
    // empty/bad input.
    return ERROR_BAD_PARAMETER;
  }

  size_t initial = uncompressed.size();

  uint32_t uncompressed_len;
  // uncompressed size is encoded in bytes 1-4, starting from offset 0,
  // encoded as a uint32_t in big endian format
  memcpy(&uncompressed_len, compressed + 1, sizeof(uncompressed_len));
  uncompressed_len = basics::BigToHost<uint32_t>(uncompressed_len);

  if (uncompressed_len == 0 ||
      uncompressed_len >= static_cast<size_t>(LZ4_MAX_INPUT_SIZE)) {
    // uncompressed size is larger than what LZ4 can actually compress.
    // suspicious!
    return ERROR_BAD_PARAMETER;
  }

  if constexpr (std::is_same_v<T, std::string>) {
    // TODO(mbkkt) why amortized?
    basics::StrResizeAmortized(uncompressed, initial + uncompressed_len);
  } else if constexpr (std::is_same_v<T, vpack::BufferUInt8>) {
    uncompressed.reserve(initial + uncompressed_len);
  } else {
    static_assert(std::is_same_v<T, basics::StringBuffer>);
    uncompressed.resizeAdditional(initial, uncompressed_len);
  }

  // uncompress directly into the result
  // this should not go wrong because we have a big enough output buffer.
  int size = LZ4_decompress_safe(
    reinterpret_cast<const char*>(compressed) + kLz4HeaderLen,
    const_cast<char*>(reinterpret_cast<const char*>(uncompressed.data())),
    static_cast<int>(compressed_len - kLz4HeaderLen),
    static_cast<int>(uncompressed_len));
  SDB_ASSERT(size > 0);
  SDB_ASSERT(size < LZ4_MAX_INPUT_SIZE);
  SDB_ASSERT(uncompressed_len == static_cast<size_t>(size));
  if (size != static_cast<int>(uncompressed_len)) {
    return ERROR_BAD_PARAMETER;
  }

  if constexpr (std::is_same_v<T, vpack::BufferUInt8>) {
    uncompressed.resetTo(initial + uncompressed_len);
  }

  return ERROR_OK;
}

template<typename T>
ErrorCode GZipCompress(const uint8_t* uncompressed, size_t uncompressed_len,
                       T& compressed) {
  compressed.clear();

  z_stream strm;
  memset(&strm, 0, sizeof(strm));
  strm.next_in =
    const_cast<Bytef*>(reinterpret_cast<const Bytef*>(uncompressed));
  strm.avail_in = static_cast<uInt>(uncompressed_len);

  // from https://stackoverflow.com/a/57699371/3042070:
  //   hard to believe they don't have a macro for gzip encoding, "Add 16" is
  //   the best thing zlib can do: "Add 16 to windowBits to write a simple gzip
  //   header and trailer around the compressed data instead of a zlib wrapper"
  if (deflateInit2(&strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 | 16, 8,
                   Z_DEFAULT_STRATEGY) != Z_OK) {
    return ERROR_OUT_OF_MEMORY;
  }

  int ret = CompressStream(strm, compressed);
  deflateEnd(&strm);

  switch (ret) {
    case Z_STREAM_END:
      return ERROR_OK;
    case Z_MEM_ERROR:
      return ERROR_OUT_OF_MEMORY;
    default:
      return ERROR_INTERNAL;
  }
}

template<typename T>
ErrorCode ZLibDeflate(const uint8_t* uncompressed, size_t uncompressed_len,
                      T& compressed) {
  compressed.clear();

  if (uncompressed_len == 0) {
    // empty input
    return ERROR_OK;
  }

  z_stream strm;
  memset(&strm, 0, sizeof(strm));
  strm.next_in =
    const_cast<Bytef*>(reinterpret_cast<const Bytef*>(uncompressed));
  strm.avail_in = static_cast<uInt>(uncompressed_len);

  if (deflateInit(&strm, Z_DEFAULT_COMPRESSION) != Z_OK) {
    return ERROR_OUT_OF_MEMORY;
  }

  int ret = CompressStream(strm, compressed);
  deflateEnd(&strm);

  switch (ret) {
    case Z_STREAM_END:
      return ERROR_OK;
    case Z_MEM_ERROR:
      return ERROR_OUT_OF_MEMORY;
    default:
      return ERROR_INTERNAL;
  }
}

template<typename T>
ErrorCode Lz4Compress(const uint8_t* uncompressed, size_t uncompressed_len,
                      T& compressed) {
  SDB_ASSERT(uncompressed_len > 0);

  compressed.clear();
  if (uncompressed_len >= static_cast<size_t>(LZ4_MAX_INPUT_SIZE)) {
    return ERROR_BAD_PARAMETER;
  }
  int max_len = LZ4_COMPRESSBOUND(static_cast<int>(uncompressed_len));
  if (max_len <= 0) {
    return ERROR_BAD_PARAMETER;
  }
  // store compressed value.
  // the compressed value is prepended by the following header:
  // - byte 0: hard-coded to 0x01 (can be used as a version number later)
  // - byte 1-4: uncompressed size, encoded as a uint32_t in big endian order
  int compressed_len = kLz4HeaderLen + max_len;
  if constexpr (std::is_same_v<T, std::string>) {
    // TODO(mbkkt) maybe amortized?
    basics::StrResize(compressed, compressed_len);
  } else if constexpr (std::is_same_v<T, vpack::BufferUInt8>) {
    compressed.reserve(compressed_len);
  } else {
    static_assert(std::is_same_v<T, basics::StringBuffer>);
    // TODO(mbkkt) maybe amortized?
    basics::StrResize(compressed.Impl(), compressed_len);
  }
  auto* data = reinterpret_cast<char*>(compressed.data());
  data[0] = 0x01U;  // version

  auto original_len = static_cast<uint32_t>(uncompressed_len);
  original_len = basics::HostToBig(original_len);
  memcpy(data + 1, &original_len, sizeof(original_len));

  // compress data into output buffer. writes start at byte 5.
  compressed_len = LZ4_compress_fast_extState_fastReset(
    MakeLZ4Stream(), reinterpret_cast<const char*>(uncompressed),
    data + kLz4HeaderLen, static_cast<int>(uncompressed_len),
    static_cast<int>(max_len - kLz4HeaderLen),
    /*accelerationFactor*/ 1);

  if (compressed_len <= 0) {
    return ERROR_INTERNAL;
  }

  compressed_len += kLz4HeaderLen;
  if constexpr (std::is_same_v<T, vpack::BufferUInt8>) {
    compressed.resetTo(compressed_len);
  } else {
    static_assert(std::is_same_v<T, std::string> ||
                  std::is_same_v<T, basics::StringBuffer>);
    compressed.erase(compressed_len);
  }

  return ERROR_OK;
}

// template instantiations

// uncompress methods
template ErrorCode GZipUncompress<vpack::BufferUInt8>(
  const uint8_t* compressed, size_t compressedLen,
  vpack::BufferUInt8& uncompressed);

template ErrorCode GZipUncompress<basics::StringBuffer>(
  const uint8_t* compressed, size_t compressedLen,
  basics::StringBuffer& uncompressed);

template ErrorCode GZipUncompress<std::string>(const uint8_t* compressed,
                                               size_t compressedLen,
                                               std::string& uncompressed);

template ErrorCode ZLibInflate<vpack::BufferUInt8>(
  const uint8_t* compressed, size_t compressedLen,
  vpack::BufferUInt8& uncompressed);

template ErrorCode ZLibInflate<basics::StringBuffer>(
  const uint8_t* compressed, size_t compressedLen,
  basics::StringBuffer& uncompressed);

template ErrorCode ZLibInflate<std::string>(const uint8_t* compressed,
                                            size_t compressedLen,
                                            std::string& uncompressed);

template ErrorCode Lz4Uncompress<vpack::BufferUInt8>(
  const uint8_t* compressed, size_t compressedLen,
  vpack::BufferUInt8& uncompressed);

template ErrorCode Lz4Uncompress<basics::StringBuffer>(
  const uint8_t* compressed, size_t compressedLen,
  basics::StringBuffer& uncompressed);

// compression methods
template ErrorCode GZipCompress<vpack::BufferUInt8>(
  const uint8_t* uncompressed, size_t uncompressedLen,
  vpack::BufferUInt8& compressed);

template ErrorCode GZipCompress<basics::StringBuffer>(
  const uint8_t* uncompressed, size_t uncompressedLen,
  basics::StringBuffer& compressed);

template ErrorCode ZLibDeflate<vpack::BufferUInt8>(
  const uint8_t* uncompressed, size_t uncompressedLen,
  vpack::BufferUInt8& compressed);

template ErrorCode ZLibDeflate<basics::StringBuffer>(
  const uint8_t* uncompressed, size_t uncompressedLen,
  basics::StringBuffer& compressed);

template ErrorCode ZLibDeflate<std::string>(const uint8_t* uncompressed,
                                            size_t uncompressedLen,
                                            std::string& compressed);

template ErrorCode Lz4Compress<vpack::BufferUInt8>(
  const uint8_t* uncompressed, size_t uncompressedLen,
  vpack::BufferUInt8& compressed);

template ErrorCode Lz4Compress<basics::StringBuffer>(
  const uint8_t* uncompressed, size_t uncompressedLen,
  basics::StringBuffer& compressed);

}  // namespace sdb::encoding
