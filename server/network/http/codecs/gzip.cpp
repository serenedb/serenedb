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

#include <zlib.h>

#include <iresearch/utils/assert.hpp>

#include "network/http/codecs/codec.h"

namespace sdb::network::http {
namespace {

inline constexpr int kDefaultLevel = 6;

static_assert(kDeflateLevels.min == Z_BEST_SPEED &&
              kDeflateLevels.max == Z_BEST_COMPRESSION);

// https://www.zlib.net/manual.html#Advanced : windowBits 15 + 16 selects the
// gzip wrapper around deflate.
int WindowBits(ContentCoding coding) {
  SDB_ASSERT(coding == ContentCoding::Gzip || coding == ContentCoding::Deflate);
  return coding == ContentCoding::Gzip ? 15 + 16 : 15;
}

bool IsZlibHeader(std::string_view in) {
  const auto cmf = static_cast<uint8_t>(in[0]);
  if ((cmf & 0x0F) != Z_DEFLATED || (cmf >> 4) > 7) {
    return false;
  }
  return in.size() < 2 || (cmf * 256 + static_cast<uint8_t>(in[1])) % 31 == 0;
}

}  // namespace

ZlibEncoder::ZlibEncoder(ContentCoding coding, int level)
  : _stream{}, _coding{coding} {
  SDB_ASSERT(kDeflateLevels.Contains(level));
  const int rc =
    deflateInit2(&_stream, level == kNoLevel ? kDefaultLevel : level,
                 Z_DEFLATED, WindowBits(coding), 8, Z_DEFAULT_STRATEGY);
  if (rc != Z_OK) {
    ThrowCodecError(ContentCodingToken(coding), zError(rc));
  }
}

ZlibEncoder::~ZlibEncoder() { deflateEnd(&_stream); }

void ZlibEncoder::Encode(std::string_view in, bool finish,
                         message::Writer& out) {
  auto& stream = _stream;
  stream.next_in =
    const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data()));
  stream.avail_in = static_cast<uInt>(in.size());
  const int flush = finish ? Z_FINISH : Z_NO_FLUSH;
  int rc = Z_OK;
  do {
    out.Write(kOutBlock, [&](uint8_t* dst) {
      stream.next_out = dst;
      stream.avail_out = static_cast<uInt>(kOutBlock);
      rc = deflate(&stream, flush);
      // Z_BUF_ERROR only reports "no progress possible", which is expected
      // once the input is drained; anything else is fatal.
      if (rc != Z_OK && rc != Z_STREAM_END && rc != Z_BUF_ERROR) {
        ThrowCodecError(ContentCodingToken(_coding), zError(rc));
      }
      return kOutBlock - stream.avail_out;
    });
  } while (rc != Z_BUF_ERROR && stream.avail_out == 0);
  if (stream.avail_in != 0) {
    ThrowCodecError(ContentCodingToken(_coding), "input not consumed");
  }
}

void ZlibEncoder::EncodeAll(std::string_view in, message::Writer& out) {
  auto& stream = _stream;
  const size_t bound = deflateBound(&stream, in.size());
  out.Write(bound, [&](uint8_t* dst) {
    stream.next_in =
      const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data()));
    stream.avail_in = static_cast<uInt>(in.size());
    stream.next_out = dst;
    stream.avail_out = static_cast<uInt>(bound);
    if (deflate(&stream, Z_FINISH) != Z_STREAM_END) {
      ThrowCodecError(ContentCodingToken(_coding),
                      "output exceeds the deflate bound");
    }
    return bound - stream.avail_out;
  });
}

ZlibDecoder::ZlibDecoder(ContentCoding coding) : _stream{}, _coding{coding} {
  if (inflateInit2(&_stream, WindowBits(coding)) != Z_OK) {
    ThrowCodecError(ContentCodingToken(coding),
                    "cannot initialize the decoder");
  }
}

ZlibDecoder::~ZlibDecoder() { inflateEnd(&_stream); }

void ZlibDecoder::Decode(std::string_view in, bool finish, DecodeSink sink) {
  auto& stream = _stream;
  const auto name = ContentCodingToken(_coding);
  if (_coding == ContentCoding::Deflate && !_started && !in.empty()) {
    _started = true;
    if (!IsZlibHeader(in) && inflateReset2(&stream, -15) != Z_OK) {
      ThrowCorrupt(name, "cannot initialize the decoder");
    }
  }
  stream.next_in =
    const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data()));
  stream.avail_in = static_cast<uInt>(in.size());
  for (;;) {
    if (_done) {
      if (stream.avail_in == 0) {
        break;
      }
      if (_coding != ContentCoding::Gzip) {
        ThrowCorrupt(name, "data after the end of the stream");
      }
      if (inflateReset(&stream) != Z_OK) {
        ThrowCorrupt(name, "cannot start the next member");
      }
      _done = false;
    }
    stream.next_out = _out.data();
    stream.avail_out = static_cast<uInt>(_out.size());
    const int rc = inflate(&stream, Z_NO_FLUSH);
    const size_t produced = _out.size() - stream.avail_out;
    if (produced != 0) {
      sink({reinterpret_cast<const char*>(_out.data()), produced});
    }
    if (rc == Z_STREAM_END) {
      _done = true;
      continue;
    }
    if (rc == Z_BUF_ERROR) {
      break;
    }
    if (rc != Z_OK) {
      ThrowCorrupt(name, stream.msg ? stream.msg : zError(rc));
    }
    if (stream.avail_in == 0 && stream.avail_out != 0) {
      break;
    }
  }
  if (finish && !_done) {
    ThrowCorrupt(name, "truncated body");
  }
}

}  // namespace sdb::network::http
