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

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include <iresearch/utils/assert.hpp>

#include "network/http/codecs/codec.h"

namespace sdb::network::http {
namespace {

static_assert(kZstdLevels.min == -ZSTD_TARGETLENGTH_MAX);

// https://www.rfc-editor.org/rfc/rfc8878#section-3.1.1.1.2 : an HTTP decoder
// need not accept windows above 8 MiB.
inline constexpr int kMaxWindowLog = 23;

}  // namespace

void ZstdEncoder::Deleter::operator()(ZSTD_CCtx* cctx) const noexcept {
  ZSTD_freeCCtx(cctx);
}

void ZstdDecoder::Deleter::operator()(ZSTD_DCtx* dctx) const noexcept {
  ZSTD_freeDCtx(dctx);
}

// https://facebook.github.io/zstd/zstd_manual.html#Chapter9
ZstdEncoder::ZstdEncoder(int level) : _cctx{ZSTD_createCCtx()} {
  SDB_ASSERT(kZstdLevels.Contains(level));
  if (!_cctx) {
    ThrowCodecError("zstd", "cannot initialize the encoder");
  }
  const size_t rc =
    ZSTD_CCtx_setParameter(_cctx.get(), ZSTD_c_compressionLevel, level);
  if (ZSTD_isError(rc)) {
    ThrowCodecError("zstd", ZSTD_getErrorName(rc));
  }
}

void ZstdEncoder::Encode(std::string_view in, bool finish,
                         message::Writer& out) {
  auto* cctx = _cctx.get();
  ZSTD_inBuffer input{in.data(), in.size(), 0};
  const auto mode = finish ? ZSTD_e_end : ZSTD_e_continue;
  size_t remaining = 0;
  do {
    out.Write(kOutBlock, [&](uint8_t* dst) {
      ZSTD_outBuffer output{dst, kOutBlock, 0};
      remaining = ZSTD_compressStream2(cctx, &output, &input, mode);
      if (ZSTD_isError(remaining)) {
        ThrowCodecError("zstd", ZSTD_getErrorName(remaining));
      }
      return output.pos;
    });
  } while (finish ? remaining != 0 : input.pos != input.size);
}

void ZstdEncoder::EncodeAll(std::string_view in, message::Writer& out) {
  const size_t bound = ZSTD_compressBound(in.size());
  out.Write(bound, [&](uint8_t* dst) {
    const size_t size =
      ZSTD_compress2(_cctx.get(), dst, bound, in.data(), in.size());
    if (ZSTD_isError(size)) {
      ThrowCodecError("zstd", ZSTD_getErrorName(size));
    }
    return size;
  });
}

ZstdDecoder::ZstdDecoder() : _dctx{ZSTD_createDCtx()} {
  if (!_dctx) {
    ThrowCodecError("zstd", "cannot initialize the decoder");
  }
  ZSTD_DCtx_setParameter(_dctx.get(), ZSTD_d_windowLogMax, kMaxWindowLog);
}

void ZstdDecoder::Decode(std::string_view in, bool finish, DecodeSink sink) {
  ZSTD_inBuffer input{in.data(), in.size(), 0};
  size_t consumed = 0;
  for (;;) {
    ZSTD_outBuffer output{_out.data(), _out.size(), 0};
    const size_t rc = ZSTD_decompressStream(_dctx.get(), &output, &input);
    if (ZSTD_isError(rc)) {
      ThrowCorrupt("zstd", ZSTD_getErrorName(rc));
    }
    if (output.pos != 0) {
      sink({static_cast<const char*>(output.dst), output.pos});
    }
    if (input.pos != consumed || output.pos != 0) {
      _pending = rc;
    }
    consumed = input.pos;
    if (input.pos == input.size && output.pos < output.size) {
      break;
    }
  }
  if (finish && _pending != 0) {
    ThrowCorrupt("zstd", "truncated body");
  }
}

}  // namespace sdb::network::http
