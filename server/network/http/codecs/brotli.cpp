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

#include <brotli/decode.h>
#include <brotli/encode.h>

#include <algorithm>
#include <iresearch/utils/assert.hpp>

#include "network/http/codecs/codec.h"

namespace sdb::network::http {
namespace {

inline constexpr int kDefaultQuality = 5;

}  // namespace

// https://github.com/google/brotli/blob/master/c/include/brotli/encode.h
BrotliEncoder::BrotliEncoder(int level)
  : _state{BrotliEncoderCreateInstance(CodecPool::Allocate, CodecPool::Free,
                                       nullptr)} {
  SDB_ASSERT(kBrotliLevels.Contains(level));
  if (_state == nullptr) {
    ThrowCodecError("br", "cannot initialize the encoder");
  }
  BrotliEncoderSetParameter(
    _state, BROTLI_PARAM_QUALITY,
    static_cast<uint32_t>(level == kNoLevel ? kDefaultQuality : level));
}

BrotliEncoder::~BrotliEncoder() { BrotliEncoderDestroyInstance(_state); }

void BrotliEncoder::Encode(std::string_view in, bool finish,
                           message::Writer& out) {
  size_t avail_in = in.size();
  const auto* next_in = reinterpret_cast<const uint8_t*>(in.data());
  const auto op = finish ? BROTLI_OPERATION_FINISH : BROTLI_OPERATION_PROCESS;
  for (;;) {
    out.Write(kOutBlock, [&](uint8_t* dst) {
      size_t avail_out = kOutBlock;
      uint8_t* next_out = dst;
      if (!BrotliEncoderCompressStream(_state, op, &avail_in, &next_in,
                                       &avail_out, &next_out, nullptr)) {
        ThrowCodecError("br", "compression failed");
      }
      return kOutBlock - avail_out;
    });
    if (avail_in == 0 && !BrotliEncoderHasMoreOutput(_state) &&
        (!finish || BrotliEncoderIsFinished(_state))) {
      return;
    }
  }
}

void BrotliEncoder::EncodeAll(std::string_view in, message::Writer& out) {
  const size_t bound = BrotliEncoderMaxCompressedSize(in.size());
  if (bound == 0) {
    Encode(in, true, out);
    return;
  }
  BrotliEncoderSetParameter(
    _state, BROTLI_PARAM_SIZE_HINT,
    static_cast<uint32_t>(std::min<size_t>(in.size(), 1u << 30)));
  out.Write(bound, [&](uint8_t* dst) {
    size_t avail_in = in.size();
    const auto* next_in = reinterpret_cast<const uint8_t*>(in.data());
    size_t avail_out = bound;
    uint8_t* next_out = dst;
    if (!BrotliEncoderCompressStream(_state, BROTLI_OPERATION_FINISH, &avail_in,
                                     &next_in, &avail_out, &next_out,
                                     nullptr) ||
        !BrotliEncoderIsFinished(_state)) {
      ThrowCodecError("br", "compression failed");
    }
    return bound - avail_out;
  });
}

// https://github.com/google/brotli/blob/master/c/include/brotli/decode.h
BrotliDecoder::BrotliDecoder()
  : _state{BrotliDecoderCreateInstance(CodecPool::Allocate, CodecPool::Free,
                                       nullptr)},
    _out{kOutBlock} {
  if (_state == nullptr) {
    ThrowCodecError("br", "cannot initialize the decoder");
  }
}

BrotliDecoder::~BrotliDecoder() { BrotliDecoderDestroyInstance(_state); }

void BrotliDecoder::Decode(std::string_view in, bool finish, DecodeSink sink) {
  size_t avail_in = in.size();
  const auto* next_in = reinterpret_cast<const uint8_t*>(in.data());
  for (;;) {
    if (_done) {
      if (avail_in != 0) {
        ThrowCorrupt("br", "data after the end of the stream");
      }
      break;
    }
    size_t avail_out = _out.Size();
    uint8_t* next_out = _out.Data();
    const auto rc = BrotliDecoderDecompressStream(
      _state, &avail_in, &next_in, &avail_out, &next_out, nullptr);
    const size_t produced = _out.Size() - avail_out;
    if (produced != 0) {
      sink({reinterpret_cast<const char*>(_out.Data()), produced});
    }
    if (rc == BROTLI_DECODER_RESULT_ERROR) {
      ThrowCorrupt("br",
                   BrotliDecoderErrorString(BrotliDecoderGetErrorCode(_state)));
    }
    if (rc == BROTLI_DECODER_RESULT_SUCCESS) {
      _done = true;
      continue;
    }
    if (rc == BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT) {
      break;
    }
  }
  if (finish && !_done) {
    ThrowCorrupt("br", "truncated body");
  }
}

}  // namespace sdb::network::http
