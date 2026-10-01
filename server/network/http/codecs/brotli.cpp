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

#include "network/http/codecs/codec.h"

namespace sdb::network::http {
namespace {

// https://github.com/google/brotli/blob/master/c/include/brotli/encode.h
class BrotliEncoder final : public ContentEncoder {
 public:
  BrotliEncoder()
    : _state{BrotliEncoderCreateInstance(nullptr, nullptr, nullptr)} {
    if (_state == nullptr) {
      ThrowCodecError("br", "cannot initialize the encoder");
    }
    BrotliEncoderSetParameter(_state, BROTLI_PARAM_QUALITY, kQuality);
  }

  ~BrotliEncoder() override { BrotliEncoderDestroyInstance(_state); }

  void Encode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    size_t avail_in = in.size();
    const auto* next_in = reinterpret_cast<const uint8_t*>(in.data());
    const auto op = finish ? BROTLI_OPERATION_FINISH : BROTLI_OPERATION_PROCESS;
    for (;;) {
      size_t avail_out = _out.size();
      uint8_t* next_out = _out.data();
      if (!BrotliEncoderCompressStream(_state, op, &avail_in, &next_in,
                                       &avail_out, &next_out, nullptr)) {
        ThrowCodecError("br", "compression failed");
      }
      const size_t produced = _out.size() - avail_out;
      if (produced != 0) {
        sink({reinterpret_cast<const char*>(_out.data()), produced});
      }
      if (avail_in == 0 && !BrotliEncoderHasMoreOutput(_state) &&
          (!finish || BrotliEncoderIsFinished(_state))) {
        return;
      }
    }
  }

 private:
  static constexpr uint32_t kQuality = 5;

  BrotliEncoderState* _state;
  std::array<uint8_t, kOutBlock> _out;
};

// https://github.com/google/brotli/blob/master/c/include/brotli/decode.h
class BrotliDecoder final : public ContentDecoder {
 public:
  BrotliDecoder()
    : _state{BrotliDecoderCreateInstance(nullptr, nullptr, nullptr)} {
    if (_state == nullptr) {
      ThrowCodecError("br", "cannot initialize the decoder");
    }
  }

  ~BrotliDecoder() override { BrotliDecoderDestroyInstance(_state); }

  void Decode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    size_t avail_in = in.size();
    const auto* next_in = reinterpret_cast<const uint8_t*>(in.data());
    for (;;) {
      if (_done) {
        if (avail_in != 0) {
          ThrowCorrupt("br", "data after the end of the stream");
        }
        break;
      }
      size_t avail_out = _out.size();
      uint8_t* next_out = _out.data();
      const auto rc = BrotliDecoderDecompressStream(
        _state, &avail_in, &next_in, &avail_out, &next_out, nullptr);
      const size_t produced = _out.size() - avail_out;
      if (produced != 0) {
        sink({reinterpret_cast<const char*>(_out.data()), produced});
      }
      if (rc == BROTLI_DECODER_RESULT_ERROR) {
        ThrowCorrupt(
          "br", BrotliDecoderErrorString(BrotliDecoderGetErrorCode(_state)));
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

 private:
  BrotliDecoderState* _state;
  bool _done = false;
  std::array<uint8_t, kOutBlock> _out;
};

}  // namespace

std::unique_ptr<ContentEncoder> MakeBrotliEncoder() {
  return std::make_unique<BrotliEncoder>();
}

std::unique_ptr<ContentDecoder> MakeBrotliDecoder() {
  return std::make_unique<BrotliDecoder>();
}

}  // namespace sdb::network::http
