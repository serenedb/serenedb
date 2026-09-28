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

#include <lz4frame.h>

#include <algorithm>
#include <vector>

#include "network/http/codecs/codec.h"

namespace sdb::network::http {
namespace {

// https://github.com/lz4/lz4/blob/dev/lib/lz4frame.h
class Lz4Encoder final : public ContentEncoder {
 public:
  Lz4Encoder() {
    const auto rc = LZ4F_createCompressionContext(&_cctx, LZ4F_VERSION);
    if (LZ4F_isError(rc)) {
      ThrowCodecError("lz4", LZ4F_getErrorName(rc));
    }
    _out.resize(std::max(LZ4F_compressBound(kSlice, &_prefs),
                         LZ4F_compressBound(0, &_prefs)));
  }

  ~Lz4Encoder() override { LZ4F_freeCompressionContext(_cctx); }

  void Encode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    if (!_started) {
      Emit(LZ4F_compressBegin(_cctx, _out.data(), _out.size(), &_prefs), sink);
      _started = true;
    }
    while (!in.empty()) {
      const auto slice = in.substr(0, kSlice);
      Emit(LZ4F_compressUpdate(_cctx, _out.data(), _out.size(), slice.data(),
                               slice.size(), nullptr),
           sink);
      in.remove_prefix(slice.size());
    }
    if (finish) {
      Emit(LZ4F_compressEnd(_cctx, _out.data(), _out.size(), nullptr), sink);
    }
  }

 private:
  static constexpr size_t kSlice = kOutBlock;

  void Emit(size_t rc, absl::FunctionRef<void(std::string_view)> sink) {
    if (LZ4F_isError(rc)) {
      ThrowCodecError("lz4", LZ4F_getErrorName(rc));
    }
    if (rc != 0) {
      sink({reinterpret_cast<const char*>(_out.data()), rc});
    }
  }

  LZ4F_cctx* _cctx = nullptr;
  LZ4F_preferences_t _prefs{};
  std::vector<uint8_t> _out;
  bool _started = false;
};

class Lz4Decoder final : public ContentDecoder {
 public:
  Lz4Decoder() {
    const auto rc = LZ4F_createDecompressionContext(&_dctx, LZ4F_VERSION);
    if (LZ4F_isError(rc)) {
      ThrowCodecError("lz4", LZ4F_getErrorName(rc));
    }
  }

  ~Lz4Decoder() override { LZ4F_freeDecompressionContext(_dctx); }

  void Decode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    for (;;) {
      size_t produced = _out.size();
      size_t consumed = in.size();
      const size_t rc = LZ4F_decompress(_dctx, _out.data(), &produced,
                                        in.data(), &consumed, nullptr);
      if (LZ4F_isError(rc)) {
        ThrowCorrupt("lz4", LZ4F_getErrorName(rc));
      }
      in.remove_prefix(consumed);
      if (produced != 0) {
        sink({reinterpret_cast<const char*>(_out.data()), produced});
      }
      if (consumed != 0 || produced != 0) {
        _pending = rc;
      }
      if (in.empty() && produced < _out.size()) {
        break;
      }
    }
    if (finish && _pending != 0) {
      ThrowCorrupt("lz4", "truncated body");
    }
  }

 private:
  LZ4F_dctx* _dctx = nullptr;
  size_t _pending = 1;
  std::array<uint8_t, kOutBlock> _out;
};

}  // namespace

std::unique_ptr<ContentEncoder> MakeLz4Encoder() {
  return std::make_unique<Lz4Encoder>();
}

std::unique_ptr<ContentDecoder> MakeLz4Decoder() {
  return std::make_unique<Lz4Decoder>();
}

}  // namespace sdb::network::http
