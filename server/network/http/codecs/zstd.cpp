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

#include <zstd.h>

#include <iresearch/utils/zstd_context.hpp>

#include "network/http/codecs/codec.h"

namespace sdb::network::http {
namespace {

// https://facebook.github.io/zstd/zstd_manual.html#Chapter9
class ZstdEncoder final : public ContentEncoder {
 public:
  void Encode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    ZSTD_inBuffer input{in.data(), in.size(), 0};
    const auto mode = finish ? ZSTD_e_end : ZSTD_e_continue;
    for (;;) {
      ZSTD_outBuffer output{_out.data(), _out.size(), 0};
      const size_t remaining =
        ZSTD_compressStream2(_cctx.get(), &output, &input, mode);
      if (ZSTD_isError(remaining)) {
        ThrowCodecError("zstd", ZSTD_getErrorName(remaining));
      }
      if (output.pos != 0) {
        sink({static_cast<const char*>(output.dst), output.pos});
      }
      if (finish ? remaining == 0 : input.pos == input.size) {
        return;
      }
    }
  }

 private:
  irs::utils::ZstdCCtxPtr _cctx = irs::utils::MakeZstdCCtx();
  std::array<uint8_t, kOutBlock> _out;
};

class ZstdDecoder final : public ContentDecoder {
 public:
  void Decode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
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

 private:
  irs::utils::ZstdDCtxPtr _dctx = irs::utils::MakeZstdDCtx();
  size_t _pending = 1;
  std::array<uint8_t, kOutBlock> _out;
};

}  // namespace

std::unique_ptr<ContentEncoder> MakeZstdEncoder() {
  return std::make_unique<ZstdEncoder>();
}

std::unique_ptr<ContentDecoder> MakeZstdDecoder() {
  return std::make_unique<ZstdDecoder>();
}

}  // namespace sdb::network::http
