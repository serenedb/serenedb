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

#include <iresearch/utils/string_utils.hpp>
#include <iresearch/utils/zstd_context.hpp>

#include "network/http/codecs/codec.h"

namespace sdb::network::http {
namespace {

// https://facebook.github.io/zstd/zstd_manual.html#Chapter9
struct CompressState {
  CompressState() {}

  bool Reset() noexcept {
    return !ZSTD_isError(ZSTD_CCtx_reset(cctx.get(), ZSTD_reset_session_only));
  }

  irs::utils::ZstdCCtxPtr cctx = irs::utils::MakeZstdCCtx();
  std::array<uint8_t, kOutBlock> out;
};

// https://www.rfc-editor.org/rfc/rfc8878#section-3.1.1.1.2 : an HTTP decoder
// need not accept windows above 8 MiB.
inline constexpr int kMaxWindowLog = 23;

struct DecompressState {
  DecompressState() {
    ZSTD_DCtx_setParameter(dctx.get(), ZSTD_d_windowLogMax, kMaxWindowLog);
  }

  bool Reset() noexcept {
    return !ZSTD_isError(ZSTD_DCtx_reset(dctx.get(), ZSTD_reset_session_only));
  }

  irs::utils::ZstdDCtxPtr dctx = irs::utils::MakeZstdDCtx();
  std::array<uint8_t, kOutBlock> out;
};

class ZstdEncoder final : public ContentEncoder {
 public:
  explicit ZstdEncoder(std::optional<int> level) {
    const size_t rc =
      ZSTD_CCtx_setParameter(_state->cctx.get(), ZSTD_c_compressionLevel,
                             ClampLevel(level, ZSTD_CLEVEL_DEFAULT,
                                        ZSTD_minCLevel(), ZSTD_maxCLevel()));
    if (ZSTD_isError(rc)) {
      ThrowCodecError("zstd", ZSTD_getErrorName(rc));
    }
  }

  void Encode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    auto& out = _state->out;
    ZSTD_inBuffer input{in.data(), in.size(), 0};
    const auto mode = finish ? ZSTD_e_end : ZSTD_e_continue;
    for (;;) {
      ZSTD_outBuffer output{out.data(), out.size(), 0};
      const size_t remaining =
        ZSTD_compressStream2(_state->cctx.get(), &output, &input, mode);
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

  void EncodeAll(std::string_view in, std::string& out) override {
    irs::utils::StrResize(out, ZSTD_compressBound(in.size()));
    const size_t size = ZSTD_compress2(_state->cctx.get(), out.data(),
                                       out.size(), in.data(), in.size());
    if (ZSTD_isError(size)) {
      ThrowCodecError("zstd", ZSTD_getErrorName(size));
    }
    out.resize(size);
  }

 private:
  Pooled<CompressState> _state;
};

class ZstdDecoder final : public ContentDecoder {
 public:
  void Decode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    auto& out = _state->out;
    ZSTD_inBuffer input{in.data(), in.size(), 0};
    size_t consumed = 0;
    for (;;) {
      ZSTD_outBuffer output{out.data(), out.size(), 0};
      const size_t rc =
        ZSTD_decompressStream(_state->dctx.get(), &output, &input);
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
  Pooled<DecompressState> _state;
  size_t _pending = 1;
};

}  // namespace

std::unique_ptr<ContentEncoder> MakeZstdEncoder(std::optional<int> level) {
  return std::make_unique<ZstdEncoder>(level);
}

std::unique_ptr<ContentDecoder> MakeZstdDecoder() {
  return std::make_unique<ZstdDecoder>();
}

}  // namespace sdb::network::http
