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
#include <iresearch/utils/string_utils.hpp>
#include <memory>

#include "network/http/codecs/codec.h"

namespace sdb::network::http {
namespace {

// https://github.com/lz4/lz4/blob/dev/lib/lz4frame.h
inline constexpr size_t kSlice = kOutBlock;
inline constexpr int kMaxLevel = 9;

struct CompressState {
  CompressState() {
    const auto rc = LZ4F_createCompressionContext(&cctx, LZ4F_VERSION);
    if (LZ4F_isError(rc)) {
      ThrowCodecError("lz4", LZ4F_getErrorName(rc));
    }
    out_size = std::max(LZ4F_compressBound(kSlice, &prefs),
                        LZ4F_compressBound(0, &prefs));
  }

  ~CompressState() { LZ4F_freeCompressionContext(cctx); }

  bool Reset() noexcept { return true; }

  LZ4F_cctx* cctx = nullptr;
  LZ4F_preferences_t prefs{};
  size_t out_size = 0;
};

struct DecompressState {
  DecompressState() {
    const auto rc = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
    if (LZ4F_isError(rc)) {
      ThrowCodecError("lz4", LZ4F_getErrorName(rc));
    }
  }

  ~DecompressState() { LZ4F_freeDecompressionContext(dctx); }

  bool Reset() noexcept {
    LZ4F_resetDecompressionContext(dctx);
    return true;
  }

  LZ4F_dctx* dctx = nullptr;
  std::array<uint8_t, kOutBlock> out;
};

class Lz4Encoder final : public ContentEncoder {
 public:
  explicit Lz4Encoder(int level) {
    _state->prefs.compressionLevel = ClampLevel(level, 0, 0, kMaxLevel);
  }

  void Encode(std::string_view in, bool finish, EncodeOutput& out) override {
    auto& state = *_state;
    if (!_started) {
      out.Write(LZ4F_HEADER_SIZE_MAX, [&](uint8_t* dst) {
        return Check(LZ4F_compressBegin(state.cctx, dst, LZ4F_HEADER_SIZE_MAX,
                                        &state.prefs));
      });
      _started = true;
    }
    while (!in.empty()) {
      const auto slice = in.substr(0, kSlice);
      out.Write(state.out_size, [&](uint8_t* dst) {
        return Check(LZ4F_compressUpdate(state.cctx, dst, state.out_size,
                                         slice.data(), slice.size(), nullptr));
      });
      in.remove_prefix(slice.size());
    }
    if (finish) {
      out.Write(state.out_size, [&](uint8_t* dst) {
        return Check(
          LZ4F_compressEnd(state.cctx, dst, state.out_size, nullptr));
      });
    }
  }

  void EncodeAll(std::string_view in, std::string& out) override {
    auto& state = *_state;
    irs::utils::StrResize(
      out, LZ4F_HEADER_SIZE_MAX + LZ4F_compressBound(in.size(), &state.prefs));
    auto* dst = reinterpret_cast<uint8_t*>(out.data());
    size_t pos =
      Check(LZ4F_compressBegin(state.cctx, dst, out.size(), &state.prefs));
    pos += Check(LZ4F_compressUpdate(state.cctx, dst + pos, out.size() - pos,
                                     in.data(), in.size(), nullptr));
    pos +=
      Check(LZ4F_compressEnd(state.cctx, dst + pos, out.size() - pos, nullptr));
    out.resize(pos);
  }

 private:
  static size_t Check(size_t rc) {
    if (LZ4F_isError(rc)) {
      ThrowCodecError("lz4", LZ4F_getErrorName(rc));
    }
    return rc;
  }

  Pooled<CompressState> _state;
  bool _started = false;
};

class Lz4Decoder final : public ContentDecoder {
 public:
  void Decode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    auto& out = _state->out;
    for (;;) {
      size_t produced = out.size();
      size_t consumed = in.size();
      const size_t rc = LZ4F_decompress(_state->dctx, out.data(), &produced,
                                        in.data(), &consumed, nullptr);
      if (LZ4F_isError(rc)) {
        ThrowCorrupt("lz4", LZ4F_getErrorName(rc));
      }
      in.remove_prefix(consumed);
      if (produced != 0) {
        sink({reinterpret_cast<const char*>(out.data()), produced});
      }
      if (consumed != 0 || produced != 0) {
        _pending = rc;
      }
      if (in.empty() && produced < out.size()) {
        break;
      }
    }
    if (finish && _pending != 0) {
      ThrowCorrupt("lz4", "truncated body");
    }
  }

 private:
  Pooled<DecompressState> _state;
  size_t _pending = 1;
};

}  // namespace

std::unique_ptr<ContentEncoder> MakeLz4Encoder(int level) {
  return std::make_unique<Lz4Encoder>(level);
}

std::unique_ptr<ContentDecoder> MakeLz4Decoder() {
  return std::make_unique<Lz4Decoder>();
}

}  // namespace sdb::network::http
