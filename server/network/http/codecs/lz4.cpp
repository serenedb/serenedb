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

#define LZ4F_STATIC_LINKING_ONLY
#include <lz4frame.h>

#include <algorithm>
#include <cstring>
#include <iresearch/utils/assert.hpp>
#include <string_view>

#include "network/http/codecs/codec.h"

namespace sdb::network::http {
namespace {

// https://github.com/lz4/lz4/blob/dev/lib/lz4frame.h
inline constexpr size_t kMaxWrite = 64 * 1024;
inline constexpr size_t kBlockHeader = 4;
inline constexpr size_t kEndMark = 4;
inline constexpr size_t kSlice = kMaxWrite - kBlockHeader - kEndMark;

constexpr LZ4F_CustomMem kMemory{CodecPool::Allocate, nullptr, CodecPool::Free,
                                 nullptr};

LZ4F_preferences_t Preferences(int level) {
  LZ4F_preferences_t prefs{};
  prefs.autoFlush = 1;
  prefs.compressionLevel = level;
  return prefs;
}

size_t Check(size_t rc) {
  if (LZ4F_isError(rc)) {
    ThrowCodecError("lz4", LZ4F_getErrorName(rc));
  }
  return rc;
}

}  // namespace

void Lz4Encoder::Deleter::operator()(LZ4F_cctx* cctx) const noexcept {
  LZ4F_freeCompressionContext(cctx);
}

void Lz4Decoder::Deleter::operator()(LZ4F_dctx* dctx) const noexcept {
  LZ4F_freeDecompressionContext(dctx);
}

Lz4Encoder::Lz4Encoder(int level)
  : _cctx{LZ4F_createCompressionContext_advanced(kMemory, LZ4F_VERSION)},
    _prefs{Preferences(level)},
    _out_size{std::max(LZ4F_compressBound(kSlice, &_prefs),
                       LZ4F_compressBound(0, &_prefs))} {
  SDB_ASSERT(kLz4Levels.Contains(level));
  SDB_ASSERT(_out_size <= kMaxWrite);
  if (_cctx == nullptr) {
    ThrowCodecError("lz4", "cannot initialize the encoder");
  }
}

void Lz4Encoder::Encode(std::string_view in, bool finish,
                        message::Writer& out) {
  if (!_started) {
    out.Write(LZ4F_HEADER_SIZE_MAX, [&](uint8_t* dst) {
      return Check(
        LZ4F_compressBegin(_cctx.get(), dst, LZ4F_HEADER_SIZE_MAX, &_prefs));
    });
    _started = true;
  }
  while (!in.empty()) {
    if (_pending_size == 0 && in.size() >= kSlice) {
      Compress(in.substr(0, kSlice), in.size() >= 2 * kSlice, out);
      in.remove_prefix(kSlice);
      continue;
    }
    if (!_pending) {
      _pending.emplace(2 * kSlice);
    }
    const auto take = std::min(kSlice - _pending_size, in.size());
    std::memcpy(PendingData() + _pending_size, in.data(), take);
    _pending_size += take;
    in.remove_prefix(take);
    if (_pending_size == kSlice) {
      Compress(Pending(), true, out);
      _pending_half ^= 1;
      _pending_size = 0;
    }
  }
  if (finish) {
    if (_pending_size != 0) {
      Compress(Pending(), false, out);
      _pending_size = 0;
    }
    out.Write(_out_size, [&](uint8_t* dst) {
      return Check(LZ4F_compressEnd(_cctx.get(), dst, _out_size, nullptr));
    });
  }
}

void Lz4Encoder::EncodeAll(std::string_view in, message::Writer& out) {
  const size_t bound =
    LZ4F_HEADER_SIZE_MAX + LZ4F_compressBound(in.size(), &_prefs);
  out.Write(bound, [&](uint8_t* dst) {
    auto* cctx = _cctx.get();
    size_t pos = Check(LZ4F_compressBegin(cctx, dst, bound, &_prefs));
    pos += Check(LZ4F_compressUpdate(cctx, dst + pos, bound - pos, in.data(),
                                     in.size(), nullptr));
    pos += Check(LZ4F_compressEnd(cctx, dst + pos, bound - pos, nullptr));
    return pos;
  });
}

uint8_t* Lz4Encoder::PendingData() const noexcept {
  return _pending->Data() + _pending_half * kSlice;
}

std::string_view Lz4Encoder::Pending() const noexcept {
  return {reinterpret_cast<const char*>(PendingData()), _pending_size};
}

void Lz4Encoder::Compress(std::string_view slice, bool stable,
                          message::Writer& out) {
  const LZ4F_compressOptions_t options{.stableSrc = stable ? 1U : 0U};
  out.Write(_out_size, [&](uint8_t* dst) {
    return Check(LZ4F_compressUpdate(_cctx.get(), dst, _out_size, slice.data(),
                                     slice.size(), &options));
  });
}

Lz4Decoder::Lz4Decoder()
  : _dctx{LZ4F_createDecompressionContext_advanced(kMemory, LZ4F_VERSION)},
    _out{kOutBlock} {
  if (_dctx == nullptr) {
    ThrowCodecError("lz4", "cannot initialize the decoder");
  }
}

void Lz4Decoder::Decode(std::string_view in, bool finish, DecodeSink sink) {
  for (;;) {
    size_t produced = _out.Size();
    size_t consumed = in.size();
    const size_t rc = LZ4F_decompress(_dctx.get(), _out.Data(), &produced,
                                      in.data(), &consumed, nullptr);
    if (LZ4F_isError(rc)) {
      ThrowCorrupt("lz4", LZ4F_getErrorName(rc));
    }
    in.remove_prefix(consumed);
    if (produced != 0) {
      sink({reinterpret_cast<const char*>(_out.Data()), produced});
    }
    if (consumed != 0 || produced != 0) {
      _pending = rc;
    }
    if (in.empty() && produced < _out.Size()) {
      break;
    }
  }
  if (finish && _pending != 0) {
    ThrowCorrupt("lz4", "truncated body");
  }
}

}  // namespace sdb::network::http
