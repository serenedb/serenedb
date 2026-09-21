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

#include "network/http/compression.h"

#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>
#include <lz4frame.h>
#include <zlib.h>
#include <zstd.h>
#include <zxc.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/zstd_context.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace sdb::network::http {
namespace {

constexpr size_t kOutBlock = 16 * 1024;

// A codec failure is never expected; it must not be an assert, which compiles
// out in release builds and would leave the drain loops spinning forever on a
// sticky error. The session turns this into a 500 (or drops the connection
// once the head is out).
[[noreturn]] void ThrowCodecError(std::string_view coding,
                                  std::string_view detail) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INTERNAL_ERROR),
    ERR_MSG("HTTP response compression (", coding, ") failed: ", detail));
}

// https://www.zlib.net/manual.html#Advanced : windowBits 15 + 16 selects the
// gzip wrapper around deflate.
class GzipEncoder final : public ContentEncoder {
 public:
  GzipEncoder() {
    const int rc = deflateInit2(&_stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                                15 + 16, 8, Z_DEFAULT_STRATEGY);
    if (rc != Z_OK) {
      ThrowCodecError("gzip", zError(rc));
    }
    _initialized = true;
  }

  ~GzipEncoder() override {
    if (_initialized) {
      deflateEnd(&_stream);
    }
  }

  void Encode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    _stream.next_in =
      const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data()));
    _stream.avail_in = static_cast<uInt>(in.size());
    const int flush = finish ? Z_FINISH : Z_NO_FLUSH;
    do {
      _stream.next_out = _out.data();
      _stream.avail_out = static_cast<uInt>(_out.size());
      const int rc = deflate(&_stream, flush);
      // Z_BUF_ERROR only reports "no progress possible", which is expected
      // once the input is drained; anything else is fatal.
      if (rc != Z_OK && rc != Z_STREAM_END && rc != Z_BUF_ERROR) {
        ThrowCodecError("gzip", zError(rc));
      }
      const size_t produced = _out.size() - _stream.avail_out;
      if (produced != 0) {
        sink({reinterpret_cast<const char*>(_out.data()), produced});
      }
      if (rc == Z_BUF_ERROR) {
        break;
      }
    } while (_stream.avail_out == 0);
    if (_stream.avail_in != 0) {
      ThrowCodecError("gzip", "input not consumed");
    }
  }

 private:
  z_stream _stream{};
  bool _initialized = false;
  std::array<uint8_t, kOutBlock> _out;
};

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

// https://github.com/serenedb/zxc/blob/main/include/zxc_pstream.h
class ZxcEncoder final : public ContentEncoder {
 public:
  ZxcEncoder() : _stream{zxc_cstream_create(nullptr)} {
    if (_stream == nullptr) {
      ThrowCodecError("zxc", "stream creation failed");
    }
  }

  ~ZxcEncoder() override { zxc_cstream_free(_stream); }

  void Encode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    zxc_inbuf_t input{.src = in.data(), .size = in.size(), .pos = 0};
    Drain(sink, [&](zxc_outbuf_t& out) {
      return zxc_cstream_compress(_stream, &out, &input);
    });
    if (finish) {
      Drain(sink,
            [&](zxc_outbuf_t& out) { return zxc_cstream_end(_stream, &out); });
    }
  }

 private:
  template<typename Step>
  void Drain(absl::FunctionRef<void(std::string_view)> sink, Step step) {
    for (;;) {
      zxc_outbuf_t output{.dst = _out.data(), .size = _out.size(), .pos = 0};
      const int64_t rc = step(output);
      // zxc errors are sticky: without this the loop would call the failing
      // function forever (zxc_pstream.h, "Errors are sticky").
      if (rc < 0) {
        ThrowCodecError("zxc", zxc_error_name(static_cast<int>(rc)));
      }
      if (output.pos != 0) {
        sink({reinterpret_cast<const char*>(_out.data()), output.pos});
      }
      if (rc == 0) {
        return;
      }
      if (output.pos == 0) {
        ThrowCodecError("zxc",
                        "stream reported pending bytes but produced "
                        "none");
      }
    }
  }

  zxc_cstream* _stream;
  std::array<uint8_t, 4 * kOutBlock> _out;
};

template<typename Encoder>
std::unique_ptr<ContentEncoder> Make() {
  return std::make_unique<Encoder>();
}

// Declaration order IS the server's preference order, used whenever the
// client accepts more than one of these equally.
constexpr std::array kContentCodings = {
  ContentCoding{.token = "zstd", .make = Make<ZstdEncoder>},
  ContentCoding{.token = "gzip", .make = Make<GzipEncoder>},
  ContentCoding{.token = "zxc", .make = Make<ZxcEncoder>},
  ContentCoding{.token = "lz4", .make = Make<Lz4Encoder>},
};

struct AcceptedCoding {
  std::string_view token;
  double quality = 1.0;
};

// codings = coding [ ";" parameter ]... ; only the "q" weight is defined for
// Accept-Encoding, but other parameters must not hide it.
// https://www.rfc-editor.org/rfc/rfc9110#name-quality-values
std::optional<AcceptedCoding> ParseAccepted(std::string_view element) {
  element = absl::StripAsciiWhitespace(element);
  if (element.empty()) {
    return std::nullopt;
  }
  AcceptedCoding accepted;
  bool first = true;
  for (std::string_view part : absl::StrSplit(element, ';')) {
    part = absl::StripAsciiWhitespace(part);
    if (std::exchange(first, false)) {
      accepted.token = part;
      continue;
    }
    if (absl::StartsWithIgnoreCase(part, "q=")) {
      part.remove_prefix(2);
      if (!absl::SimpleAtod(part, &accepted.quality) ||
          accepted.quality < 0.0 || accepted.quality > 1.0) {
        return std::nullopt;
      }
    }
  }
  return accepted;
}

}  // namespace

const ContentCoding* FindContentCoding(std::string_view token) {
  for (const auto& coding : kContentCodings) {
    if (absl::EqualsIgnoreCase(coding.token, token)) {
      return &coding;
    }
  }
  return nullptr;
}

Negotiation NegotiateContentCoding(std::string_view accept_encoding) {
  if (absl::StripAsciiWhitespace(accept_encoding).empty()) {
    // No field, or an empty one: the body goes out uncompressed. (RFC 9110
    // lets a server compress when the field is absent; clients that cannot
    // decode outnumber the bytes that would save.)
    return {};
  }

  std::vector<AcceptedCoding> accepted;
  std::optional<double> wildcard;
  for (const auto element : absl::StrSplit(accept_encoding, ',')) {
    if (absl::StripAsciiWhitespace(element).empty()) {
      continue;
    }
    const auto parsed = ParseAccepted(element);
    if (!parsed) {
      return {.acceptance = Acceptance::Malformed};
    }
    if (parsed->token == "*") {
      wildcard = parsed->quality;
    } else {
      accepted.push_back(*parsed);
    }
  }

  // An explicit weight wins over the wildcard, whatever their order.
  const auto quality_of = [&](std::string_view token) -> std::optional<double> {
    for (const auto& candidate : accepted) {
      if (absl::EqualsIgnoreCase(candidate.token, token)) {
        return candidate.quality;
      }
    }
    return wildcard;
  };

  // "the acceptable content coding with the highest non-zero qvalue is
  // preferred"; server order breaks ties, which is the usual case since most
  // clients send no weights at all.
  const ContentCoding* best = nullptr;
  double best_quality = 0.0;
  for (const auto& coding : kContentCodings) {
    const double quality = quality_of(coding.token).value_or(0.0);
    if (quality > best_quality) {
      best = &coding;
      best_quality = quality;
    }
  }
  if (best != nullptr) {
    return {.coding = best};
  }

  // Nothing we encode is acceptable. The uncompressed form still is, unless
  // the client ruled it out too -- then there is no representation to send.
  // https://www.rfc-editor.org/rfc/rfc9110#name-accept-encoding
  if (quality_of("identity").value_or(1.0) > 0.0) {
    return {};
  }
  return {.acceptance = Acceptance::NotAcceptable};
}

}  // namespace sdb::network::http
