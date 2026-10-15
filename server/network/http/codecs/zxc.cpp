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

#include <zxc.h>

#include <algorithm>
#include <iresearch/utils/assert.hpp>

#include "network/http/codecs/codec.h"

namespace sdb::network::http {
namespace {

static_assert(kZxcLevels.min == ZXC_LEVEL_FASTEST &&
              kZxcLevels.max == ZXC_LEVEL_COMPACT);

template<typename Step>
void Drain(message::Writer& out, Step step) {
  int64_t rc = 0;
  do {
    out.Write(kOutBlock, [&](uint8_t* dst) {
      zxc_outbuf_t output{.dst = dst, .size = kOutBlock, .pos = 0};
      rc = step(output);
      // zxc errors are sticky: without this the loop would call the failing
      // function forever (zxc_pstream.h, "Errors are sticky").
      if (rc < 0) {
        ThrowCodecError("zxc", zxc_error_name(static_cast<int>(rc)));
      }
      if (rc != 0 && output.pos == 0) {
        ThrowCodecError("zxc",
                        "stream reported pending bytes but produced "
                        "none");
      }
      return output.pos;
    });
  } while (rc != 0);
}

zxc_cstream* CreateCStream(int level) {
  SDB_ASSERT(kZxcLevels.Contains(level));
  zxc_compress_opts_t opts{};
  opts.level = level;
  return zxc_cstream_create(&opts);
}

zxc_dstream* CreateDStream() {
  auto* stream = zxc_dstream_create(nullptr);
  if (!stream) {
    ThrowCodecError("zxc", "stream creation failed");
  }
  return stream;
}

}  // namespace

// https://github.com/serenedb/zxc/blob/main/include/zxc_pstream.h
ZxcEncoder::ZxcEncoder(int level) : _stream{CreateCStream(level)} {
  if (!_stream) {
    ThrowCodecError("zxc", "stream creation failed");
  }
}

ZxcEncoder::~ZxcEncoder() { zxc_cstream_free(_stream); }

void ZxcEncoder::Encode(std::string_view in, bool finish,
                        message::Writer& out) {
  zxc_inbuf_t input{.src = in.data(), .size = in.size(), .pos = 0};
  Drain(out, [&](zxc_outbuf_t& output) {
    return zxc_cstream_compress(_stream, &output, &input);
  });
  if (finish) {
    Drain(out, [&](zxc_outbuf_t& output) {
      return zxc_cstream_end(_stream, &output);
    });
  }
}

void ZxcDecoder::Deleter::operator()(zxc_dstream* stream) const noexcept {
  zxc_dstream_free(stream);
}

ZxcDecoder::ZxcDecoder()
  : _stream{CreateDStream()},
    _out_size{std::max(zxc_dstream_out_size(_stream.get()), kOutBlock)},
    _out{std::make_unique_for_overwrite<uint8_t[]>(_out_size)} {}

void ZxcDecoder::Decode(std::string_view in, bool finish, DecodeSink sink) {
  zxc_inbuf_t input{.src = in.data(), .size = in.size(), .pos = 0};
  for (;;) {
    zxc_outbuf_t output{.dst = _out.get(), .size = _out_size, .pos = 0};
    const int64_t rc = zxc_dstream_decompress(_stream.get(), &output, &input);
    if (rc < 0) {
      ThrowCorrupt("zxc", zxc_error_name(static_cast<int>(rc)));
    }
    if (output.pos != 0) {
      sink({reinterpret_cast<const char*>(_out.get()), output.pos});
    }
    if (rc == 0) {
      break;
    }
  }
  if (finish && zxc_dstream_finished(_stream.get()) == 0) {
    ThrowCorrupt("zxc", "truncated body");
  }
}

}  // namespace sdb::network::http
