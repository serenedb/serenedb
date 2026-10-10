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
#include <memory>

#include "network/http/codecs/codec.h"

namespace sdb::network::http {
namespace {

// https://github.com/serenedb/zxc/blob/main/include/zxc_pstream.h
class ZxcEncoder final : public ContentEncoder {
 public:
  explicit ZxcEncoder(int level) : _stream{CreateStream(level)} {
    if (_stream == nullptr) {
      ThrowCodecError("zxc", "stream creation failed");
    }
  }

  ~ZxcEncoder() override { zxc_cstream_free(_stream); }

  void Encode(std::string_view in, bool finish, EncodeOutput& out) override {
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

 private:
  template<typename Step>
  static void Drain(EncodeOutput& out, Step step) {
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

  static zxc_cstream* CreateStream(int level) {
    zxc_compress_opts_t opts{};
    opts.level = ClampLevel(level, zxc_default_level(), zxc_min_level(),
                            ZXC_LEVEL_COMPACT);
    return zxc_cstream_create(&opts);
  }

  zxc_cstream* _stream;
};

class ZxcDecoder final : public ContentDecoder {
 public:
  ZxcDecoder() : _stream{zxc_dstream_create(nullptr)} {
    if (_stream == nullptr) {
      ThrowCodecError("zxc", "stream creation failed");
    }
    _out_size = std::max(zxc_dstream_out_size(_stream), kOutBlock);
    _out = std::make_unique_for_overwrite<uint8_t[]>(_out_size);
  }

  ~ZxcDecoder() override { zxc_dstream_free(_stream); }

  void Decode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    zxc_inbuf_t input{.src = in.data(), .size = in.size(), .pos = 0};
    for (;;) {
      zxc_outbuf_t output{.dst = _out.get(), .size = _out_size, .pos = 0};
      const int64_t rc = zxc_dstream_decompress(_stream, &output, &input);
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
    if (finish && zxc_dstream_finished(_stream) == 0) {
      ThrowCorrupt("zxc", "truncated body");
    }
  }

 private:
  zxc_dstream* _stream;
  std::unique_ptr<uint8_t[]> _out;
  size_t _out_size = 0;
};

}  // namespace

std::unique_ptr<ContentEncoder> MakeZxcEncoder(int level) {
  return std::make_unique<ZxcEncoder>(level);
}

std::unique_ptr<ContentDecoder> MakeZxcDecoder() {
  return std::make_unique<ZxcDecoder>();
}

}  // namespace sdb::network::http
