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
#include <vector>

#include "network/http/codecs/codec.h"

namespace sdb::network::http {
namespace {

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

class ZxcDecoder final : public ContentDecoder {
 public:
  ZxcDecoder() : _stream{zxc_dstream_create(nullptr)} {
    if (_stream == nullptr) {
      ThrowCodecError("zxc", "stream creation failed");
    }
    _out.resize(std::max(zxc_dstream_out_size(_stream), kOutBlock));
  }

  ~ZxcDecoder() override { zxc_dstream_free(_stream); }

  void Decode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    zxc_inbuf_t input{.src = in.data(), .size = in.size(), .pos = 0};
    for (;;) {
      zxc_outbuf_t output{.dst = _out.data(), .size = _out.size(), .pos = 0};
      const int64_t rc = zxc_dstream_decompress(_stream, &output, &input);
      if (rc < 0) {
        ThrowCorrupt("zxc", zxc_error_name(static_cast<int>(rc)));
      }
      if (output.pos != 0) {
        sink({reinterpret_cast<const char*>(_out.data()), output.pos});
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
  std::vector<uint8_t> _out;
};

}  // namespace

std::unique_ptr<ContentEncoder> MakeZxcEncoder() {
  return std::make_unique<ZxcEncoder>();
}

std::unique_ptr<ContentDecoder> MakeZxcDecoder() {
  return std::make_unique<ZxcDecoder>();
}

}  // namespace sdb::network::http
