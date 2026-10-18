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

#include <snappy-sinksource.h>
#include <snappy.h>

#include <exception>
#include <iresearch/utils/assert.hpp>

#include "network/http/codecs/codec.h"

// https://github.com/google/snappy/blob/main/format_description.txt
namespace sdb::network::http {
namespace {

class ForwardSink final : public snappy::Sink {
 public:
  explicit ForwardSink(DecodeSink sink) : _sink{sink} {}

  void Append(const char* bytes, size_t n) override {
    if (_error) {
      return;
    }
    try {
      _sink({bytes, n});
    } catch (...) {
      _error = std::current_exception();
    }
  }

  void RethrowError() const {
    if (_error) {
      std::rethrow_exception(_error);
    }
  }

 private:
  DecodeSink _sink;
  std::exception_ptr _error;
};

}  // namespace

SnappyEncoder::SnappyEncoder(int level) { SDB_ASSERT(level == kNoLevel); }

void SnappyEncoder::Encode(std::string_view in, bool finish,
                           message::Writer& out) {
  if (!finish) {
    _pending.append(in);
    return;
  }
  if (!_pending.empty()) {
    _pending.append(in);
    in = _pending;
  }
  out.Write(snappy::MaxCompressedLength(in.size()), [&](uint8_t* dst) {
    size_t size = 0;
    snappy::RawCompress(in.data(), in.size(), reinterpret_cast<char*>(dst),
                        &size);
    return size;
  });
  _pending.clear();
}

void SnappyDecoder::Decode(std::string_view in, bool finish, DecodeSink sink) {
  if (!finish) {
    _pending.append(in);
    return;
  }
  if (!_pending.empty()) {
    _pending.append(in);
    in = _pending;
  }
  snappy::ByteArraySource source{in.data(), in.size()};
  ForwardSink out{sink};
  const bool ok = snappy::Uncompress(&source, &out);
  out.RethrowError();
  if (!ok) {
    ThrowCorrupt("snappy", "corrupt or truncated body");
  }
  _pending.clear();
}

}  // namespace sdb::network::http
