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

#include <iresearch/utils/string_utils.hpp>
#include <string>

#include "network/http/codecs/codec.h"

// https://github.com/google/snappy/blob/main/format_description.txt
namespace sdb::network::http {
namespace {

class SnappyEncoder final : public ContentEncoder {
 public:
  void Encode(std::string_view in, bool finish, EncodeOutput& out) override {
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

  void EncodeAll(std::string_view in, std::string& out) override {
    irs::utils::StrResize(out, snappy::MaxCompressedLength(in.size()));
    size_t size = 0;
    snappy::RawCompress(in.data(), in.size(), out.data(), &size);
    out.resize(size);
  }

 private:
  std::string _pending;
};

class ForwardSink final : public snappy::Sink {
 public:
  explicit ForwardSink(absl::FunctionRef<void(std::string_view)> sink)
    : _sink{sink} {}

  void Append(const char* bytes, size_t n) override { _sink({bytes, n}); }

 private:
  absl::FunctionRef<void(std::string_view)> _sink;
};

class SnappyDecoder final : public ContentDecoder {
 public:
  void Decode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
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
    if (!snappy::Uncompress(&source, &out)) {
      ThrowCorrupt("snappy", "corrupt or truncated body");
    }
    _pending.clear();
  }

 private:
  std::string _pending;
};

}  // namespace

std::unique_ptr<ContentEncoder> MakeSnappyEncoder(int) {
  return std::make_unique<SnappyEncoder>();
}

std::unique_ptr<ContentDecoder> MakeSnappyDecoder() {
  return std::make_unique<SnappyDecoder>();
}

}  // namespace sdb::network::http
