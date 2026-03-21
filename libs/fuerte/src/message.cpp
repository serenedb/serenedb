////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Jan Christoph Uhde
////////////////////////////////////////////////////////////////////////////////

#include <fuerte/helper.h>
#include <fuerte/message.h>
#include <vpack/validator.h>

#include <sstream>

#include "basics/assert.h"

namespace {

static const std::string kEmptyString;

inline int Hex2int(char ch, int error_code) {
  if ('0' <= ch && ch <= '9') {
    return ch - '0';
  } else if ('A' <= ch && ch <= 'F') {
    return ch - 'A' + 10;
  } else if ('a' <= ch && ch <= 'f') {
    return ch - 'a' + 10;
  }

  return error_code;
}

}  // namespace
namespace sdb::fuerte {

void MessageHeader::setMeta(StringMap map) {
  if (!this->_meta.empty()) {
    for (auto& pair : map) {
      this->addMeta(std::move(pair.first), std::move(pair.second));
    }
  } else {
    this->_meta = std::move(map);
  }
}

// Get value for header metadata key, returns empty string if not found.
const std::string& MessageHeader::metaByKey(const std::string& key,
                                            bool& found) const {
  if (_meta.empty()) {
    found = false;
    return ::kEmptyString;
  }
  const auto& it = _meta.find(key);
  if (it == _meta.end()) {
    found = false;
    return ::kEmptyString;
  } else {
    found = true;
    return it->second;
  }
}

void RequestHeader::acceptType(const std::string& type) {
  addMeta(kFuAcceptKey, type);
}

/// @brief analyze path and split into components
/// strips /_db/<name> prefix, sets db name and fills parameters
void RequestHeader::parseSerenePath(std::string_view p) {
  this->path = ExtractPathParameters(p, this->parameters);

  // extract database prefix /_db/<name>/
  const char* q = this->path.c_str();
  if (this->path.size() >= 4 && q[0] == '/' && q[1] == '_' && q[2] == 'd' &&
      q[3] == 'b' && q[4] == '/') {
    // request contains database name
    q += 5;
    const char* path_begin = q;
    // read until end of database name
    while (*q != '\0' && *q != '/' && *q != '?' && *q != ' ' && *q != '\n' &&
           *q != '\r') {
      ++q;
    }
    SDB_ASSERT(q >= path_begin);
    this->database.clear();
    const char* p = path_begin;
    while (p != q) {
      std::string::value_type c = (*p);
      if (c == '%') {
        if (p + 2 < q) {
          int h = ::Hex2int(p[1], 256) << 4;
          h += ::Hex2int(p[2], 256);
          if (h >= 256) {
            throw std::invalid_argument(
              "invalid encoding value in request URL");
          }
          this->database.push_back(static_cast<char>(h & 0xFF));
          p += 2;
        } else {
          throw std::invalid_argument("invalid encoding value in request URL");
        }
      } else if (c == '+') {
        this->database.push_back(' ');
      } else {
        this->database.push_back(c);
      }
      ++p;
    }
    if (*q == '\0') {
      this->path = "/";
    } else {
      this->path = std::string(q, this->path.c_str() + this->path.size());
    }
  }
}

///////////////////////////////////////////////
// class Message
///////////////////////////////////////////////

ContentEncoding Message::contentEncoding() const {
  return messageHeader().contentEncoding();
}

// content-type header accessors

ContentType Message::contentType() const {
  return messageHeader().contentType();
}

bool Message::isContentTypeJSON() const {
  return (contentType() == ContentType::Json);
}

bool Message::isContentTypeVPack() const {
  return (contentType() == ContentType::VPack);
}

bool Message::isContentTypeHtml() const {
  return (contentType() == ContentType::Html);
}

bool Message::isContentTypeText() const {
  return (contentType() == ContentType::Text);
}

ContentType Request::acceptType() const { return header.acceptType(); }

void Request::addVPack(const vpack::Slice slice) {
  header.contentType(ContentType::VPack);
  _payload.append(slice.start(), slice.byteSize());
}

void Request::addVPack(const vpack::BufferUInt8& buffer) {
  header.contentType(ContentType::VPack);
  _payload.append(buffer);
}

void Request::addVPack(vpack::BufferUInt8&& buffer) {
  header.contentType(ContentType::VPack);
  _payload = std::move(buffer);
}

// add binary data
void Request::addBinary(const uint8_t* data, size_t length) {
  _payload.append(data, length);
}

// get payload as slices
std::vector<vpack::Slice> Request::slices() const {
  std::vector<vpack::Slice> slices;
  if (isContentTypeVPack()) {
    auto length = _payload.size();
    auto cursor = _payload.data();
    while (length) {
      slices.emplace_back(cursor);
      auto slice_size = slices.back().byteSize();
      if (length < slice_size) {
        throw std::logic_error("invalid buffer");
      }
      cursor += slice_size;
      length -= slice_size;
    }
  }
  return slices;
}

// get payload as binary
asio_ns::const_buffer Request::payload() const {
  return asio_ns::const_buffer(_payload.data(), payloadSize());
}

size_t Request::payloadSize() const { return _payload.size(); }

std::vector<vpack::Slice> Response::slices() const {
  std::vector<vpack::Slice> slices;
  if (isContentTypeVPack() && payloadSize() > 0) {
    vpack::Validator validator;

    auto length = _payload.size() - _payload_offset;
    auto cursor = _payload.data() + _payload_offset;
    while (length) {
      // will throw on an error
      validator.validate(cursor, length, true);

      slices.emplace_back(cursor);
      auto slice_size = slices.back().byteSize();
      if (length < slice_size) {
        throw std::logic_error("invalid buffer");
      }
      SDB_ASSERT(length >= slice_size);
      cursor += slice_size;
      length -= slice_size;
    }
  }
  return slices;
}

asio_ns::const_buffer Response::payload() const {
  return asio_ns::const_buffer(_payload.data() + _payload_offset,
                               _payload.size() - _payload_offset);
}

size_t Response::payloadSize() const {
  auto payload_byte_size = _payload.size();
  if (_payload_offset > payload_byte_size) {
    return 0;
  }
  return payload_byte_size - _payload_offset;
}

std::shared_ptr<vpack::BufferUInt8> Response::copyPayload() const {
  auto buffer = std::make_shared<vpack::BufferUInt8>();
  if (payloadSize() > 0) {
    buffer->append(_payload.data() + _payload_offset,
                   _payload.size() - _payload_offset);
  }
  return buffer;
}

std::shared_ptr<vpack::BufferUInt8> Response::stealPayload() {
  if (_payload_offset == 0) {
    return std::make_shared<vpack::BufferUInt8>(std::move(_payload));
  }

  auto buffer = std::make_shared<vpack::BufferUInt8>();
  buffer->append(_payload.data() + _payload_offset,
                 _payload.size() - _payload_offset);
  _payload.clear();
  _payload_offset = 0;
  return buffer;
}

}  // namespace sdb::fuerte
