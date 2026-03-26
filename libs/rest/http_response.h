////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "basics/debugging.h"
#include "basics/string_buffer.h"
#include "rest/general_response.h"

namespace sdb {

class HttpResponse : public GeneralResponse {
 public:
  HttpResponse(ResponseCode code, uint64_t mid, basics::StringBufferPtr buffer,
               rest::ResponseCompressionType rct);
  ~HttpResponse() override = default;

  void setCookie(const std::string& name, const std::string& value,
                 int life_time_seconds, const std::string& path,
                 const std::string& domain, bool secure, bool http_only);
  const std::vector<std::string>& cookies() const { return _cookies; }

  // In case of HEAD request, no body must be defined. However, the response
  // needs to know the size of body.
  void headResponse(size_t);

  // Returns a reference to the body. This reference is only valid as long as
  // http response exists. You can add data to the body by appending
  // information to the string buffer. Note that adding data to the body
  // invalidates any previously returned header. You must call header
  // again.
  basics::StringBuffer& body() {
    SDB_ASSERT(_body);
    return *_body;
  }

  size_t bodySize() const override;

  void sealBody() { _body_size = _body->size(); }

  // you should call writeHeader only after the body has been created
  void writeHeader(basics::StringBuffer*);  // override;

  void clearBody() noexcept override;

  void reset(ResponseCode code) final;

  void addPayload(vpack::Slice slice, const vpack::Options* = nullptr) final;
  void addPayload(vpack::BufferUInt8&&, const vpack::Options* = nullptr) final;
  void addRawPayload(std::string_view payload) final;

  void setAllowCompression(rest::ResponseCompressionType rct) noexcept final;

  rest::ResponseCompressionType compressionAllowed() const noexcept final;

  bool isResponseEmpty() const noexcept final { return _body->empty(); }

  ErrorCode reservePayload(size_t size) final {
    _body->reserve(size);
    return ERROR_OK;
  }

  sdb::Endpoint::TransportType transportType() final {
    return sdb::Endpoint::TransportType::HTTP;
  }

  std::unique_ptr<basics::StringBuffer> stealBody() {
    std::unique_ptr<basics::StringBuffer> body(std::move(_body));
    return body;
  }

 private:
  // the body must already be set. deflate is then run on the existing body
  ErrorCode ZLibDeflate(bool only_if_smaller) override;

  // the body must already be set. gzip compression is then run on the existing
  // body
  ErrorCode GZipCompress(bool only_if_smaller) override;

  // the body must already be set. lz4 compression is then run on the existing
  // body
  ErrorCode Lz4Compress(bool only_if_smaller) override;

  void addPayloadInternal(const uint8_t* data, size_t length,
                          const vpack::Options* options);

  std::vector<std::string> _cookies;
  basics::StringBufferPtr _body;
  size_t _body_size;
  rest::ResponseCompressionType _allow_compression;
};

}  // namespace sdb
