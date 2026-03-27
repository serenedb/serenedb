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

#include <llhttp.h>

#include <memory>

#include "general_server/general_comm_task.h"

namespace sdb {

class HttpRequest;

namespace basics {

class StringBuffer;
}

namespace rest {

template<SocketType T>
class HttpCommTask final : public GeneralCommTask<T> {
 public:
  HttpCommTask(GeneralServer& server, ConnectionInfo,
               std::shared_ptr<AsioSocket<T>> so);
  ~HttpCommTask() override;

  void Start() final;

 protected:
  bool ReadCallback(asio_ns::error_code ec) final;
  void SetIOTimeout() final;

  void SendResponse(std::unique_ptr<GeneralResponse> response,
                    RequestStatistics::Item stat) final;

  std::unique_ptr<GeneralResponse> CreateResponse(rest::ResponseCode,
                                                  uint64_t message_id) final;

 private:
  static int on_message_began(llhttp_t* p);
  static int on_url(llhttp_t* p, const char* at, size_t len);
  static int on_status(llhttp_t* p, const char* at, size_t len);
  static int on_header_field(llhttp_t* p, const char* at, size_t len);
  static int on_header_value(llhttp_t* p, const char* at, size_t len);
  static int on_header_complete(llhttp_t* p);
  static int on_body(llhttp_t* p, const char* at, size_t len);
  static int on_message_complete(llhttp_t* p);

  /// verify if the transfer-encoding in the header is chunked, which is not
  /// implemented
  static bool transferEncodingContainsChunked(HttpCommTask<T>& comm_task,
                                              std::string_view encoding);

  void CheckProtocolUpgrade();

  void ProcessRequest();
  void DoProcessRequest();

  // called on IO context thread
  void WriteResponse(RequestStatistics::Item stat);

  std::string url() const;

  /// the node http-parser
  llhttp_t _parser;
  llhttp_settings_t _parser_settings;

  vpack::BufferUInt8 _header;

  // ==== parser state ====
  std::string _last_header_field;
  std::string _last_header_value;
  std::string _origin;  // value of the HTTP origin header the client sent
  std::string _url;
  std::unique_ptr<HttpRequest> _request;
  std::unique_ptr<basics::StringBuffer> _response;
  bool _last_header_was_value;
  bool _should_keep_alive;  /// keep connection open
  bool _message_done;
};

}  // namespace rest
}  // namespace sdb
