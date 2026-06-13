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

#pragma once

#include <yaclib/async/future.hpp>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>

#include <duckdb/common/unique_ptr.hpp>

#include "network/http/request.h"
#include "network/http/response_writer.h"

namespace duckdb {
class Connection;
class MaterializedQueryResult;
}

namespace sdb::network {

// Session-scoped services a handler may need; implemented by the session and
// valid for the duration of one Handle call (on the session's duckdb task).
class RequestContext {
 public:
  virtual ~RequestContext() = default;
  // Lazily-created per-session duckdb connection (the storage seam: handlers
  // speak SQL through it, never to a concrete table backend). Prefer
  // RunQuery for executing SQL -- a raw Connection().Query() blocks the whole
  // query on the scheduler worker and starves it under concurrency.
  virtual duckdb::Connection& Connection() = 0;
  // Cooperatively drives `sql` to a materialized result: runs it as inline
  // executor slices, yielding/parking the session task between them so the
  // scheduler worker is returned to the pool (other sessions + this query's
  // own parallel sub-tasks keep progressing). `writes` arms the storage
  // transaction. The result may carry an error (check HasError()).
  virtual yaclib::Future<duckdb::unique_ptr<duckdb::MaterializedQueryResult>>
  RunQuery(std::string sql, bool writes) = 0;
  // Authenticated user; empty = trust/anonymous.
  virtual std::string_view User() const = 0;
};

// Runs on the session's duckdb task (NOT an io thread): handlers may drive
// queries and block on backpressure. Output goes exclusively through the
// writer, straight into the session's send buffer; a handler that returns
// without finishing the response gets a 500 if the head is not yet out, or
// the connection dropped if it is.
class HttpHandler {
 public:
  virtual ~HttpHandler() = default;

  virtual yaclib::Future<> Handle(RequestContext& context,
                                  const HttpRequest& request,
                                  http::HttpResponseWriter& writer) = 0;
};

}  // namespace sdb::network
