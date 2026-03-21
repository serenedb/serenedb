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

#include <fuerte/types.h>
#include <vpack/slice.h>

#include <string>

#include "basics/buffer.h"
#include "basics/errors.h"
#include "basics/result.h"
#include "network/types.h"
#include "rest/common_defines.h"
#include "utils/coro_helper.h"
#include "utils/operation_result.h"

namespace vpack {
class Builder;
}
namespace sdb {
namespace consensus {
class Agent;
}
class ClusterInfo;
class NetworkFeature;

namespace network {
struct RequestOptions;

/// resolve 'shard:' or 'server:' url to actual endpoint
yaclib::Future<ErrorCode> ResolveDestination(const NetworkFeature&,
                                             const DestinationId& dest,
                                             network::EndpointSpec&);
yaclib::Future<ErrorCode> ResolveDestination(ClusterInfo&,
                                             const DestinationId& dest,
                                             network::EndpointSpec&);

Result ResultFromBody(const std::shared_ptr<vpack::BufferUInt8>& b,
                      ErrorCode default_error);
Result ResultFromBody(const std::shared_ptr<vpack::Builder>& b,
                      ErrorCode default_error);
Result ResultFromBody(vpack::Slice b, ErrorCode default_error);

/// extract the error from a cluster response
template<typename T>
OperationResult OpResultFromBody(const T& body, ErrorCode default_error_code,
                                 OperationOptions&& options) {
  return OperationResult{sdb::network::ResultFromBody(body, default_error_code),
                         body, std::move(options)};
}

/// extract the error code form the body
ErrorCode ErrorCodeFromBody(vpack::Slice body,
                            ErrorCode default_error_code = ERROR_OK);

/// Extract all error baby-style error codes and store them in a map
void ErrorCodesFromHeaders(network::Headers headers, ErrorsCount& error_counter,
                           bool include_not_found);

/// transform response into error code
ErrorCode FuerteToSereneErrorCode(const network::Response& res);
ErrorCode FuerteToSereneErrorCode(fuerte::Error err);
std::string_view FuerteToSereneErrorMessage(const network::Response& res);
std::string_view FuerteToSereneErrorMessage(fuerte::Error err);
ErrorCode FuerteStatusToSereneErrorCode(const fuerte::Response& res);
ErrorCode FuerteStatusToSereneErrorCode(const fuerte::StatusCode& code);
std::string FuerteStatusToSereneErrorMessage(const fuerte::Response& res);
std::string FuerteStatusToSereneErrorMessage(const fuerte::StatusCode& code);

/// convert between serene and fuerte rest methods
fuerte::RestVerb SereneRestVerbToFuerte(rest::RequestType);
rest::RequestType FuerteRestVerbToSerene(fuerte::RestVerb);

void AddSourceHeader(consensus::Agent* agent, fuerte::Request& req);

/// add "user" request parameter
void AddUserParameter(RequestOptions& req_opts, std::string_view value);

}  // namespace network
}  // namespace sdb
