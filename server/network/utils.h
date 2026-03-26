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

namespace vpack {

class Builder;

}  // namespace vpack
namespace sdb {
namespace network {

struct RequestOptions;

Result ResultFromBody(const std::shared_ptr<vpack::BufferUInt8>& b,
                      ErrorCode default_error);
Result ResultFromBody(const std::shared_ptr<vpack::Builder>& b,
                      ErrorCode default_error);
Result ResultFromBody(vpack::Slice b, ErrorCode default_error);

/// extract the error code form the body
ErrorCode ErrorCodeFromBody(vpack::Slice body,
                            ErrorCode default_error_code = ERROR_OK);

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

}  // namespace network
}  // namespace sdb
