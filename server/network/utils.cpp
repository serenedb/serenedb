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

#include "utils.h"

#include <fuerte/types.h>
#include <vpack/parser.h>

#include "app/app_server.h"
#include "auth/token_cache.h"
#include "basics/common.h"
#include "basics/logger/logger.h"
#include "basics/number_utils.h"
#include "basics/static_strings.h"
#include "database/ticks.h"
#include "general_server/authentication_feature.h"
#include "general_server/state.h"
#include "network/methods.h"
#include "network/network_feature.h"

namespace sdb::network {

/// extract the error code form the body
ErrorCode ErrorCodeFromBody(vpack::Slice body, ErrorCode default_error_code) {
  if (body.isObject()) {
    vpack::Slice num = body.get(StaticStrings::kErrorNum);
    if (num.isNumber()) {
      // we found an error number, so let's use it!
      return ErrorCode{num.getNumber<int>()};
    }
  }
  return default_error_code;
}

Result ResultFromBody(const std::shared_ptr<vpack::BufferUInt8>& body,
                      ErrorCode default_error) {
  // read the error number from the response and use it if present
  if (body && !body->empty()) {
    return ResultFromBody(vpack::Slice(body->data()), default_error);
  }
  return Result(default_error);
}

Result ResultFromBody(const std::shared_ptr<vpack::Builder>& body,
                      ErrorCode default_error) {
  // read the error number from the response and use it if present
  if (body) {
    return ResultFromBody(body->slice(), default_error);
  }

  return Result(default_error);
}

Result ResultFromBody(vpack::Slice slice, ErrorCode default_error) {
  // read the error number from the response and use it if present
  if (slice.isObject()) {
    if (vpack::Slice num = slice.get(StaticStrings::kErrorNum);
        num.isNumber()) {
      auto error_code = ErrorCode{num.getNumber<int>()};
      if (vpack::Slice msg = slice.get(StaticStrings::kErrorMessage);
          msg.isString()) {
        // found an error number and an error message, so let's use it!
        return Result(error_code, msg.stringView());
      }
      // we found an error number, so let's use it!
      return Result(error_code);
    }
  }
  return Result(default_error);
}

////////////////////////////////////////////////////////////////////////////////
/// Extract all error baby-style error codes and store them in a map
////////////////////////////////////////////////////////////////////////////////

void ErrorCodesFromHeaders(network::Headers headers, ErrorsCount& error_counter,
                           bool include_not_found) {
  const auto& codes = headers.find(StaticStrings::kErrorCodes);
  if (codes != headers.end()) {
    auto parsed_codes = vpack::Parser::fromJson(codes->second);
    vpack::Slice codes_slice = parsed_codes->slice();
    if (!codes_slice.isObject()) {
      return;
    }

    for (auto code : vpack::ObjectIterator(codes_slice)) {
      auto str = code.key.stringView();
      auto code_nr = ErrorCode{
        number_utils::AtoiZero<int>(str.data(), str.data() + str.size())};
      if (include_not_found || code_nr != ERROR_SERVER_DOCUMENT_NOT_FOUND) {
        error_counter[code_nr] += code.value().getNumber<size_t>();
      }
    }
  }
}

namespace {

ErrorCode ToSereneErrorCodeInternal(fuerte::Error err) {
  // This function creates an error code from a fuerte::Error,
  // but only if it is a communication error. If the communication
  // was successful and there was an HTTP error code, this function
  // returns ERROR_OK.
  // If ERROR_OK is returned, then the result was CL_COMM_RECEIVED
  // and .answer can safely be inspected.

  switch (err) {
    case fuerte::Error::NoError:
      return ERROR_OK;

    case fuerte::Error::CouldNotConnect:
      return ERROR_CLUSTER_BACKEND_UNAVAILABLE;

    case fuerte::Error::ConnectionClosed:
    case fuerte::Error::CloseRequested:
      return ERROR_CLUSTER_CONNECTION_LOST;

    case fuerte::Error::RequestTimeout:  // No reply, we give up:
      return ERROR_CLUSTER_TIMEOUT;

    case fuerte::Error::ConnectionCanceled:
    case fuerte::Error::QueueCapacityExceeded:  // there is no result
    case fuerte::Error::ReadError:
    case fuerte::Error::WriteError:
    case fuerte::Error::ProtocolError:
      return ERROR_CLUSTER_CONNECTION_LOST;
  }

  return ERROR_INTERNAL;
}

}  // namespace

fuerte::RestVerb SereneRestVerbToFuerte(rest::RequestType verb) {
  switch (verb) {
    case rest::RequestType::DeleteReq:
      return fuerte::RestVerb::Delete;
    case rest::RequestType::Get:
      return fuerte::RestVerb::Get;
    case rest::RequestType::Post:
      return fuerte::RestVerb::Post;
    case rest::RequestType::Put:
      return fuerte::RestVerb::Put;
    case rest::RequestType::Head:
      return fuerte::RestVerb::Head;
    case rest::RequestType::Patch:
      return fuerte::RestVerb::Patch;
    case rest::RequestType::Options:
      return fuerte::RestVerb::Options;
    case rest::RequestType::Illegal:
      return fuerte::RestVerb::Illegal;
  }

  return fuerte::RestVerb::Illegal;
}

rest::RequestType FuerteRestVerbToSerene(fuerte::RestVerb verb) {
  switch (verb) {
    case fuerte::RestVerb::Illegal:
      return rest::RequestType::Illegal;
    case fuerte::RestVerb::Delete:
      return rest::RequestType::DeleteReq;
    case fuerte::RestVerb::Get:
      return rest::RequestType::Get;
    case fuerte::RestVerb::Post:
      return rest::RequestType::Post;
    case fuerte::RestVerb::Put:
      return rest::RequestType::Put;
    case fuerte::RestVerb::Head:
      return rest::RequestType::Head;
    case fuerte::RestVerb::Patch:
      return rest::RequestType::Patch;
    case fuerte::RestVerb::Options:
      return rest::RequestType::Options;
  }

  return rest::RequestType::Illegal;
}

ErrorCode FuerteToSereneErrorCode(const network::Response& res) {
  SDB_ERROR_IF(
    "xxxxx", Logger::COMMUNICATION, res.error != fuerte::Error::NoError,
    "communication error: '", fuerte::ToString(res.error),
    "' from destination '", res.destination, "'",
    [](const network::Response& res) {
      if (res.hasRequest()) {
        return std::string(", url: ") +
               ToString(res.request().header.rest_verb) + " " +
               res.request().header.path;
      }
      return std::string();
    }(res),
    ", request ptr: ",
    (res.hasRequest() ? std::bit_cast<size_t>(&res.request()) : 0));
  return ToSereneErrorCodeInternal(res.error);
}

ErrorCode FuerteToSereneErrorCode(fuerte::Error err) {
  SDB_ERROR_IF("xxxxx", Logger::COMMUNICATION, err != fuerte::Error::NoError,
               "communication error: '", fuerte::ToString(err), "'");
  return ToSereneErrorCodeInternal(err);
}

std::string_view FuerteToSereneErrorMessage(const network::Response& res) {
  // TODO(mbkkt) return string_view
  if (res.payloadSize() > 0) {
    // check "errorMessage" attribute first
    vpack::Slice s = res.slice();
    if (s.isObject()) {
      s = s.get(StaticStrings::kErrorMessage);
      if (s.isString() && !s.isEmptyString()) {
        return s.stringView();
      }
    }
  }
  return GetErrorStr(FuerteToSereneErrorCode(res));
}

std::string_view FuerteToSereneErrorMessage(fuerte::Error err) {
  return GetErrorStr(FuerteToSereneErrorCode(err));
}

ErrorCode FuerteStatusToSereneErrorCode(const fuerte::Response& res) {
  return FuerteStatusToSereneErrorCode(res.statusCode());
}

ErrorCode FuerteStatusToSereneErrorCode(const fuerte::StatusCode& status_code) {
  if (fuerte::StatusIsSuccess(status_code)) {
    return ERROR_OK;
  } else if (status_code > 0) {
    return ErrorCode{static_cast<int>(status_code)};
  } else {
    return ERROR_INTERNAL;
  }
}

std::string FuerteStatusToSereneErrorMessage(const fuerte::Response& res) {
  return FuerteStatusToSereneErrorMessage(res.statusCode());
}

std::string FuerteStatusToSereneErrorMessage(
  const fuerte::StatusCode& status_code) {
  return fuerte::StatusCodeToString(status_code);
}

void AddSourceHeader(consensus::Agent* agent, fuerte::Request& req) {
  // note: agent can be a nullptr here
  auto state = ServerState::instance();
  if (state->IsCoordinator() || state->IsDBServer()) {
    req.header.addMeta(StaticStrings::kClusterCommSource, state->GetId());
#if 0
  } else if (state->IsAgent() && agent != nullptr) {
    // note: header intentionally not sent to save cluster-internal
    // traffic
    req.header.addMeta(StaticStrings::ClusterCommSource, agent->id());
#endif
  }
}

void AddUserParameter(RequestOptions& req_opts, std::string_view value) {
  if (!value.empty()) {
    // if no user name is set, we cannot add it to the request options
    // as a URL parameter, because they will assert that the provided
    // value is non-empty
    req_opts.param(StaticStrings::kUserString, value);
  }
}

}  // namespace sdb::network
