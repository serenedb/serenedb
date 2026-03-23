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

#include "general_response.h"

#include <vpack/vpack_helper.h>

#include "basics/debugging.h"
#include "basics/string_utils.h"

using namespace sdb;
using namespace sdb::basics;

bool GeneralResponse::isValidResponseCode(uint64_t code) {
  return ((code >= 100) && (code < 600));
}

std::string GeneralResponse::responseString(ResponseCode code) {
  switch (code) {
    //  Informational 1xx
    case ResponseCode::Continue:
      return "100 Continue";
    case ResponseCode::SwitchingProtocols:
      return "101 Switching Protocols";
    case ResponseCode::Processing:
      return "102 Processing";

    //  Success 2xx
    case ResponseCode::Ok:
      return "200 OK";
    case ResponseCode::Created:
      return "201 Created";
    case ResponseCode::Accepted:
      return "202 Accepted";
    case ResponseCode::Partial:
      return "203 Non-Authoritative Information";
    case ResponseCode::NoContent:
      return "204 No Content";
    case ResponseCode::ResetContent:
      return "205 Reset Content";
    case ResponseCode::PartialContent:
      return "206 Partial Content";

    //  Redirection 3xx
    case ResponseCode::MovedPermanently:
      return "301 Moved Permanently";
    case ResponseCode::Found:
      return "302 Found";
    case ResponseCode::SeeOther:
      return "303 See Other";
    case ResponseCode::NotModified:
      return "304 Not Modified";
    case ResponseCode::TemporaryRedirect:
      return "307 Temporary Redirect";
    case ResponseCode::PermanentRedirect:
      return "308 Permanent Redirect";

    //  Client Error 4xx
    case ResponseCode::Bad:
      return "400 Bad Request";
    case ResponseCode::Unauthorized:
      return "401 Unauthorized";
    case ResponseCode::PaymentRequired:
      return "402 Payment Required";
    case ResponseCode::Forbidden:
      return "403 Forbidden";
    case ResponseCode::NotFound:
      return "404 Not Found";
    case ResponseCode::MethodNotAllowed:
      return "405 Method Not Allowed";
    case ResponseCode::NotAcceptable:
      return "406 Not Acceptable";
    case ResponseCode::RequestTimeout:
      return "408 Request Timeout";
    case ResponseCode::Conflict:
      return "409 Conflict";
    case ResponseCode::Gone:
      return "410 Gone";
    case ResponseCode::LengthRequired:
      return "411 Length Required";
    case ResponseCode::PreconditionFailed:
      return "412 Precondition Failed";
    case ResponseCode::RequestEntityTooLarge:
      return "413 Payload Too Large";
    case ResponseCode::RequestUriTooLong:
      return "414 Request-URI Too Long";
    case ResponseCode::UnsupportedMediaType:
      return "415 Unsupported Media Type";
    case ResponseCode::RequestedRangeNotSatisfiable:
      return "416 Requested Range Not Satisfiable";
    case ResponseCode::ExpectationFailed:
      return "417 Expectation Failed";
    case ResponseCode::IAmATeapot:
      return "418 I'm a teapot";
    case ResponseCode::EnhanceYourCalm:
      return "420 Enhance Your Calm";
    case ResponseCode::UnprocessableEntity:
      return "422 Unprocessable Entity";
    case ResponseCode::Locked:
      return "423 Locked";
    case ResponseCode::PreconditionRequired:
      return "428 Precondition Required";
    case ResponseCode::TooManyRequests:
      return "429 Too Many Requests";
    case ResponseCode::RequestHeaderFieldsTooLarge:
      return "431 Request Header Fields Too Large";
    case ResponseCode::UnavailableForLegalReasons:
      return "451 Unavailable For Legal Reasons";

    //  Server Error 5xx
    case ResponseCode::ServerError:
      return "500 Internal Server Error";
    case ResponseCode::NotImplemented:
      return "501 Not Implemented";
    case ResponseCode::BadGateway:
      return "502 Bad Gateway";
    case ResponseCode::ServiceUnavailable:
      return "503 Service Unavailable";
    case ResponseCode::HttpVersionNotSupported:
      return "505 HTTP Version Not Supported";
    case ResponseCode::BandwidthLimitExceeded:
      return "509 Bandwidth Limit Exceeded";
    case ResponseCode::NotExtended:
      return "510 Not Extended";

    // default
    default: {
      // print generic group responses, based on error code group
      int group = ((int)code) / 100;
      switch (group) {
        case 1:
          return string_utils::Itoa((int)code) + " Informational";
        case 2:
          return string_utils::Itoa((int)code) + " Success";
        case 3:
          return string_utils::Itoa((int)code) + " Redirection";
        case 4:
          return string_utils::Itoa((int)code) + " Client error";
        case 5:
          return string_utils::Itoa((int)code) + " Server error";
        case 0:
          if (static_cast<int>(code) != 0) {
            return string_utils::Itoa(500) + " Internal Server error";
          }
          break;
        default:
          break;
      }
    }
  }

  return string_utils::Itoa(500) + " Internal Server error - Unknown";
}

rest::ResponseCode GeneralResponse::responseCode(const std::string& str) {
  int number = ::atoi(str.c_str());

  switch (number) {
    case 100:
      return ResponseCode::Continue;
    case 101:
      return ResponseCode::SwitchingProtocols;
    case 102:
      return ResponseCode::Processing;

    case 200:
      return ResponseCode::Ok;
    case 201:
      return ResponseCode::Created;
    case 202:
      return ResponseCode::Accepted;
    case 203:
      return ResponseCode::Partial;
    case 204:
      return ResponseCode::NoContent;
    case 205:
      return ResponseCode::ResetContent;
    case 206:
      return ResponseCode::PartialContent;

    case 301:
      return ResponseCode::MovedPermanently;
    case 302:
      return ResponseCode::Found;
    case 303:
      return ResponseCode::SeeOther;
    case 304:
      return ResponseCode::NotModified;
    case 307:
      return ResponseCode::TemporaryRedirect;
    case 308:
      return ResponseCode::PermanentRedirect;

    case 400:
      return ResponseCode::Bad;
    case 401:
      return ResponseCode::Unauthorized;
    case 402:
      return ResponseCode::PaymentRequired;
    case 403:
      return ResponseCode::Forbidden;
    case 404:
      return ResponseCode::NotFound;
    case 405:
      return ResponseCode::MethodNotAllowed;
    case 406:
      return ResponseCode::NotAcceptable;
    case 408:
      return ResponseCode::RequestTimeout;
    case 409:
      return ResponseCode::Conflict;
    case 410:
      return ResponseCode::Gone;
    case 411:
      return ResponseCode::LengthRequired;
    case 412:
      return ResponseCode::PreconditionFailed;
    case 413:
      return ResponseCode::RequestEntityTooLarge;
    case 414:
      return ResponseCode::RequestUriTooLong;
    case 415:
      return ResponseCode::UnsupportedMediaType;
    case 416:
      return ResponseCode::RequestedRangeNotSatisfiable;
    case 417:
      return ResponseCode::ExpectationFailed;
    case 418:
      return ResponseCode::IAmATeapot;
    case 420:
      return ResponseCode::EnhanceYourCalm;
    case 422:
      return ResponseCode::UnprocessableEntity;
    case 423:
      return ResponseCode::Locked;
    case 428:
      return ResponseCode::PreconditionRequired;
    case 429:
      return ResponseCode::TooManyRequests;
    case 431:
      return ResponseCode::RequestHeaderFieldsTooLarge;
    case 451:
      return ResponseCode::UnavailableForLegalReasons;

    case 500:
      return ResponseCode::ServerError;
    case 501:
      return ResponseCode::NotImplemented;
    case 502:
      return ResponseCode::BadGateway;
    case 503:
      return ResponseCode::ServiceUnavailable;
    case 505:
      return ResponseCode::HttpVersionNotSupported;
    case 509:
      return ResponseCode::BandwidthLimitExceeded;
    case 510:
      return ResponseCode::NotExtended;

    default:
      return ResponseCode::NotImplemented;
  }
}

rest::ResponseCode GeneralResponse::responseCode(ErrorCode code) {
  SDB_ASSERT(code != ERROR_OK);

  switch (static_cast<int>(code)) {
    case static_cast<int>(ERROR_HTTP_CORRUPTED_JSON):
    case static_cast<int>(ERROR_BAD_PARAMETER):
    case static_cast<int>(ERROR_SERVER_DATABASE_NAME_INVALID):
    case static_cast<int>(ERROR_SERVER_DOCUMENT_KEY_BAD):
    case static_cast<int>(ERROR_SERVER_DOCUMENT_KEY_UNEXPECTED):
    case static_cast<int>(ERROR_SERVER_DOCUMENT_KEY_MISSING):
    case static_cast<int>(ERROR_SERVER_DOCUMENT_TYPE_INVALID):
    case static_cast<int>(ERROR_SERVER_DOCUMENT_HANDLE_BAD):
    case static_cast<int>(ERROR_CLUSTER_TOO_MANY_SHARDS):
    case static_cast<int>(ERROR_CLUSTER_MUST_NOT_CHANGE_SHARDING_ATTRIBUTES):
    case static_cast<int>(ERROR_CLUSTER_MUST_NOT_SPECIFY_KEY):
    case static_cast<int>(ERROR_CLUSTER_NOT_ALL_SHARDING_ATTRIBUTES_GIVEN):
    case static_cast<int>(ERROR_TYPE_ERROR):
    case static_cast<int>(ERROR_QUERY_NUMBER_OUT_OF_RANGE):
    case static_cast<int>(ERROR_QUERY_VARIABLE_NAME_INVALID):
    case static_cast<int>(ERROR_QUERY_VARIABLE_REDECLARED):
    case static_cast<int>(ERROR_QUERY_VARIABLE_NAME_UNKNOWN):
    case static_cast<int>(ERROR_QUERY_COLLECTION_LOCK_FAILED):
    case static_cast<int>(ERROR_QUERY_TOO_MANY_COLLECTIONS):
    case static_cast<int>(ERROR_QUERY_FUNCTION_NAME_UNKNOWN):
    case static_cast<int>(ERROR_QUERY_FUNCTION_ARGUMENT_NUMBER_MISMATCH):
    case static_cast<int>(ERROR_QUERY_FUNCTION_ARGUMENT_TYPE_MISMATCH):
    case static_cast<int>(ERROR_QUERY_INVALID_REGEX):
    case static_cast<int>(ERROR_QUERY_BIND_PARAMETERS_INVALID):
    case static_cast<int>(ERROR_QUERY_BIND_PARAMETER_MISSING):
    case static_cast<int>(ERROR_QUERY_BIND_PARAMETER_TYPE):
    case static_cast<int>(ERROR_QUERY_INVALID_ARITHMETIC_VALUE):
    case static_cast<int>(ERROR_QUERY_DIVISION_BY_ZERO):
    case static_cast<int>(ERROR_QUERY_ARRAY_EXPECTED):
    case static_cast<int>(ERROR_QUERY_FAIL_CALLED):
    case static_cast<int>(ERROR_QUERY_INVALID_DATE_VALUE):
    case static_cast<int>(ERROR_QUERY_MULTI_MODIFY):
    case static_cast<int>(ERROR_QUERY_TOO_MUCH_NESTING):
    case static_cast<int>(ERROR_QUERY_COMPILE_TIME_OPTIONS):
    case static_cast<int>(ERROR_QUERY_INVALID_OPTIONS_ATTRIBUTE):
    case static_cast<int>(ERROR_QUERY_DISALLOWED_DYNAMIC_CALL):
    case static_cast<int>(ERROR_QUERY_ACCESS_AFTER_MODIFICATION):
    case static_cast<int>(ERROR_QUERY_FUNCTION_INVALID_NAME):
    case static_cast<int>(ERROR_QUERY_FUNCTION_INVALID_CODE):
    case static_cast<int>(ERROR_REPLICATION_INVALID_APPLIER_CONFIGURATION):
    case static_cast<int>(ERROR_REPLICATION_RUNNING):
    case static_cast<int>(ERROR_REPLICATION_NO_START_TICK):
    case static_cast<int>(ERROR_SERVER_INVALID_KEY_GENERATOR):
    case static_cast<int>(ERROR_SERVER_INVALID_EDGE_ATTRIBUTE):
    case static_cast<int>(ERROR_SERVER_INDEX_CREATION_FAILED):
    case static_cast<int>(ERROR_SERVER_OBJECT_TYPE_MISMATCH):
    case static_cast<int>(ERROR_SERVER_COLLECTION_TYPE_INVALID):
    case static_cast<int>(ERROR_SERVER_ATTRIBUTE_PARSER_FAILED):
    case static_cast<int>(ERROR_SERVER_CROSS_COLLECTION_REQUEST):
    case static_cast<int>(ERROR_SERVER_ILLEGAL_NAME):
    case static_cast<int>(ERROR_SERVER_INDEX_HANDLE_BAD):
    case static_cast<int>(ERROR_SERVER_DOCUMENT_TOO_LARGE):
    case static_cast<int>(ERROR_QUERY_PARSE):
    case static_cast<int>(ERROR_QUERY_EMPTY):
    case static_cast<int>(ERROR_TRANSACTION_NESTED):
    case static_cast<int>(ERROR_TRANSACTION_UNREGISTERED_COLLECTION):
    case static_cast<int>(ERROR_TRANSACTION_DISALLOWED_OPERATION):
    case static_cast<int>(ERROR_USER_INVALID_NAME):
    case static_cast<int>(ERROR_TASK_INVALID_ID):
    case static_cast<int>(ERROR_GRAPH_CREATE_MALFORMED_EDGE_DEFINITION):
    case static_cast<int>(ERROR_GRAPH_WRONG_COLLECTION_TYPE_VERTEX):
    case static_cast<int>(ERROR_GRAPH_NOT_IN_ORPHAN_COLLECTION):
    case static_cast<int>(ERROR_GRAPH_COLLECTION_USED_IN_EDGE_DEF):
    case static_cast<int>(ERROR_GRAPH_INVALID_NUMBER_OF_ARGUMENTS):
    case static_cast<int>(ERROR_GRAPH_EDGE_COL_DOES_NOT_EXIST):
    case static_cast<int>(ERROR_VALIDATION_FAILED):
    case static_cast<int>(ERROR_VALIDATION_BAD_PARAMETER):
      return ResponseCode::Bad;

    case static_cast<int>(ERROR_SERVER_USE_SYSTEM_DATABASE):
    case static_cast<int>(ERROR_SERVER_READ_ONLY):
    case static_cast<int>(ERROR_FORBIDDEN):
      return ResponseCode::Forbidden;

    case static_cast<int>(ERROR_HTTP_NOT_FOUND):
    case static_cast<int>(ERROR_SERVER_DATABASE_NOT_FOUND):
    case static_cast<int>(ERROR_SERVER_DATA_SOURCE_NOT_FOUND):
    case static_cast<int>(ERROR_SERVER_COLLECTION_NOT_LOADED):
    case static_cast<int>(ERROR_SERVER_DOCUMENT_NOT_FOUND):
    case static_cast<int>(ERROR_SERVER_INDEX_NOT_FOUND):
    case static_cast<int>(ERROR_CURSOR_NOT_FOUND):
    case static_cast<int>(ERROR_QUERY_FUNCTION_NOT_FOUND):
    case static_cast<int>(ERROR_QUERY_NOT_FOUND):
    case static_cast<int>(ERROR_USER_NOT_FOUND):
    case static_cast<int>(ERROR_TRANSACTION_NOT_FOUND):
    case static_cast<int>(ERROR_TASK_NOT_FOUND):
    case static_cast<int>(ERROR_GRAPH_VERTEX_COL_DOES_NOT_EXIST):
      return ResponseCode::NotFound;

    case static_cast<int>(ERROR_CLUSTER_SHARD_LEADER_REFUSES_REPLICATION):
    case static_cast<int>(ERROR_CLUSTER_SHARD_FOLLOWER_REFUSES_OPERATION):
      return ResponseCode::NotAcceptable;

    case static_cast<int>(ERROR_REQUEST_CANCELED):
    case static_cast<int>(ERROR_QUERY_KILLED):
    case static_cast<int>(ERROR_TRANSACTION_ABORTED):
      return ResponseCode::Gone;

    case static_cast<int>(ERROR_SERVER_CONFLICT):
    case static_cast<int>(ERROR_SERVER_DUPLICATE_NAME):
    case static_cast<int>(ERROR_SERVER_UNIQUE_CONSTRAINT_VIOLATED):
    case static_cast<int>(ERROR_CURSOR_BUSY):
    case static_cast<int>(ERROR_USER_DUPLICATE):
    case static_cast<int>(ERROR_TASK_DUPLICATE_ID):
    case static_cast<int>(ERROR_CLUSTER_FOLLOWER_TRANSACTION_COMMIT_PERFORMED):
      return ResponseCode::Conflict;

    case static_cast<int>(ERROR_HTTP_PRECONDITION_FAILED):
    case static_cast<int>(ERROR_CLUSTER_CREATE_COLLECTION_PRECONDITION_FAILED):
      return ResponseCode::PreconditionFailed;

    case static_cast<int>(ERROR_DEADLOCK):
    case static_cast<int>(ERROR_SERVER_OUT_OF_KEYS):
    case static_cast<int>(ERROR_CLUSTER_SHARD_GONE):
    case static_cast<int>(ERROR_CLUSTER_TIMEOUT):
    case static_cast<int>(ERROR_LOCK_TIMEOUT):
    case static_cast<int>(ERROR_LOCKED):
    case static_cast<int>(ERROR_DEBUG):
    case static_cast<int>(ERROR_OUT_OF_MEMORY):
    case static_cast<int>(ERROR_INTERNAL):
    case static_cast<int>(ERROR_TRANSACTION_INTERNAL):
      return ResponseCode::ServerError;

    case static_cast<int>(ERROR_CLUSTER_SHARD_LEADER_RESIGNED):
      return ResponseCode::MisdirectedRequest;

    case static_cast<int>(ERROR_CLUSTER_BACKEND_UNAVAILABLE):
    case static_cast<int>(ERROR_CLUSTER_LEADERSHIP_CHALLENGE_ONGOING):
    case static_cast<int>(ERROR_CLUSTER_NOT_LEADER):
    case static_cast<int>(ERROR_SHUTTING_DOWN):
    case static_cast<int>(ERROR_STARTING_UP):
    case static_cast<int>(ERROR_CLUSTER_CONNECTION_LOST):
    case static_cast<int>(ERROR_REPLICATION_WRITE_CONCERN_NOT_FULFILLED):
      return ResponseCode::ServiceUnavailable;

    case static_cast<int>(ERROR_HTTP_NOT_IMPLEMENTED):
    case static_cast<int>(ERROR_CLUSTER_UNSUPPORTED):
    case static_cast<int>(ERROR_NOT_IMPLEMENTED):
    case static_cast<int>(ERROR_CLUSTER_ONLY_ON_COORDINATOR):
    case static_cast<int>(ERROR_CLUSTER_ONLY_ON_DBSERVER):
      return ResponseCode::NotImplemented;

    default:
      return ResponseCode::ServerError;
  }
}

GeneralResponse::GeneralResponse(ResponseCode response_code, uint64_t mid)
  : _message_id(mid),
    _response_code(response_code),
    _content_type(ContentType::Unset),
    _content_type_requested(ContentType::Unset),
    _generate_body(true) {}
