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

#include <cstdint>
#include <ostream>
#include <string>

namespace sdb::rest {

enum class RequestType {
  DeleteReq = 0,  // windows redefines DELETE
  Get,
  Post,
  Put,
  Head,
  Patch,
  Options,
  Illegal,  // must be last
};

inline const char* RequestToString(RequestType request_type) {
  switch (request_type) {
    case RequestType::DeleteReq:
      return "DELETE";
    case RequestType::Get:
      return "GET";
    case RequestType::Post:
      return "POST";
    case RequestType::Put:
      return "PUT";
    case RequestType::Head:
      return "HEAD";
    case RequestType::Patch:
      return "PATCH";
    case RequestType::Options:
      return "OPTIONS";
    case RequestType::Illegal:
    default:
      return "ILLEGAL";
  }
}

enum class ContentType {
  Custom,  // use Content-Type from _headers
  Json,    // application/json
  Text,    // text/plain
  Html,    // text/html
  Dump,    // application/x-serene-dump
  Unset,
};

std::string ContentTypeToString(ContentType type);
ContentType StringToContentType(const std::string& input, ContentType def);

enum class ResponseCompressionType {
  Unset,
  NoCompression,
  AllowCompression,
};

enum class EncodingType {
  Deflate,
  GZip,
  Lz4,
  Unset,
};

enum class AuthenticationMethod : uint8_t {
  None = 0,
  Basic = 1,
  Jwt = 2,
};

enum class ResponseCode {
  Continue = 100,
  SwitchingProtocols = 101,
  Processing = 102,

  Ok = 200,
  Created = 201,
  Accepted = 202,
  Partial = 203,
  NoContent = 204,
  ResetContent = 205,
  PartialContent = 206,

  MovedPermanently = 301,
  Found = 302,
  SeeOther = 303,
  NotModified = 304,
  TemporaryRedirect = 307,
  PermanentRedirect = 308,

  Bad = 400,
  Unauthorized = 401,
  PaymentRequired = 402,
  Forbidden = 403,
  NotFound = 404,
  MethodNotAllowed = 405,
  NotAcceptable = 406,
  RequestTimeout = 408,
  Conflict = 409,
  Gone = 410,
  LengthRequired = 411,
  PreconditionFailed = 412,
  RequestEntityTooLarge = 413,
  RequestUriTooLong = 414,
  UnsupportedMediaType = 415,
  RequestedRangeNotSatisfiable = 416,
  ExpectationFailed = 417,
  IAmATeapot = 418,
  EnhanceYourCalm = 420,
  MisdirectedRequest = 421,
  UnprocessableEntity = 422,
  Locked = 423,
  PreconditionRequired = 428,
  TooManyRequests = 429,
  RequestHeaderFieldsTooLarge = 431,
  UnavailableForLegalReasons = 451,

  ServerError = 500,
  NotImplemented = 501,
  BadGateway = 502,
  ServiceUnavailable = 503,
  GatewayTimeout = 504,
  HttpVersionNotSupported = 505,
  BandwidthLimitExceeded = 509,
  NotExtended = 510,
};

inline const char* ResponseToString(ResponseCode response_code) {
  switch (response_code) {
    case ResponseCode::Continue:
      return "100 CONTINUE";
    case ResponseCode::SwitchingProtocols:
      return "101 SWITCHING_PROTOCOLS";
    case ResponseCode::Processing:
      return "102 PROCESSING";
    case ResponseCode::Ok:
      return "200 OK";
    case ResponseCode::Created:
      return "201 CREATED";
    case ResponseCode::Accepted:
      return "202 ACCEPTED";
    case ResponseCode::Partial:
      return "203 PARTIAL";
    case ResponseCode::NoContent:
      return "204 NO_CONTENT";
    case ResponseCode::ResetContent:
      return "205 RESET_CONTENT";
    case ResponseCode::PartialContent:
      return "206 PARTIAL_CONTENT";
    case ResponseCode::MovedPermanently:
      return "301 MOVED_PERMANENTLY";
    case ResponseCode::Found:
      return "302 FOUND";
    case ResponseCode::SeeOther:
      return "303 SEE_OTHER";
    case ResponseCode::NotModified:
      return "304 NOT_MODIFIED";
    case ResponseCode::TemporaryRedirect:
      return "307 TEMPORARY_REDIRECT";
    case ResponseCode::PermanentRedirect:
      return "308 PERMANENT_REDIRECT";
    case ResponseCode::Bad:
      return "400 BAD";
    case ResponseCode::Unauthorized:
      return "401 UNAUTHORIZED";
    case ResponseCode::PaymentRequired:
      return "402 PAYMENT_REQUIRED";
    case ResponseCode::Forbidden:
      return "403 FORBIDDEN";
    case ResponseCode::NotFound:
      return "404 NOT_FOUND";
    case ResponseCode::MethodNotAllowed:
      return "405 METHOD_NOT_ALLOWED";
    case ResponseCode::NotAcceptable:
      return "406 NOT_ACCEPTABLE";
    case ResponseCode::RequestTimeout:
      return "408 REQUEST_TIMEOUT";
    case ResponseCode::Conflict:
      return "409 CONFLICT";
    case ResponseCode::Gone:
      return "410 GONE";
    case ResponseCode::LengthRequired:
      return "411 LENGTH_REQUIRED";
    case ResponseCode::PreconditionFailed:
      return "412 PRECONDITION_FAILED";
    case ResponseCode::RequestEntityTooLarge:
      return "413 REQUEST_ENTITY_TOO_LARGE";
    case ResponseCode::RequestUriTooLong:
      return "414 REQUEST_URI_TOO_LONG";
    case ResponseCode::UnsupportedMediaType:
      return "415 UNSUPPORTED_MEDIA_TYPE";
    case ResponseCode::RequestedRangeNotSatisfiable:
      return "416 REQUESTED_RANGE_NOT_SATISFIABLE";
    case ResponseCode::ExpectationFailed:
      return "417 EXPECTATION_FAILED";
    case ResponseCode::IAmATeapot:
      return "418 I_AM_A_TEAPOT";
    case ResponseCode::EnhanceYourCalm:
      return "420 ENHANCE YOUR CALM";
    case ResponseCode::MisdirectedRequest:
      return "421 MISDIRECTED_REQUEST";
    case ResponseCode::UnprocessableEntity:
      return "422 UNPROCESSABLE_ENTITY";
    case ResponseCode::Locked:
      return "423 LOCKED";
    case ResponseCode::PreconditionRequired:
      return "428 PRECONDITION_REQUIRED";
    case ResponseCode::TooManyRequests:
      return "429 TOO_MANY_REQUESTS";
    case ResponseCode::RequestHeaderFieldsTooLarge:
      return "431 REQUEST_HEADER_FIELDS_TOO_LARGE";
    case ResponseCode::UnavailableForLegalReasons:
      return "451 UNAVAILABLE_FOR_LEGAL_REASONS";
    case ResponseCode::ServerError:
      return "500 SERVER_ERROR";
    case ResponseCode::NotImplemented:
      return "501 NOT_IMPLEMENTED";
    case ResponseCode::BadGateway:
      return "502 BAD_GATEWAY";
    case ResponseCode::ServiceUnavailable:
      return "503 SERVICE_UNAVAILABLE";
    case ResponseCode::GatewayTimeout:
      return "504 GATEWAY_TIMEOUT";
    case ResponseCode::HttpVersionNotSupported:
      return "505 HTTP_VERSION_NOT_SUPPORTED";
    case ResponseCode::BandwidthLimitExceeded:
      return "509 BANDWIDTH_LIMIT_EXCEEDED";
    case ResponseCode::NotExtended:
      return "510 NOT_EXTENDED";
  }
  return "??? UNEXPECTED";
}

}  // namespace sdb::rest
