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

#include "static_strings.h"

using namespace sdb;

// constants
const std::string StaticStrings::kEmpty;
const std::string StaticStrings::kN1800("1800");

// SereneDB connector name
const std::string StaticStrings::kSereneDBConnector("serenedb");

// URL parameter names
const std::string StaticStrings::kWaitForSyncString("waitForSync");
const std::string StaticStrings::kUserString("user");

const std::string StaticStrings::kDataSourceId("id");
const std::string StaticStrings::kDataSourceName("name");

// query options
const std::string StaticStrings::kFilter("filter");

// HTTP headers
const std::string StaticStrings::kAccept("accept");
const std::string StaticStrings::kAcceptEncoding("accept-encoding");
const std::string StaticStrings::kAccessControlAllowCredentials(
  "access-control-allow-credentials");
const std::string StaticStrings::kAccessControlAllowHeaders(
  "access-control-allow-headers");
const std::string StaticStrings::kAccessControlAllowMethods(
  "access-control-allow-methods");
const std::string StaticStrings::kAccessControlAllowOrigin(
  "access-control-allow-origin");
const std::string StaticStrings::kAccessControlExposeHeaders(
  "access-control-expose-headers");
const std::string StaticStrings::kAccessControlMaxAge("access-control-max-age");
const std::string StaticStrings::kAccessControlRequestHeaders(
  "access-control-request-headers");
const std::string StaticStrings::kAllow("allow");
const std::string StaticStrings::kAllowDirtyReads("x-serene-allow-dirty-read");
const std::string StaticStrings::kAsync("x-serene-async");
const std::string StaticStrings::kAsyncId("x-serene-async-id");
const std::string StaticStrings::kAuthorization("authorization");
const std::string StaticStrings::kCacheControl("cache-control");
const std::string StaticStrings::kChunked("chunked");
const std::string StaticStrings::kClusterCommSource("x-serene-source");
const std::string StaticStrings::kCode("code");
const std::string StaticStrings::kConnection("connection");
const std::string StaticStrings::kContentEncoding("content-encoding");
const std::string StaticStrings::kContentLength("content-length");
const std::string StaticStrings::kContentTypeHeader("content-type");
const std::string StaticStrings::kCookie("cookie");
const std::string StaticStrings::kCorsMethods(
  "DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT");
const std::string StaticStrings::kError("error");
const std::string StaticStrings::kErrorCode("errorCode");
const std::string StaticStrings::kErrorMessage("errorMessage");
const std::string StaticStrings::kErrorNum("errorNum");
const std::string StaticStrings::kErrors("x-serene-errors");
const std::string StaticStrings::kErrorCodes("x-serene-error-codes");
const std::string StaticStrings::kExpect("expect");
const std::string StaticStrings::kExposedCorsHeaders(
  "etag, content-encoding, content-length, location, server, "
  "x-serene-errors, x-serene-async-id");
const std::string StaticStrings::kHlcHeader("x-serene-hlc");
const std::string StaticStrings::kLocation("location");
const std::string StaticStrings::kNoSniff("nosniff");
const std::string StaticStrings::kOrigin("origin");
const std::string StaticStrings::kRequestForwardedTo(
  "x-serene-request-forwarded-to");
const std::string StaticStrings::kServer("server");
const std::string StaticStrings::kTransferEncoding("transfer-encoding");
const std::string StaticStrings::kTransactionId("x-serene-trx-id");

const std::string StaticStrings::kWwwAuthenticate("www-authenticate");
const std::string StaticStrings::kXContentTypeOptions("x-content-type-options");
const std::string StaticStrings::kXSereneFrontend("x-serene-frontend");
const std::string StaticStrings::kXSereneQueueTimeSeconds(
  "x-serene-queue-time-seconds");
const std::string StaticStrings::kContentSecurityPolicy(
  "content-security-policy");
const std::string StaticStrings::kPragma("pragma");
const std::string StaticStrings::kExpires("expires");
const std::string StaticStrings::kHsts("strict-transport-security");

// mime types
const std::string StaticStrings::kMimeTypeDump(
  "application/x-serene-dump; charset=utf-8");
const std::string StaticStrings::kMimeTypeDumpNoEncoding(
  "application/x-serene-dump");
const std::string StaticStrings::kMimeTypeHtml("text/html; charset=utf-8");
const std::string StaticStrings::kMimeTypeHtmlNoEncoding("text/html");
const std::string StaticStrings::kMimeTypeJson(
  "application/json; charset=utf-8");
const std::string StaticStrings::kMimeTypeJsonNoEncoding("application/json");
const std::string StaticStrings::kMimeTypeText("text/plain; charset=utf-8");
const std::string StaticStrings::kMimeTypeTextNoEncoding("text/plain");
const std::string StaticStrings::kMimeTypeVPack("application/x-vpack");

// accept-encodings
const std::string StaticStrings::kEncodingSereneLz4("x-serene-lz4");
const std::string StaticStrings::kEncodingDeflate("deflate");
const std::string StaticStrings::kEncodingGzip("gzip");
const std::string StaticStrings::kEncodingLz4("lz4");

// collection attributes
const std::string StaticStrings::kAllowUserKeys("allowUserKeys");
const std::string StaticStrings::kObjectId("objectId");

// misc strings
const std::string StaticStrings::kLastValue("lastValue");

// Replication
const std::string StaticStrings::kRevisionTreeCount("count");
const std::string StaticStrings::kRevisionTreeHash("hash");
const std::string StaticStrings::kRevisionTreeMaxDepth("maxDepth");
const std::string StaticStrings::kRevisionTreeNodes("nodes");
const std::string StaticStrings::kRevisionTreeRangeMax("rangeMax");
const std::string StaticStrings::kRevisionTreeRangeMin("rangeMin");
const std::string StaticStrings::kRevisionTreeInitialRangeMin(
  "initialRangeMin");
const std::string StaticStrings::kRevisionTreeVersion("version");
