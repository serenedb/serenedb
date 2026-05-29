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

#include <string>
#include <string_view>

namespace sdb {

class StaticStrings {
  StaticStrings() = delete;

 public:
  static constexpr std::string_view kLgplNotice =
    "This executable uses the GNU C library (glibc), which is licensed under "
    "the GNU Lesser General Public License v2.1 (LGPL-2.1), see "
    "https://www.gnu.org/licenses/old-licenses/lgpl-2.1.html";

  static constexpr std::string_view kRocksDbEngineRoot = "engine_rocksdb";
  static const std::string kSereneDBConnector;

  // constants
  static const std::string kEmpty;
  static const std::string kN1800;

  // URL parameter names
  static const std::string kWaitForSyncString;
  static const std::string kUserString;

  // database names
  static constexpr std::string_view kDefaultDatabase = "postgres";
  // user names
  static constexpr std::string_view kDefaultUser = "postgres";
  // system schema names
  static constexpr std::string_view kPublic = "public";
  static constexpr std::string_view kPgCatalogSchema = "pg_catalog";
  static constexpr std::string_view kInformationSchema = "information_schema";

  static const std::string kDataSourceId;
  static const std::string kDataSourceName;
  static constexpr std::string_view kDataSourceType = "type";

  // query options
  static const std::string kFilter;

  // HTTP headers
  static const std::string kAccept;
  static const std::string kAcceptEncoding;
  static const std::string kAccessControlAllowCredentials;
  static const std::string kAccessControlAllowHeaders;
  static const std::string kAccessControlAllowMethods;
  static const std::string kAccessControlAllowOrigin;
  static const std::string kAccessControlExposeHeaders;
  static const std::string kAccessControlMaxAge;
  static const std::string kAccessControlRequestHeaders;
  static const std::string kAllow;
  static const std::string kAllowDirtyReads;
  static const std::string kAsync;
  static const std::string kAsyncId;
  static const std::string kAuthorization;
  static const std::string kCacheControl;
  static const std::string kChunked;
  static const std::string kClusterCommSource;
  static const std::string kCode;
  static const std::string kConnection;
  static const std::string kContentEncoding;
  static const std::string kContentLength;
  static const std::string kContentTypeHeader;
  static const std::string kCookie;
  static const std::string kCorsMethods;
  static const std::string kError;
  static const std::string kErrorCode;
  static const std::string kErrorMessage;
  static const std::string kErrorNum;
  static const std::string kErrors;
  static const std::string kErrorCodes;
  static const std::string kExpect;
  static const std::string kExposedCorsHeaders;
  static const std::string kHlcHeader;
  static const std::string kLocation;
  static const std::string kNoSniff;
  static const std::string kOrigin;
  static const std::string kRequestForwardedTo;
  static const std::string kServer;
  static const std::string kTransferEncoding;
  static const std::string kTransactionId;
  static const std::string kWwwAuthenticate;
  static const std::string kXContentTypeOptions;
  static const std::string kXSereneFrontend;
  static const std::string kXSereneQueueTimeSeconds;
  static const std::string kContentSecurityPolicy;
  static const std::string kPragma;
  static const std::string kExpires;
  static const std::string kHsts;

  // mime types
  static const std::string kMimeTypeDump;
  static const std::string kMimeTypeDumpNoEncoding;
  static const std::string kMimeTypeHtml;
  static const std::string kMimeTypeHtmlNoEncoding;
  static const std::string kMimeTypeJson;
  static const std::string kMimeTypeJsonNoEncoding;
  static const std::string kMimeTypeText;
  static const std::string kMimeTypeTextNoEncoding;
  static const std::string kMimeTypeVPack;

  // encodings
  static const std::string kEncodingSereneLz4;
  static const std::string kEncodingDeflate;
  static const std::string kEncodingGzip;
  static const std::string kEncodingLz4;

  // collection attributes
  static const std::string kAllowUserKeys;
  static const std::string kObjectId;

  // generic attribute names
  static constexpr std::string_view kAttrCoordinatorId = "coordinatorId";
  static constexpr std::string_view kAttrCoordinatorRebootId =
    "coordinatorRebootId";

  // misc strings
  static const std::string kLastValue;

  // Replication
  static const std::string kRevisionTreeCount;
  static const std::string kRevisionTreeHash;
  static const std::string kRevisionTreeMaxDepth;
  static const std::string kRevisionTreeNodes;
  static const std::string kRevisionTreeRangeMax;
  static const std::string kRevisionTreeRangeMin;
  static const std::string kRevisionTreeInitialRangeMin;
  static const std::string kRevisionTreeVersion;

  // validation
  static constexpr std::string_view kValidationLevelNone = "none";
  static constexpr std::string_view kValidationLevelNew = "new";
  static constexpr std::string_view kValidationLevelModerate = "moderate";
  static constexpr std::string_view kValidationLevelStrict = "strict";

  static constexpr std::string_view kValidationParameterMessage = "message";
  static constexpr std::string_view kValidationParameterLevel = "level";
  static constexpr std::string_view kValidationParameterRule = "rule";
  static constexpr std::string_view kValidationParameterType = "type";
};

}  // namespace sdb
