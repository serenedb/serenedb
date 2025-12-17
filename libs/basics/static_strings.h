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
    "the GNU Lesser General Public License (LGPL), see "
    "https://www.gnu.org/copyleft/lesser.html and "
    "https://www.gnu.org/licenses/gpl.html";

  static constexpr std::string_view kRocksDbEngineRoot = "engine_rocksdb";

  // constants
  static const std::string kEmpty;
  static const std::string kN1800;

  // index lookup strings
  static const std::string kIndexEq;
  static const std::string kIndexIn;
  static const std::string kIndexLe;
  static const std::string kIndexLt;
  static const std::string kIndexGe;
  static const std::string kIndexGt;

  // system attribute names
  static constexpr std::string_view kKeyString = "_key";
  static constexpr std::string_view kRevString = "_rev";
  static constexpr std::string_view kFromString = "_from";
  static constexpr std::string_view kToString = "_to";

  static constexpr std::string_view kDoc = "doc";

  // URL parameter names
  static const std::string kIgnoreRevsString;
  static const std::string kIsRestoreString;
  static const std::string kKeepNullString;
  static const std::string kMergeObjectsString;
  static const std::string kReturnNewString;
  static const std::string kReturnOldString;
  static const std::string kSilentString;
  static const std::string kWaitForSyncString;
  static const std::string kSkipDocumentValidation;
  static const std::string kIsSynchronousReplicationString;
  static const std::string kVersionAttributeString;
  static const std::string kOverwrite;
  static const std::string kOverwriteMode;
  static const std::string kCompact;
  static const std::string kUserString;

  // dump headers
  static const std::string kDumpAuthUser;
  static const std::string kDumpBlockCounts;
  static const std::string kDumpId;
  static const std::string kDumpShardId;

  // replication headers
  static const std::string kReplicationHeaderCheckMore;
  static const std::string kReplicationHeaderLastIncluded;
  static const std::string kReplicationHeaderLastScanned;
  static const std::string kReplicationHeaderLastTick;
  static const std::string kReplicationHeaderFromPresent;

  // database names
  static constexpr std::string_view kSystemDatabase = "_system";
  static constexpr std::string_view kPublic = "public";
  static constexpr std::string_view kCalculationDatabase = "Z";
  static constexpr std::string_view kPgDefaultDatabase = "postgres";
  // system schema names
  static constexpr std::string_view kPgCatalogSchema = "pg_catalog";
  static constexpr std::string_view kInformationSchema = "information_schema";

  // user names
  static constexpr std::string_view kPgDefaultUser = "postgres";

  // database definition fields
  static const std::string kDatabaseId;
  static const std::string kDatabaseName;
  static const std::string kProperties;

  static const std::string kDataSourceId;
  static const std::string kDataSourceName;
  static const std::string kDataSourcePlanId;
  static constexpr std::string_view kDataSourceType = "type";
  static const std::string kDataSourceParameters;

  // Index definition fields
  static const std::string
    kIndexDeduplicate;  // index deduplicate flag (for array indexes)
  static const std::string kIndexExpireAfter;   // ttl index expire value
  static const std::string kIndexFields;        // index fields
  static const std::string kIndexId;            // index id
  static const std::string kIndexInBackground;  // index in background
  static constexpr std::string_view kIndexParallelism{"parallelism"};
  static const std::string kIndexName;          // index name
  static const std::string kIndexSparse;        // index sparsity marker
  static const std::string kIndexStoredValues;  // index stored values
  static const std::string kIndexType;          // index type
  static const std::string kIndexUnique;        // index uniqueness marker
  static const std::string kIndexEstimates;     // index estimates flag
  static constexpr std::string_view kIndexLookahead{"lookahead"};

  // static index names
  static const std::string kIndexNameEdge;
  static const std::string kIndexNameEdgeFrom;
  static const std::string kIndexNameEdgeTo;
  static const std::string kIndexNamePrimary;

  // index hint strings
  static const std::string kIndexHintDisableIndex;
  static const std::string kIndexHintOption;
  static const std::string kIndexHintOptionForce;

  // query options
  static const std::string kFilter;
  static const std::string kMaxProjections;
  static const std::string kProducesResult;
  static const std::string kReadOwnWrites;
  static const std::string kParallelism;
  static const std::string kForceColocatedAttributeValue;
  static const std::string kJoinStrategyType;

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
  static const std::string kBatchContentType;
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
  static const std::string kEtag;
  static const std::string kExpect;
  static const std::string kExposedCorsHeaders;
  static const std::string kHlcHeader;
  static const std::string kLocation;
  static const std::string kLockLocation;
  static const std::string kNoSniff;
  static const std::string kOrigin;
  static const std::string kPotentialDirtyRead;
  static const std::string kRequestForwardedTo;
  static const std::string kServer;
  static const std::string kTransferEncoding;
  static const std::string kTransactionBody;
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
  static const std::string kMultiPartContentType;

  // encodings
  static const std::string kEncodingSereneLz4;
  static const std::string kEncodingDeflate;
  static const std::string kEncodingGzip;
  static const std::string kEncodingLz4;

  // serenesh result body
  static const std::string kBody;
  static const std::string kParsedBody;

  // collection attributes
  static const std::string kAllowUserKeys;
  static constexpr std::string_view kDistributeShardsLike =
    "distributeShardsLike";
  static const std::string kIndexes;
  static const std::string kKeyOptions;
  static const std::string kNumberOfShards;
  static const std::string kObjectId;
  static const std::string kReplicationFactor;
  static const std::string kShardingStrategy;
  static const std::string kSchema;
  static const std::string kWriteConcern;

  // graph attribute names
  static constexpr std::string_view kGraphFrom = "from";
  static constexpr std::string_view kGraphTo = "to";
  static constexpr std::string_view kGraphOptions = "options";
  static constexpr std::string_view kGraphName = "name";
  static constexpr std::string_view kGraphTraversalProfileLevel =
    "traversalProfile";

  // Graph directions
  static const std::string kGraphDirection;

  // Query Strings
  static const std::string kQuerySortAsc;
  static const std::string kQuerySortDesc;

  // Graph Query Strings
  static const std::string kGraphQueryEdges;
  static const std::string kGraphQueryVertices;
  static const std::string kGraphQueryPath;
  static const std::string kGraphQueryGlobal;
  static const std::string kGraphQueryNone;
  static const std::string kGraphQueryWeight;
  static const std::string kGraphQueryWeights;
  static const std::string kGraphQueryOrder;
  static const std::string kGraphQueryOrderBfs;
  static const std::string kGraphQueryOrderDfs;
  static const std::string kGraphQueryOrderWeighted;
  static const std::string kGraphQueryShortestPathType;

  // Replication
  static const std::string kReplicationSoftLockOnly;
  static const std::string kFailoverCandidates;
  static const std::string kRevisionTreeCount;
  static const std::string kRevisionTreeHash;
  static const std::string kRevisionTreeMaxDepth;
  static const std::string kRevisionTreeNodes;
  static const std::string kRevisionTreeRangeMax;
  static const std::string kRevisionTreeRangeMin;
  static const std::string kRevisionTreeInitialRangeMin;
  static const std::string kRevisionTreeRanges;
  static const std::string kRevisionTreeResume;
  static const std::string kRevisionTreeVersion;
  static const std::string kFollowingTermId;

  // generic attribute names
  static constexpr std::string_view kAttrIsBuilding = "isBuilding";
  static constexpr std::string_view kAttrCoordinatorId = "coordinatorId";
  static constexpr std::string_view kAttrCoordinatorRebootId =
    "coordinatorRebootId";

  // misc strings
  static const std::string kLastValue;
  static const std::string kRebootId;
  static const std::string kNew;
  static const std::string kOld;
  static const std::string kUpgradeEnvName;
  static const std::string kBackupToDeleteName;
  static const std::string kBackupSearchToDeleteName;

  // aql api strings
  static const std::string kAqlDocumentCall;
  static const std::string kAqlFastPath;
  static const std::string kAqlRemoteExecute;
  static const std::string kAqlRemoteCallStack;
  static const std::string kAqlRemoteFullCount;
  static const std::string kAqlRemoteOffset;
  static const std::string kAqlRemoteInfinity;
  static const std::string kAqlRemoteResult;
  static const std::string kAqlRemoteBlock;
  static const std::string kAqlRemoteSkipped;
  static const std::string kAqlRemoteState;
  static const std::string kAqlRemoteStateDone;
  static const std::string kAqlRemoteStateHasmore;

  // aql http headers
  static const std::string kAqlShardIdHeader;

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

// TODO(mbkkt) remove all system attributes
inline bool IsDocumentSystemAttribute(std::string_view key) noexcept {
  switch (key.size()) {
    case 4:
      return key == StaticStrings::kKeyString ||
             key == StaticStrings::kRevString;
    default:
      return false;
  }
}

inline bool IsEdgeSystemAttribute(std::string_view key) noexcept {
  switch (key.size()) {
    case 3:
      return key == StaticStrings::kToString;
    case 4:
      return key == StaticStrings::kKeyString ||
             key == StaticStrings::kRevString;
    case 5:
      return key == StaticStrings::kFromString;
    default:
      return false;
  }
}

}  // namespace sdb
