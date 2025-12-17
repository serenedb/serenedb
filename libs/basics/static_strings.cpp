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

// index lookup strings
const std::string StaticStrings::kIndexEq("eq");
const std::string StaticStrings::kIndexIn("in");
const std::string StaticStrings::kIndexLe("le");
const std::string StaticStrings::kIndexLt("lt");
const std::string StaticStrings::kIndexGe("ge");
const std::string StaticStrings::kIndexGt("gt");

// URL parameter names
const std::string StaticStrings::kIgnoreRevsString("ignoreRevs");
const std::string StaticStrings::kIsRestoreString("isRestore");
const std::string StaticStrings::kKeepNullString("keepNull");
const std::string StaticStrings::kMergeObjectsString("mergeObjects");
const std::string StaticStrings::kReturnNewString("returnNew");
const std::string StaticStrings::kReturnOldString("returnOld");
const std::string StaticStrings::kSilentString("silent");
const std::string StaticStrings::kWaitForSyncString("waitForSync");
const std::string StaticStrings::kSkipDocumentValidation(
  "skipDocumentValidation");
const std::string StaticStrings::kIsSynchronousReplicationString(
  "isSynchronousReplication");
const std::string StaticStrings::kVersionAttributeString("versionAttribute");
const std::string StaticStrings::kOverwrite("overwrite");
const std::string StaticStrings::kOverwriteMode("overwriteMode");
const std::string StaticStrings::kCompact("compact");
const std::string StaticStrings::kUserString("user");

// dump headers
const std::string StaticStrings::kDumpAuthUser("x-serene-dump-auth-user");
const std::string StaticStrings::kDumpBlockCounts("x-serene-dump-block-counts");
const std::string StaticStrings::kDumpId("x-serene-dump-id");
const std::string StaticStrings::kDumpShardId("x-serene-dump-shard-id");

// replication headers
const std::string StaticStrings::kReplicationHeaderCheckMore(
  "x-serene-replication-checkmore");
const std::string StaticStrings::kReplicationHeaderLastIncluded(
  "x-serene-replication-lastincluded");
const std::string StaticStrings::kReplicationHeaderLastScanned(
  "x-serene-replication-lastscanned");
const std::string StaticStrings::kReplicationHeaderLastTick(
  "x-serene-replication-lasttick");
const std::string StaticStrings::kReplicationHeaderFromPresent(
  "x-serene-replication-frompresent");

// Database definition field
const std::string StaticStrings::kDatabaseName("name");
const std::string StaticStrings::kDatabaseId("id");
const std::string StaticStrings::kProperties("properties");

const std::string StaticStrings::kDataSourceId("id");
const std::string StaticStrings::kDataSourceName("name");
const std::string StaticStrings::kDataSourcePlanId("planId");
const std::string StaticStrings::kDataSourceParameters("parameters");

// Index definition fields
const std::string StaticStrings::kIndexDeduplicate("deduplicate");
const std::string StaticStrings::kIndexExpireAfter("expireAfter");
const std::string StaticStrings::kIndexFields("fields");
const std::string StaticStrings::kIndexId("id");
const std::string StaticStrings::kIndexInBackground("inBackground");
const std::string StaticStrings::kIndexName("name");
const std::string StaticStrings::kIndexSparse("sparse");
const std::string StaticStrings::kIndexStoredValues("storedValues");
const std::string StaticStrings::kIndexType("type");
const std::string StaticStrings::kIndexUnique("unique");
const std::string StaticStrings::kIndexEstimates("estimates");

// static index names
const std::string StaticStrings::kIndexNameEdge("edge");
const std::string StaticStrings::kIndexNameEdgeFrom("edge_from");
const std::string StaticStrings::kIndexNameEdgeTo("edge_to");
const std::string StaticStrings::kIndexNamePrimary("primary");

// index hint strings
const std::string StaticStrings::kIndexHintDisableIndex("disableIndex");
const std::string StaticStrings::kIndexHintOption("indexHint");
const std::string StaticStrings::kIndexHintOptionForce("forceIndexHint");

// query options
const std::string StaticStrings::kFilter("filter");
const std::string StaticStrings::kMaxProjections("maxProjections");
const std::string StaticStrings::kProducesResult("producesResult");
const std::string StaticStrings::kReadOwnWrites("readOwnWrites");
const std::string StaticStrings::kParallelism("parallelism");
const std::string StaticStrings::kForceColocatedAttributeValue(
  "forceColocatedAttributeValue");
const std::string StaticStrings::kJoinStrategyType("joinStrategyType");

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
const std::string StaticStrings::kBatchContentType(
  "application/x-serene-batchpart");
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
const std::string StaticStrings::kEtag("etag");
const std::string StaticStrings::kExpect("expect");
const std::string StaticStrings::kExposedCorsHeaders(
  "etag, content-encoding, content-length, location, server, "
  "x-serene-errors, x-serene-async-id");
const std::string StaticStrings::kHlcHeader("x-serene-hlc");
const std::string StaticStrings::kLocation("location");
const std::string StaticStrings::kLockLocation("lockLocation");
const std::string StaticStrings::kNoSniff("nosniff");
const std::string StaticStrings::kOrigin("origin");
const std::string StaticStrings::kPotentialDirtyRead(
  "x-serene-potential-dirty-read");
const std::string StaticStrings::kRequestForwardedTo(
  "x-serene-request-forwarded-to");
const std::string StaticStrings::kServer("server");
const std::string StaticStrings::kTransferEncoding("transfer-encoding");
const std::string StaticStrings::kTransactionBody("x-serene-trx-body");
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
const std::string StaticStrings::kMultiPartContentType("multipart/form-data");

// accept-encodings
const std::string StaticStrings::kEncodingSereneLz4("x-serene-lz4");
const std::string StaticStrings::kEncodingDeflate("deflate");
const std::string StaticStrings::kEncodingGzip("gzip");
const std::string StaticStrings::kEncodingLz4("lz4");

const std::string StaticStrings::kBody("body");
const std::string StaticStrings::kParsedBody("parsedBody");

// collection attributes
const std::string StaticStrings::kAllowUserKeys("allowUserKeys");
const std::string StaticStrings::kIndexes("indexes");
const std::string StaticStrings::kKeyOptions("keyOptions");
const std::string StaticStrings::kNumberOfShards("numberOfShards");
const std::string StaticStrings::kObjectId("objectId");
const std::string StaticStrings::kReplicationFactor("replicationFactor");
const std::string StaticStrings::kShardingStrategy("shardingStrategy");
const std::string StaticStrings::kSchema("schema");
const std::string StaticStrings::kWriteConcern("writeConcern");

// Graph directions
const std::string StaticStrings::kGraphDirection("direction");

// Query Strings
const std::string StaticStrings::kQuerySortAsc("ASC");
const std::string StaticStrings::kQuerySortDesc("DESC");

// Graph Query Strings
const std::string StaticStrings::kGraphQueryEdges("edges");
const std::string StaticStrings::kGraphQueryVertices("vertices");
const std::string StaticStrings::kGraphQueryPath("path");
const std::string StaticStrings::kGraphQueryGlobal("global");
const std::string StaticStrings::kGraphQueryNone("none");
const std::string StaticStrings::kGraphQueryWeight("weight");
const std::string StaticStrings::kGraphQueryWeights("weights");
const std::string StaticStrings::kGraphQueryOrder("order");
const std::string StaticStrings::kGraphQueryOrderBfs("bfs");
const std::string StaticStrings::kGraphQueryOrderDfs("dfs");
const std::string StaticStrings::kGraphQueryOrderWeighted("weighted");
const std::string StaticStrings::kGraphQueryShortestPathType(
  "shortestPathType");

// Replication
const std::string StaticStrings::kReplicationSoftLockOnly("doSoftLockOnly");
const std::string StaticStrings::kFailoverCandidates("failoverCandidates");
const std::string StaticStrings::kRevisionTreeCount("count");
const std::string StaticStrings::kRevisionTreeHash("hash");
const std::string StaticStrings::kRevisionTreeMaxDepth("maxDepth");
const std::string StaticStrings::kRevisionTreeNodes("nodes");
const std::string StaticStrings::kRevisionTreeRangeMax("rangeMax");
const std::string StaticStrings::kRevisionTreeRangeMin("rangeMin");
const std::string StaticStrings::kRevisionTreeInitialRangeMin(
  "initialRangeMin");
const std::string StaticStrings::kRevisionTreeRanges("ranges");
const std::string StaticStrings::kRevisionTreeResume("resume");
const std::string StaticStrings::kRevisionTreeVersion("version");
const std::string StaticStrings::kFollowingTermId("followingTermId");

// misc strings
const std::string StaticStrings::kLastValue("lastValue");

const std::string StaticStrings::kRebootId("rebootId");

const std::string StaticStrings::kNew("new");
const std::string StaticStrings::kOld("old");
const std::string StaticStrings::kUpgradeEnvName(
  "SERENEDB_UPGRADE_DURING_RESTORE");
const std::string StaticStrings::kBackupToDeleteName("DIRECTORY_TO_DELETE");
const std::string StaticStrings::kBackupSearchToDeleteName(
  "DIRECTORY_TO_DELETE_SEARCH");

// aql api strings
const std::string StaticStrings::kAqlDocumentCall("x-serene-aql-document-aql");
const std::string StaticStrings::kAqlFastPath("x-serene-fast-path");
const std::string StaticStrings::kAqlRemoteExecute("execute");
const std::string StaticStrings::kAqlRemoteCallStack("callStack");
const std::string StaticStrings::kAqlRemoteInfinity("infinity");
const std::string StaticStrings::kAqlRemoteResult("result");
const std::string StaticStrings::kAqlRemoteBlock("block");
const std::string StaticStrings::kAqlRemoteSkipped("skipped");
const std::string StaticStrings::kAqlRemoteState("state");
const std::string StaticStrings::kAqlRemoteStateDone("done");
const std::string StaticStrings::kAqlRemoteStateHasmore("hasmore");

// aql http headers
const std::string StaticStrings::kAqlShardIdHeader("x-shard-id");
