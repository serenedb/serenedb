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

#include <vpack/builder.h>
#include <vpack/iterator.h>
#include <vpack/vpack_helper.h>

#include "basics/result_or.h"
#include "general_server/state.h"

namespace sdb {

inline constexpr std::string_view kBadParamsCreate =
  "backup payload must be an object defining optional string attribute 'label' "
  "and/or optional floating point parameter 'timeout' in seconds";

struct BackupMeta {
  std::string id;
  std::string version;
  std::string datetime;
  std::string server_id;
  std::vector<std::string> user_secret_hashes;  // might be empty
  uint64_t size_in_bytes;
  uint64_t nr_files;
  uint32_t nr_db_servers;
  uint32_t nr_pieces_present = 1;
  bool potentially_inconsistent;
  bool is_available = true;
  bool count_includes_files_only = true;

  static constexpr std::string_view kId = "id";
  static constexpr std::string_view kVersion = "version";
  static constexpr std::string_view kDatetime = "datetime";
  static constexpr std::string_view kSecretHash = "keys";
  static constexpr std::string_view kSizeInBytes = "sizeInBytes";
  static constexpr std::string_view kNrFiles = "nrFiles";
  static constexpr std::string_view kNrDBServers = "nrDBServers";
  static constexpr std::string_view kServerId = "serverId";
  static constexpr std::string_view kPotentiallyInconsistent =
    "potentiallyInconsistent";
  static constexpr std::string_view kAvailable = "available";
  static constexpr std::string_view kNrPiecesPresent = "nrPiecesPresent";
  static constexpr std::string_view kCountIncludesFilesOnly =
    "countIncludesFilesOnly";

  void toVPack(vpack::Builder& builder) const {
    vpack::ObjectBuilder ob(&builder);
    builder.add(kId, id);
    builder.add(kVersion, version);
    builder.add(kDatetime, datetime);
    builder.add(kSecretHash, vpack::Value(vpack::ValueType::Array, true));
    for (const auto& tmp : user_secret_hashes) {
      builder.openObject(/*unindexed*/ true);
      builder.add("sha256", tmp);
      builder.close();
    }
    builder.close();
    builder.add(kSizeInBytes, size_in_bytes);
    builder.add(kNrFiles, nr_files);
    builder.add(kNrDBServers, nr_db_servers);
    if (ServerState::instance()->IsDBServer()) {
      builder.add(kServerId, server_id);
    }
    if (ServerState::instance()->IsCoordinator() ||
        ServerState::instance()->IsSingle()) {
      builder.add(kAvailable, is_available);
      builder.add(kNrPiecesPresent, nr_pieces_present);
    }
    builder.add(kPotentiallyInconsistent, potentially_inconsistent);
    builder.add(kCountIncludesFilesOnly, count_includes_files_only);
  }

  static ResultOr<BackupMeta> fromSlice(vpack::Slice slice) try {
    BackupMeta meta;
    meta.id = slice.get(kId).stringView();
    meta.version = slice.get(kVersion).stringView();
    meta.datetime = slice.get(kDatetime).stringView();
    auto hashes = slice.get(kSecretHash);
    if (hashes.isArray()) {
      for (auto v : vpack::ArrayIterator{hashes}) {
        if (vpack::Slice h; v.isObject() && (h = v.get("sha256")).isString()) {
          meta.user_secret_hashes.emplace_back(h.stringView());
        }
      }
    }
    using basics::VPackHelper;
    meta.server_id = VPackHelper::getString(slice, kServerId, {});
    meta.size_in_bytes =
      VPackHelper::getNumber<uint64_t>(slice, kSizeInBytes, 0);
    meta.nr_files = VPackHelper::getNumber<uint64_t>(slice, kNrFiles, 0);
    meta.nr_db_servers =
      VPackHelper::getNumber<uint32_t>(slice, kNrDBServers, 1);
    meta.nr_pieces_present =
      VPackHelper::getNumber<uint32_t>(slice, kNrPiecesPresent, 1);
    meta.potentially_inconsistent =
      VPackHelper::getBool(slice, kPotentiallyInconsistent, false);
    meta.is_available = VPackHelper::getBool(slice, kAvailable, true);
    meta.count_includes_files_only =
      VPackHelper::getBool(slice, kCountIncludesFilesOnly, false);
    return meta;
  } catch (const std::exception& e) {
    return std::unexpected<Result>(std::in_place, ERROR_BAD_PARAMETER,
                                   e.what());
  }

  BackupMeta(std::string_view id, std::string_view version,
             std::string_view datetime, std::span<const std::string> hashes,
             uint64_t size_in_bytes, uint64_t nr_files, uint32_t nr_db_servers,
             std::string_view server_id, bool potentially_inconsistent)
    : id{id},
      version{version},
      datetime{datetime},
      server_id{server_id},
      user_secret_hashes{hashes.begin(), hashes.end()},
      size_in_bytes{size_in_bytes},
      nr_files{nr_files},
      nr_db_servers{nr_db_servers},
      potentially_inconsistent{potentially_inconsistent} {}

 private:
  BackupMeta() = default;
};

}  // namespace sdb
