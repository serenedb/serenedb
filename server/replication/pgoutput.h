////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace sdb::replication {

enum class TupleColKind : char {
  Null = 'n',
  Unchanged = 'u',
  Text = 't',
  Binary = 'b',
};

struct PgColumn {
  TupleColKind kind = TupleColKind::Null;
  std::string_view data;
};

class PgTupleReader {
 public:
  explicit PgTupleReader(std::string_view tuple);

  uint16_t Count() const noexcept { return _count; }
  bool HasNext() const noexcept { return _index < _count; }
  PgColumn Next();

 private:
  std::string_view _buf;
  size_t _pos = 0;
  uint16_t _count = 0;
  uint16_t _index = 0;
};

struct BeginMessage {
  uint64_t final_lsn = 0;
  int64_t commit_time = 0;
  uint32_t xid = 0;
};

struct CommitMessage {
  uint64_t commit_lsn = 0;
  uint64_t end_lsn = 0;
  int64_t commit_time = 0;
};

struct RelationColumn {
  bool is_key = false;
  std::string name;
  uint32_t type_oid = 0;
  int32_t type_modifier = -1;
};

struct RelationMessage {
  uint32_t relation_id = 0;
  std::string namespace_name;
  std::string relation_name;
  char replica_identity = 'd';
  std::vector<RelationColumn> columns;
};

struct InsertMessage {
  uint32_t relation_id = 0;
  std::string_view new_tuple;
};

struct UpdateMessage {
  uint32_t relation_id = 0;
  bool has_old = false;
  bool old_is_key = false;
  std::string_view old_tuple;
  std::string_view new_tuple;
};

struct DeleteMessage {
  uint32_t relation_id = 0;
  bool old_is_key = false;
  std::string_view old_tuple;
};

struct TruncateMessage {
  bool cascade = false;
  bool restart_identity = false;
  std::vector<uint32_t> relation_ids;
};

struct TypeMessage {
  uint32_t type_oid = 0;
  std::string namespace_name;
  std::string type_name;
};

struct OriginMessage {
  uint64_t origin_lsn = 0;
  std::string origin_name;
};

struct LogicalMessage {
  bool transactional = false;
};

struct StreamStartMessage {
  uint32_t xid = 0;
  bool first_segment = false;
};

struct StreamStopMessage {};

struct StreamCommitMessage {
  uint32_t xid = 0;
  uint64_t commit_lsn = 0;
  uint64_t end_lsn = 0;
  int64_t commit_time = 0;
};

struct StreamAbortMessage {
  uint32_t xid = 0;
  uint32_t subxid = 0;
};

using PgOutputMessage =
  std::variant<BeginMessage, CommitMessage, RelationMessage, InsertMessage,
               UpdateMessage, DeleteMessage, TruncateMessage, TypeMessage,
               OriginMessage, LogicalMessage, StreamStartMessage,
               StreamStopMessage, StreamCommitMessage, StreamAbortMessage>;

PgOutputMessage DecodePgOutput(std::string_view payload,
                               bool in_stream = false);

uint32_t StreamedMessageXid(std::string_view payload);

}  // namespace sdb::replication
