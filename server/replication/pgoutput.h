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

// Decoder for the PostgreSQL logical replication ("pgoutput") wire format,
// protocol version 1 -- the payload of a 'w' (XLogData) CopyData frame. Matches
// src/backend/replication/logical/proto.c in PostgreSQL 18. Tuples are exposed
// as zero-copy views into the frame buffer (PgTupleReader), never materialised
// into owning per-column strings; the apply path parses them straight into the
// destination duckdb::Vectors.

// Per-column marker in a tuple (also the on-wire byte).
enum class TupleColKind : char {
  Null = 'n',       // SQL NULL
  Unchanged = 'u',  // TOASTed value that did not change; no bytes on the wire
  Text = 't',       // publisher's text output for the column type
  Binary = 'b',     // publisher's binary (send-function) output
};

// One column yielded by PgTupleReader; `data` views into the frame buffer and
// is valid only while that frame is alive (apply before consuming the frame).
struct PgColumn {
  TupleColKind kind = TupleColKind::Null;
  std::string_view data;  // set for Text/Binary; empty otherwise
};

// Sequential, allocation-free reader over a pgoutput tuple -- the bytes
// starting at the int16 column count. Yields each column as a view.
class PgTupleReader {
 public:
  explicit PgTupleReader(std::string_view tuple);

  uint16_t Count() const noexcept { return _count; }
  bool HasNext() const noexcept { return _index < _count; }
  // Reads the next column (throws std::runtime_error if the buffer is
  // truncated or a kind byte is invalid).
  PgColumn Next();

 private:
  std::string_view _buf;
  size_t _pos = 0;
  uint16_t _count = 0;
  uint16_t _index = 0;
};

struct BeginMessage {
  uint64_t final_lsn = 0;
  int64_t commit_time = 0;  // usec since 2000-01-01
  uint32_t xid = 0;
};

struct CommitMessage {
  uint64_t commit_lsn = 0;
  uint64_t end_lsn = 0;
  int64_t commit_time = 0;
};

struct RelationColumn {
  bool is_key = false;  // part of the replica-identity key
  std::string name;
  uint32_t type_oid = 0;
  int32_t type_modifier = -1;
};

struct RelationMessage {
  uint32_t relation_id = 0;
  std::string namespace_name;  // empty on the wire means "pg_catalog"
  std::string relation_name;
  char replica_identity = 'd';
  std::vector<RelationColumn> columns;
};

// For Insert/Update/Delete, the tuple bytes are views into the frame; parse
// them with PgTupleReader. The columns are in the relation's column order.
struct InsertMessage {
  uint32_t relation_id = 0;
  std::string_view new_tuple;
};

struct UpdateMessage {
  uint32_t relation_id = 0;
  bool has_old = false;  // an old tuple (key or full row) preceded the new one
  bool old_is_key =
    false;  // K (key) vs O (full old row, REPLICA IDENTITY FULL)
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

// A message this subscriber recognises but does not act on.
struct IgnoredMessage {
  char tag = 0;
};

using PgOutputMessage =
  std::variant<BeginMessage, CommitMessage, RelationMessage, InsertMessage,
               UpdateMessage, DeleteMessage, TruncateMessage, TypeMessage,
               OriginMessage, IgnoredMessage>;

// Decode one pgoutput message. Tuple views in the returned message alias
// `payload`, so the message must be consumed before `payload`'s frame is freed.
// Throws std::runtime_error on a truncated/malformed buffer or a streaming/
// two-phase tag we never negotiate.
PgOutputMessage DecodePgOutput(std::string_view payload);

}  // namespace sdb::replication
