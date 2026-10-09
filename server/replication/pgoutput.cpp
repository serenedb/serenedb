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

#include "replication/pgoutput.h"

#include <absl/base/internal/endian.h>

#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

namespace sdb::replication {
namespace {

[[noreturn]] void Malformed(std::string_view what) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                  ERR_MSG("invalid logical replication message: ", what));
}

class Cursor {
 public:
  explicit Cursor(std::string_view buf) : _buf{buf} {}

  uint8_t Byte() { return static_cast<uint8_t>(*Take(1)); }
  uint16_t Int16() { return absl::big_endian::Load16(Take(2)); }
  uint32_t Int32() { return absl::big_endian::Load32(Take(4)); }
  uint64_t Int64() { return absl::big_endian::Load64(Take(8)); }

  std::string String() {
    const auto end = _buf.find('\0', _pos);
    if (end == std::string_view::npos) {
      Malformed("unterminated string");
    }
    std::string result{_buf.substr(_pos, end - _pos)};
    _pos = end + 1;
    return result;
  }

  std::string_view Tuple() {
    const auto start = _pos;
    const auto columns = Int16();
    for (uint16_t i = 0; i < columns; ++i) {
      switch (static_cast<TupleColKind>(Byte())) {
        case TupleColKind::Text:
        case TupleColKind::Binary:
          Take(Int32());
          break;
        case TupleColKind::Null:
        case TupleColKind::Unchanged:
          break;
        default:
          Malformed("bad tuple column kind");
      }
    }
    return _buf.substr(start, _pos - start);
  }

 private:
  const char* Take(size_t size) {
    if (_buf.size() - _pos < size) {
      Malformed("truncated message");
    }
    const auto* data = _buf.data() + _pos;
    _pos += size;
    return data;
  }

  std::string_view _buf;
  size_t _pos = 0;
};

RelationMessage ReadRelation(Cursor& c) {
  RelationMessage m;
  m.relation_id = c.Int32();
  m.namespace_name = c.String();
  m.relation_name = c.String();
  m.replica_identity = static_cast<char>(c.Byte());
  const auto columns = c.Int16();
  m.columns.reserve(columns);
  for (uint16_t i = 0; i < columns; ++i) {
    auto& column = m.columns.emplace_back();
    column.is_key = (c.Byte() & 1) != 0;
    column.name = c.String();
    column.type_oid = c.Int32();
    column.type_modifier = static_cast<int32_t>(c.Int32());
  }
  return m;
}

}  // namespace

PgTupleReader::PgTupleReader(std::string_view tuple) : _buf{tuple} {
  if (_buf.size() < 2) {
    Malformed("truncated tuple");
  }
  _count = absl::big_endian::Load16(_buf.data());
  _pos = 2;
}

PgColumn PgTupleReader::Next() {
  if (_index == _count || _pos == _buf.size()) {
    Malformed("truncated tuple");
  }
  PgColumn column;
  column.kind = static_cast<TupleColKind>(_buf[_pos++]);
  if (column.kind == TupleColKind::Text ||
      column.kind == TupleColKind::Binary) {
    if (_buf.size() - _pos < 4) {
      Malformed("truncated tuple");
    }
    const auto size = absl::big_endian::Load32(_buf.data() + _pos);
    _pos += 4;
    if (_buf.size() - _pos < size) {
      Malformed("truncated tuple");
    }
    column.data = _buf.substr(_pos, size);
    _pos += size;
  } else if (column.kind != TupleColKind::Null &&
             column.kind != TupleColKind::Unchanged) {
    Malformed("bad tuple column kind");
  }
  ++_index;
  return column;
}

uint32_t StreamedMessageXid(std::string_view payload) {
  if (payload.size() < 5) {
    Malformed("truncated message");
  }
  return absl::big_endian::Load32(payload.data() + 1);
}

PgOutputMessage DecodePgOutput(std::string_view payload, bool in_stream) {
  if (payload.empty()) {
    Malformed("empty message");
  }
  Cursor c{payload};
  const auto tag = static_cast<char>(c.Byte());
  const auto streamed_xid = [&] {
    if (in_stream) {
      c.Int32();
    }
  };
  switch (tag) {
    case 'B': {
      BeginMessage m;
      m.final_lsn = c.Int64();
      m.commit_time = static_cast<int64_t>(c.Int64());
      m.xid = c.Int32();
      return m;
    }
    case 'C': {
      CommitMessage m;
      c.Byte();
      m.commit_lsn = c.Int64();
      m.end_lsn = c.Int64();
      m.commit_time = static_cast<int64_t>(c.Int64());
      return m;
    }
    case 'R':
      streamed_xid();
      return ReadRelation(c);
    case 'I': {
      streamed_xid();
      InsertMessage m;
      m.relation_id = c.Int32();
      if (c.Byte() != 'N') {
        Malformed("insert without a new tuple");
      }
      m.new_tuple = c.Tuple();
      return m;
    }
    case 'U': {
      streamed_xid();
      UpdateMessage m;
      m.relation_id = c.Int32();
      auto kind = c.Byte();
      if (kind == 'K' || kind == 'O') {
        m.has_old = true;
        m.old_is_key = kind == 'K';
        m.old_tuple = c.Tuple();
        kind = c.Byte();
      }
      if (kind != 'N') {
        Malformed("update without a new tuple");
      }
      m.new_tuple = c.Tuple();
      return m;
    }
    case 'D': {
      streamed_xid();
      DeleteMessage m;
      m.relation_id = c.Int32();
      const auto kind = c.Byte();
      if (kind != 'K' && kind != 'O') {
        Malformed("delete without an old tuple");
      }
      m.old_is_key = kind == 'K';
      m.old_tuple = c.Tuple();
      return m;
    }
    case 'T': {
      streamed_xid();
      TruncateMessage m;
      const auto relations = c.Int32();
      const auto flags = c.Byte();
      m.cascade = (flags & 1) != 0;
      m.restart_identity = (flags & 2) != 0;
      m.relation_ids.reserve(relations);
      for (uint32_t i = 0; i < relations; ++i) {
        m.relation_ids.push_back(c.Int32());
      }
      return m;
    }
    case 'Y': {
      streamed_xid();
      TypeMessage m;
      m.type_oid = c.Int32();
      m.namespace_name = c.String();
      m.type_name = c.String();
      return m;
    }
    case 'O': {
      OriginMessage m;
      m.origin_lsn = c.Int64();
      m.origin_name = c.String();
      return m;
    }
    case 'M': {
      streamed_xid();
      return LogicalMessage{.transactional = (c.Byte() & 1) != 0};
    }
    case 'S':
      return StreamStartMessage{.xid = c.Int32(),
                                .first_segment = c.Byte() == 1};
    case 'E':
      return StreamStopMessage{};
    case 'c': {
      StreamCommitMessage m;
      m.xid = c.Int32();
      c.Byte();
      m.commit_lsn = c.Int64();
      m.end_lsn = c.Int64();
      m.commit_time = static_cast<int64_t>(c.Int64());
      return m;
    }
    case 'A': {
      StreamAbortMessage m;
      m.xid = c.Int32();
      m.subxid = c.Int32();
      return m;
    }
    default:
      Malformed(std::string{"unsupported message type \""} + tag + "\"");
  }
}

}  // namespace sdb::replication
