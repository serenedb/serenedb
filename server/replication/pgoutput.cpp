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

#include <stdexcept>

namespace sdb::replication {
namespace {

// Big-endian cursor over the message payload; every getter that runs off the
// end throws, so a malformed stream fails the apply loop rather than reading
// garbage.
class Cursor {
 public:
  explicit Cursor(std::string_view buf) : _buf(buf) {}

  uint8_t Byte() {
    Need(1);
    return static_cast<uint8_t>(_buf[_pos++]);
  }
  uint16_t Int16() {
    Need(2);
    uint16_t v = (static_cast<uint16_t>(u8(0)) << 8) | u8(1);
    _pos += 2;
    return v;
  }
  uint32_t Int32() {
    Need(4);
    uint32_t v = (static_cast<uint32_t>(u8(0)) << 24) |
                 (static_cast<uint32_t>(u8(1)) << 16) |
                 (static_cast<uint32_t>(u8(2)) << 8) | u8(3);
    _pos += 4;
    return v;
  }
  uint64_t Int64() {
    uint64_t hi = Int32();
    uint64_t lo = Int32();
    return (hi << 32) | lo;
  }
  std::string String() {
    auto nul = _buf.find('\0', _pos);
    if (nul == std::string_view::npos) {
      throw std::runtime_error("pgoutput: unterminated string");
    }
    std::string s{_buf.substr(_pos, nul - _pos)};
    _pos = nul + 1;
    return s;
  }
  void Skip(size_t n) {
    Need(n);
    _pos += n;
  }
  size_t Pos() const noexcept { return _pos; }
  std::string_view From(size_t start) const {
    return _buf.substr(start, _pos - start);
  }

 private:
  uint8_t u8(size_t off) const {
    return static_cast<uint8_t>(_buf[_pos + off]);
  }
  void Need(size_t n) const {
    if (_pos + n > _buf.size()) {
      throw std::runtime_error("pgoutput: truncated message");
    }
  }
  std::string_view _buf;
  size_t _pos = 0;
};

// Advance the cursor past one tuple (int16 ncols, then per-column kind + data)
// and return the whole tuple region (including the ncols prefix) as a view.
std::string_view SkipTuple(Cursor& c) {
  const size_t start = c.Pos();
  const uint16_t ncols = c.Int16();
  for (uint16_t i = 0; i < ncols; ++i) {
    const char kind = static_cast<char>(c.Byte());
    if (kind == 't' || kind == 'b') {
      const uint32_t len = c.Int32();
      c.Skip(len);
    } else if (kind != 'n' && kind != 'u') {
      throw std::runtime_error("pgoutput: bad tuple column kind");
    }
  }
  return c.From(start);
}

RelationMessage ReadRelation(Cursor& c) {
  RelationMessage m;
  m.relation_id = c.Int32();
  m.namespace_name = c.String();
  m.relation_name = c.String();
  m.replica_identity = static_cast<char>(c.Byte());
  const uint16_t natts = c.Int16();
  m.columns.reserve(natts);
  for (uint16_t i = 0; i < natts; ++i) {
    RelationColumn col;
    col.is_key = (c.Byte() & 1) != 0;
    col.name = c.String();
    col.type_oid = c.Int32();
    col.type_modifier = static_cast<int32_t>(c.Int32());
    m.columns.push_back(std::move(col));
  }
  return m;
}

}  // namespace

PgTupleReader::PgTupleReader(std::string_view tuple) : _buf(tuple) {
  if (_buf.size() < 2) {
    throw std::runtime_error("pgoutput: truncated tuple");
  }
  _count = (static_cast<uint16_t>(static_cast<uint8_t>(_buf[0])) << 8) |
           static_cast<uint8_t>(_buf[1]);
  _pos = 2;
}

PgColumn PgTupleReader::Next() {
  if (_index >= _count) {
    throw std::runtime_error("pgoutput: tuple column overrun");
  }
  if (_pos + 1 > _buf.size()) {
    throw std::runtime_error("pgoutput: truncated tuple column");
  }
  PgColumn col;
  col.kind = static_cast<TupleColKind>(_buf[_pos++]);
  switch (col.kind) {
    case TupleColKind::Null:
    case TupleColKind::Unchanged:
      break;
    case TupleColKind::Text:
    case TupleColKind::Binary: {
      if (_pos + 4 > _buf.size()) {
        throw std::runtime_error("pgoutput: truncated column length");
      }
      const uint32_t len =
        (static_cast<uint32_t>(static_cast<uint8_t>(_buf[_pos])) << 24) |
        (static_cast<uint32_t>(static_cast<uint8_t>(_buf[_pos + 1])) << 16) |
        (static_cast<uint32_t>(static_cast<uint8_t>(_buf[_pos + 2])) << 8) |
        static_cast<uint8_t>(_buf[_pos + 3]);
      _pos += 4;
      if (_pos + len > _buf.size()) {
        throw std::runtime_error("pgoutput: truncated column data");
      }
      col.data = _buf.substr(_pos, len);
      _pos += len;
      break;
    }
    default:
      throw std::runtime_error("pgoutput: bad tuple column kind");
  }
  ++_index;
  return col;
}

PgOutputMessage DecodePgOutput(std::string_view payload) {
  if (payload.empty()) {
    throw std::runtime_error("pgoutput: empty message");
  }
  Cursor c{payload};
  const char tag = static_cast<char>(c.Byte());
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
      c.Byte();  // flags (unused)
      m.commit_lsn = c.Int64();
      m.end_lsn = c.Int64();
      m.commit_time = static_cast<int64_t>(c.Int64());
      return m;
    }
    case 'R':
      return ReadRelation(c);
    case 'I': {
      InsertMessage m;
      m.relation_id = c.Int32();
      if (static_cast<char>(c.Byte()) != 'N') {
        throw std::runtime_error("pgoutput: insert missing new tuple");
      }
      m.new_tuple = SkipTuple(c);
      return m;
    }
    case 'U': {
      UpdateMessage m;
      m.relation_id = c.Int32();
      char sub = static_cast<char>(c.Byte());
      if (sub == 'K' || sub == 'O') {
        m.has_old = true;
        m.old_is_key = (sub == 'K');
        m.old_tuple = SkipTuple(c);
        sub = static_cast<char>(c.Byte());
      }
      if (sub != 'N') {
        throw std::runtime_error("pgoutput: update missing new tuple");
      }
      m.new_tuple = SkipTuple(c);
      return m;
    }
    case 'D': {
      DeleteMessage m;
      m.relation_id = c.Int32();
      const char sub = static_cast<char>(c.Byte());
      if (sub != 'K' && sub != 'O') {
        throw std::runtime_error("pgoutput: delete missing old tuple");
      }
      m.old_is_key = (sub == 'K');
      m.old_tuple = SkipTuple(c);
      return m;
    }
    case 'T': {
      TruncateMessage m;
      const uint32_t nrelids = c.Int32();
      const uint8_t flags = c.Byte();
      m.cascade = (flags & 1) != 0;
      m.restart_identity = (flags & 2) != 0;
      m.relation_ids.reserve(nrelids);
      for (uint32_t i = 0; i < nrelids; ++i) {
        m.relation_ids.push_back(c.Int32());
      }
      return m;
    }
    case 'Y': {
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
    case 'M':
      return IgnoredMessage{tag};
    default:
      throw std::runtime_error(std::string("pgoutput: unsupported message '") +
                               tag + "'");
  }
}

}  // namespace sdb::replication
