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

#include "iresearch/index/merge/term_keys.hpp"

#include <algorithm>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <mutex>

#include "iresearch/formats/term_reader.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

class TermKeyReader final : public duckdb::PresortedMergeReader {
 public:
  explicit TermKeyReader(const TermKeys& keys)
    : _keys{keys}, _it{keys.Reader().iterator()} {}

  void Read(duckdb::idx_t begin, duckdb::idx_t count,
            duckdb::DataChunk& keys) final {
    MoveTo(begin);
    auto writer =
      duckdb::FlatVector::Writer<duckdb::string_t>(keys.data[0], count);
    for (duckdb::idx_t i = 0; i != count; ++i) {
      if (i != 0) {
        Step();
      }
      const auto term = _it->value();
      writer.WriteValue(
        duckdb::string_t{reinterpret_cast<const char*>(term.data()),
                         static_cast<uint32_t>(term.size())});
    }
    keys.SetCardinality(count);
  }

 private:
  void Step() {
    const bool next = _it->next();
    SDB_ENSURE(next, "TermKeys: a term dictionary of ", _keys.Count(),
               " terms ends after ", _next);
    if (_next % TermKeys::kAnchorStep == 0) {
      _keys.Record(_next, _it->value());
    }
    ++_next;
  }

  void MoveTo(uint64_t row) {
    const auto want = row + 1;
    if (want < _next || want - _next > TermKeys::kAnchorStep) {
      const auto anchor = _keys.Anchor(row);
      if (anchor.row != TermKeys::kNoAnchor &&
          (want < _next || anchor.row >= _next)) {
        _it = _keys.Reader().iterator();
        const bool found = _it->seek(anchor.term);
        SDB_ENSURE(found, "TermKeys: a term dictionary lost the term of row ",
                   anchor.row);
        _next = anchor.row + 1;
      } else if (want < _next) {
        _it = _keys.Reader().iterator();
        _next = 0;
      }
    }
    while (_next < want) {
      Step();
    }
  }

  const TermKeys& _keys;
  SeekTermIterator::ptr _it;
  uint64_t _next = 0;
};

}  // namespace

duckdb::idx_t TermKeys::Count() const { return _reader.size(); }

duckdb::unique_ptr<duckdb::PresortedMergeReader> TermKeys::CreateReader()
  const {
  return duckdb::make_uniq<TermKeyReader>(*this);
}

TermKeys::TermAnchor TermKeys::Anchor(uint64_t row) const {
  std::lock_guard lock{_mutex};
  if (_anchors.empty()) {
    return {};
  }
  const auto k = std::min<uint64_t>(row / kAnchorStep, _anchors.size() - 1);
  return {.row = k * kAnchorStep, .term = _anchors[k]};
}

void TermKeys::Record(uint64_t row, bytes_view term) const {
  std::lock_guard lock{_mutex};
  if (row / kAnchorStep == _anchors.size()) {
    _anchors.emplace_back(term);
  }
}

void TermKeys::SeekBefore(SeekTermIterator& it, uint64_t row) const {
  if (row == 0) {
    return;
  }
  uint64_t next = 0;
  if (const auto anchor = Anchor(row - 1); anchor.row != kNoAnchor) {
    const bool found = it.seek(anchor.term);
    SDB_ENSURE(found, "TermKeys: a term dictionary lost the term of row ",
               anchor.row);
    next = anchor.row + 1;
  }
  for (; next < row; ++next) {
    const bool more = it.next();
    SDB_ENSURE(more, "TermKeys: a term dictionary of ", Count(),
               " terms ends after ", next);
  }
}

}  // namespace irs
