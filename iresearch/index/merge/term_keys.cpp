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

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/vector/flat_vector.hpp>

#include "iresearch/utils/log.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

uint64_t SeekAnchor(SeekTermIterator& it, const TermKeys::TermAnchor& anchor) {
  const bool found = it.seek(anchor.term);
  SDB_ENSURE(found, "TermKeys: a term dictionary lost the term of row ",
             anchor.row);
  return anchor.row + 1;
}

void NextTerm(SeekTermIterator& it, const TermKeys& keys, uint64_t row) {
  const bool more = it.next();
  SDB_ENSURE(more, "TermKeys: a term dictionary of ", keys.Count(),
             " terms ends after ", row);
}

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
  }

 private:
  void Step() {
    NextTerm(*_it, _keys, _next);
    if (_next % TermKeys::kAnchorStep == 0) {
      _keys.Record(_next, _it->value());
    }
    ++_next;
  }

  void MoveTo(uint64_t row) {
    const auto want = row + 1;
    if (want < _next || want - _next > TermKeys::kAnchorStep || _next == 0) {
      const auto anchor = _keys.Anchor(row);
      if (anchor.row != TermKeys::kNoAnchor &&
          (want < _next || anchor.row >= _next)) {
        _it = _keys.Reader().iterator();
        _next = SeekAnchor(*_it, anchor);
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

duckdb::unique_ptr<duckdb::PresortedMergeReader> TermKeys::CreateReader()
  const {
  return duckdb::make_uniq<TermKeyReader>(*this);
}

TermKeys::TermAnchor TermKeys::Anchor(uint64_t row) const {
  absl::MutexLock lock{&_mutex};
  auto it = _anchors.upper_bound(row);
  if (it == _anchors.begin()) {
    return {};
  }
  --it;
  return {.row = it->first, .term = it->second};
}

void TermKeys::Record(uint64_t row, bytes_view term) const {
  absl::MutexLock lock{&_mutex};
  _anchors.try_emplace(row, term);
}

void TermKeys::AddAnchor(uint64_t row) const {
  auto it = _reader.iterator();
  SeekBefore(*it, row + 1);
  Record(row, it->value());
}

void TermKeys::SeekBefore(SeekTermIterator& it, uint64_t row) const {
  if (row == 0) {
    return;
  }
  uint64_t next = 0;
  if (const auto anchor = Anchor(row - 1); anchor.row != kNoAnchor) {
    next = SeekAnchor(it, anchor);
  }
  for (; next < row; ++next) {
    NextTerm(it, *this, next);
  }
}

bool NextTermRuns(const duckdb::PresortedMerge& merge,
                  duckdb::PresortedMergeState& state,
                  duckdb::vector<duckdb::PresortedRun>& runs,
                  const MergeProgress& progress, field_id field) {
  runs.clear();
  duckdb::optional_idx unsorted;
  if (merge.Merge(state, runs, STANDARD_VECTOR_SIZE, progress, unsorted)) {
    return true;
  }
  if (unsorted.IsValid()) {
    SDB_ERROR(IRESEARCH, "Terms of field ", field, " in merge input ",
              unsorted.GetIndex(), " are not sorted");
  }
  return false;
}

}  // namespace irs
