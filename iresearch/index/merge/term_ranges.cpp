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

#include "iresearch/index/merge/term_ranges.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <span>
#include <string_view>
#include <utility>

#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/merge/merged_postings.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/math_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

constexpr std::string_view kPartName = "part";

std::vector<duckdb::shared_ptr<TermKeys>> KeysOf(
  std::span<const TermRangeMerge::Input> inputs) {
  std::vector<duckdb::shared_ptr<TermKeys>> keys;
  keys.reserve(inputs.size());
  for (const auto& input : inputs) {
    keys.emplace_back(duckdb::make_shared_ptr<TermKeys>(*input.terms));
  }
  return keys;
}

duckdb::vector<duckdb::shared_ptr<duckdb::PresortedMergeInput>> SourcesOf(
  std::span<const duckdb::shared_ptr<TermKeys>> keys) {
  return {keys.begin(), keys.end()};
}

template<typename Postings>
class RangeWriter {
 public:
  RangeWriter(std::span<const TermRangeMerge::Input> sources,
              std::span<const duckdb::shared_ptr<TermKeys>> keys,
              const FieldProperties& props,
              const TermRangeMerge::Target& target, Directory& dir,
              const MergeProgress& progress)
    : _sources{sources},
      _keys{keys},
      _inputs(sources.size()),
      _props{props},
      _postings{progress},
      _terms{props.index_features} {
    const FlushState state{
      .dir = &dir,
      .norms = target.norms,
      .name = kPartName,
      .scorer = target.scorer,
      .doc_count = target.doc_count,
      .index_features = props.index_features,
    };
    _writer.Prepare(state);
    _writer.BeginField(props);
  }

  bool Consume(std::span<const duckdb::PresortedRun> runs) {
    for (const auto& run : runs) {
      const auto i = static_cast<size_t>(run.input);
      SDB_ENSURE(i < _inputs.size(), "TermRangeWriter: run of input ", i,
                 " of ", _inputs.size());
      Position(i, run.begin);
      for (uint64_t k = 0; k != run.count; ++k) {
        Entry(i);
      }
      _inputs[i].next = run.begin + run.count;
    }
    return !_postings.Aborted();
  }

  bool Finish() {
    if (_open) {
      Close();
    }
    _writer.EndField();
    _writer.FinishPart();
    return !_postings.Aborted();
  }

  const bitset& Docs() const noexcept { return _writer.Docs(); }

  TermPartWriter& Terms() noexcept { return _terms; }

 private:
  struct InputState {
    SeekTermIterator::ptr it;
    TermPostings::ptr postings;
    const DocRemap* remap = nullptr;
    uint64_t next = 0;
    uint64_t group = 0;
  };

  void Position(size_t i, uint64_t row) {
    auto& input = _inputs[i];
    if (input.it) {
      SDB_ENSURE(input.next == row, "TermRangeWriter: input ", i,
                 " continues at row ", row, " instead of ", input.next);
      return;
    }
    input.it = _sources[i].terms->ReadOnceIterator();
    input.remap = _sources[i].remap;
    _keys[i]->SeekBefore(*input.it, row);
    input.next = row;
  }

  bytes_view Advance(size_t i) {
    auto& it = *_inputs[i].it;
    const bool next = it.next();
    SDB_ENSURE(next, "TermRangeWriter: input ", i, " ran out of terms");
    return it.value();
  }

  void Entry(size_t i) {
    if (_open) {
      if (_inputs[i].group == _groups) {
        Close();
      } else {
        const auto term = Advance(i);
        if (term == _current) {
          Join(i);
          return;
        }
        SDB_ASSERT(term > _current);
        Close();
        Open(i, term);
        return;
      }
    }
    Open(i, Advance(i));
  }

  void Open(size_t i, bytes_view term) {
    ++_groups;
    _current = term;
    _mask.clear();
    Join(i);
    _open = true;
  }

  void Join(size_t i) {
    _inputs[i].group = _groups;
    _mask.emplace_back(i);
  }

  void Close() {
    _open = false;
    if (_postings.Aborted()) {
      return;
    }
    _postings.Reset(
      [this](typename Postings::IteratorsT& itrs) {
        RefillPostings(itrs, _inputs, _mask, _props.index_features);
      },
      _props.index_features);
    PostingMeta meta;
    _writer.Write(_postings, meta);
    if (meta.docs_count != 0) {
      _terms.Add(_current, meta);
    }
  }

  std::span<const TermRangeMerge::Input> _sources;
  std::span<const duckdb::shared_ptr<TermKeys>> _keys;
  std::vector<InputState> _inputs;
  FieldProperties _props;
  PostingsWriter _writer;
  Postings _postings;
  TermPartWriter _terms;
  std::vector<size_t> _mask;
  bytes_view _current;
  uint64_t _groups = 0;
  bool _open = false;
};

}  // namespace

struct TermRangeMerge::Part {
  MemoryDirectory dir;
  bstring terms;
  uint64_t count = 0;
  uint64_t total_doc_freq = 0;
  uint64_t total_term_freq = 0;
  IndexInput::ptr doc;
  IndexInput::ptr pos;
  IndexInput::ptr pay;
  bool done = false;
};

TermRangeMerge::TermRangeMerge(field_id field, FieldProperties props,
                               std::vector<Input> inputs, bool sorted,
                               const MergeProgress& progress)
  : _field{field},
    _props{props},
    _inputs{std::move(inputs)},
    _keys{KeysOf(_inputs)},
    _merge{SourcesOf(_keys)},
    _progress{&progress},
    _sorted{sorted} {}

TermRangeMerge::~TermRangeMerge() = default;

uint64_t TermRangeMerge::Volume() const {
  return absl::c_accumulate(_inputs, uint64_t{0},
                            [](uint64_t volume, const Input& input) {
                              return volume + input.terms->total_term_freq();
                            });
}

void TermRangeMerge::Partition(size_t parts, const Target& target,
                               duckdb::TaskScheduler* scheduler,
                               size_t threads) {
  _target = target;
  _merge.Partition(parts, true, scheduler, threads);
  for (size_t p = 1; p < _merge.PartitionCount(); ++p) {
    for (size_t i = 0; i != _keys.size(); ++i) {
      if (const auto row = _merge.PartitionBegin(p, i);
          row != 0 && row < _keys[i]->Count()) {
        _keys[i]->AddAnchor(row - 1);
      }
    }
  }
  _parts.clear();
  for (size_t p = 0; p != _merge.PartitionCount(); ++p) {
    _parts.emplace_back(std::make_unique<Part>());
  }
  _docs.assign(bitset::bits_to_words(target.doc_count + doc_limits::min()), 0);
}

bool TermRangeMerge::MergePart(size_t part) {
  SDB_ASSERT(part < _parts.size());
  auto& p = *_parts[part];
  return _sorted ? WritePart<SortedPostings>(p, part)
                 : WritePart<CompoundPostings>(p, part);
}

template<typename Postings>
bool TermRangeMerge::WritePart(Part& part, size_t index) {
  RangeWriter<Postings> writer{_inputs, _keys,    _props,
                               _target, part.dir, *_progress};
  const auto state = _merge.BeginMerge(index);
  duckdb::vector<duckdb::PresortedRun> runs;
  while (!state->Done()) {
    if (!NextTermRuns(_merge, *state, runs, *_progress, _field) ||
        !writer.Consume(runs)) {
      return false;
    }
  }
  if (!writer.Finish()) {
    return false;
  }
  auto& terms = writer.Terms();
  part.count = terms.Count();
  part.total_doc_freq = terms.TotalDocFreq();
  part.total_term_freq = terms.TotalTermFreq();
  part.terms = std::move(terms).TakeTerms();
  const auto open = [&](std::string_view ext) -> IndexInput::ptr {
    const auto name = FileName(kPartName, ext);
    bool exists = false;
    if (!part.dir.exists(exists, name) || !exists) {
      return nullptr;
    }
    return part.dir.open(name, IOAdvice::SEQUENTIAL);
  };
  part.doc = open(PostingsWriter::kDocExt);
  part.pos = open(PostingsWriter::kPosExt);
  part.pay = open(PostingsWriter::kPayExt);
  const auto& docs = writer.Docs();
  absl::MutexLock lock{&_mutex};
  SDB_ASSERT(docs.words() <= _docs.size());
  for (size_t w = 0; w != docs.words(); ++w) {
    _docs[w] |= docs.data()[w];
  }
  part.done = true;
  return true;
}

bool TermRangeMerge::Done() const noexcept {
  return absl::c_all_of(_parts, [](const auto& part) { return part->done; });
}

std::vector<TermPart> TermRangeMerge::Parts() const {
  std::vector<TermPart> parts;
  parts.reserve(_parts.size());
  for (const auto& part : _parts) {
    parts.push_back({
      .doc = part->doc.get(),
      .pos = part->pos.get(),
      .pay = part->pay.get(),
      .terms = part->terms,
      .term_count = part->count,
      .total_doc_freq = part->total_doc_freq,
      .total_term_freq = part->total_term_freq,
    });
  }
  return parts;
}

void TermRangeMerge::ReleaseParts() noexcept {
  for (auto& part : _parts) {
    part->doc.reset();
    part->pos.reset();
    part->pay.reset();
    for (const auto ext : {PostingsWriter::kDocExt, PostingsWriter::kPosExt,
                           PostingsWriter::kPayExt}) {
      part->dir.remove(FileName(kPartName, ext));
    }
    part->terms.clear();
    part->terms.shrink_to_fit();
  }
  _docs.clear();
  _docs.shrink_to_fit();
}

uint64_t TermRangeMerge::DocCount() const noexcept {
  return math::Popcount(_docs.begin(), _docs.end());
}

}  // namespace irs
