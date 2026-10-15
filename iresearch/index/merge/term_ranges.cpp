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
#include <bit>
#include <chrono>
#include <mutex>
#include <string_view>
#include <utility>

#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/merge/merged_postings.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/log.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

constexpr std::string_view kPartName = "part";

class RangeWriterBase : public TermRangeWriter {
 public:
  virtual bool Finish() = 0;
  virtual const bitset& Docs() const noexcept = 0;
  virtual TermPartWriter& Terms() noexcept = 0;

  std::chrono::steady_clock::time_point Started() const noexcept {
    return _started;
  }

 private:
  std::chrono::steady_clock::time_point _started =
    std::chrono::steady_clock::now();
};

template<typename Postings>
class RangeWriter final : public RangeWriterBase {
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

  void Consume(std::span<const duckdb::PresortedRun> runs) final {
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
  }

  bool Finish() final {
    if (_open) {
      Close();
    }
    _writer.EndField();
    _writer.FinishPart();
    return !_postings.Aborted();
  }

  const bitset& Docs() const noexcept final { return _writer.Docs(); }

  TermPartWriter& Terms() noexcept final { return _terms; }

 private:
  struct InputState {
    SeekTermIterator::ptr it;
    TermPostings::ptr postings;
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
    input.it = _sources[i].terms->iterator();
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
    auto add = [this](typename Postings::IteratorsT& itrs) {
      const auto* const empty = TermPostings::empty().get();
      for (auto& entry : itrs) {
        if (entry.it.get() != empty) {
          _inputs[entry.source].postings = std::move(entry.it);
        }
      }
      itrs.clear();
      itrs.reserve(_mask.size());
      for (const auto i : _mask) {
        auto& input = _inputs[i];
        auto it = input.it->ReusePostings(_props.index_features,
                                          std::move(input.postings));
        SDB_ASSERT(it);
        itrs.emplace_back(std::move(it), _sources[i].remap, i);
      }
    };
    _postings.Reset(add, _props.index_features);
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
    _progress{&progress},
    _sorted{sorted} {
  _keys.reserve(_inputs.size());
  duckdb::vector<duckdb::shared_ptr<duckdb::PresortedMergeInput>> sources;
  sources.reserve(_inputs.size());
  for (const auto& input : _inputs) {
    sources.emplace_back(
      _keys.emplace_back(duckdb::make_shared_ptr<TermKeys>(*input.terms)));
  }
  _merge = duckdb::make_shared_ptr<duckdb::PresortedMerge>(std::move(sources));
}

TermRangeMerge::~TermRangeMerge() = default;

void TermRangeMerge::Partition(size_t parts, const Target& target,
                               duckdb::TaskScheduler* scheduler,
                               size_t threads) {
  _target = target;
  if (scheduler != nullptr) {
    _merge->Partition(parts, true, *scheduler, threads);
  } else {
    _merge->Partition(parts, true);
  }
  _parts.clear();
  for (size_t p = 0; p != _merge->PartitionCount(); ++p) {
    auto& part = *_parts.emplace_back(std::make_unique<Part>());
    part.done = true;
    for (size_t i = 0; i != _inputs.size(); ++i) {
      if (_merge->PartitionBegin(p, i) != _merge->PartitionBegin(p + 1, i)) {
        part.done = false;
        break;
      }
    }
  }
  _docs.assign(bitset::bits_to_words(target.doc_count + doc_limits::min()), 0);
}

std::unique_ptr<TermRangeWriter> TermRangeMerge::BeginPart(size_t part) {
  SDB_ASSERT(part < _parts.size());
  auto& dir = _parts[part]->dir;
  if (_sorted) {
    return std::make_unique<RangeWriter<SortedPostings>>(
      _inputs, _keys, _props, _target, dir, *_progress);
  }
  return std::make_unique<RangeWriter<CompoundPostings>>(
    _inputs, _keys, _props, _target, dir, *_progress);
}

bool TermRangeMerge::FinishPart(size_t part, TermRangeWriter& base) {
  SDB_ASSERT(part < _parts.size());
  auto& writer = static_cast<RangeWriterBase&>(base);
  const bool ok = writer.Finish();
  auto& p = *_parts[part];
  const auto& terms = writer.Terms();
  p.terms.assign(terms.Terms());
  p.count = terms.Count();
  p.total_doc_freq = terms.TotalDocFreq();
  p.total_term_freq = terms.TotalTermFreq();
  const auto open = [&](std::string_view ext) -> IndexInput::ptr {
    const auto name = FileName(kPartName, ext);
    bool exists = false;
    if (!p.dir.exists(exists, name) || !exists) {
      return nullptr;
    }
    return p.dir.open(name, IOAdvice::SEQUENTIAL);
  };
  p.doc = open(PostingsWriter::kDocExt);
  p.pos = open(PostingsWriter::kPosExt);
  p.pay = open(PostingsWriter::kPayExt);
  const auto& docs = writer.Docs();
  {
    std::lock_guard lock{_mutex};
    SDB_ASSERT(docs.words() <= _docs.size());
    for (size_t w = 0; w != docs.words(); ++w) {
      _docs[w] |= docs.data()[w];
    }
    p.done = ok;
  }
  _nanos +=
    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - writer.Started())
                            .count());
  return ok;
}

bool TermRangeMerge::MergePart(size_t part) {
  auto writer = BeginPart(part);
  const auto state = _merge->BeginMerge(part);
  duckdb::vector<duckdb::PresortedRun> runs;
  while (!state->Done()) {
    runs.clear();
    duckdb::optional_idx unsorted;
    if (!_merge->Merge(*state, runs, STANDARD_VECTOR_SIZE, *_progress,
                       unsorted)) {
      if (unsorted.IsValid()) {
        SDB_ERROR(IRESEARCH, "Terms of field ", _field, " in merge input ",
                  unsorted.GetIndex(), " are not sorted");
      }
      return false;
    }
    writer->Consume(runs);
  }
  return FinishPart(part, *writer);
}

bool TermRangeMerge::Done() const noexcept { return PartsLeft() == 0; }

size_t TermRangeMerge::PartsLeft() const noexcept {
  std::lock_guard lock{_mutex};
  return static_cast<size_t>(
    absl::c_count_if(_parts, [](const auto& part) { return !part->done; }));
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

uint64_t TermRangeMerge::DocCount() const noexcept {
  std::lock_guard lock{_mutex};
  uint64_t count = 0;
  for (const auto word : _docs) {
    count += static_cast<uint64_t>(std::popcount(word));
  }
  return count;
}

}  // namespace irs
