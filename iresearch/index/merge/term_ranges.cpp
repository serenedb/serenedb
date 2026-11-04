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
#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <span>
#include <string_view>
#include <utility>

#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/formats/index/term_dict_ranges.hpp"
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

uint32_t KeyWidth(std::span<const TermRangeMerge::Input> inputs) noexcept {
  if (inputs.empty()) {
    return 0;
  }
  const auto width = inputs.front().terms->key_width;
  return absl::c_all_of(inputs,
                        [&](const TermRangeMerge::Input& input) {
                          return input.terms->key_width == width;
                        })
           ? width
           : 0;
}

template<typename Postings>
class RangeWriter {
 public:
  RangeWriter(std::span<const TermRangeMerge::Input> sources,
              std::span<const duckdb::shared_ptr<TermKeys>> keys,
              const FieldProperties& props,
              const TermRangeMerge::Target& target, Directory& dir,
              const MergeProgress& progress, term_dict::RangeWriter* ranges)
    : _sources{sources},
      _keys{keys},
      _inputs(sources.size()),
      _props{props},
      _postings{progress},
      _terms{props.index_features},
      _ranges{ranges} {
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
    if (_ranges) {
      _ranges->Begin(_writer, target.doc_count, props.index_features);
    }
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
    if (_ranges) {
      _ranges->Seal();
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
    input.it = _sources[i].terms->iterator();
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
    _writer.Write(_postings, meta, _ranges ? _ranges->Tee() : DocTee{});
    if (meta.docs_count != 0) {
      _terms.Add(_current, meta);
      if (_ranges) {
        _ranges->Add(_current, meta);
      }
    }
  }

  std::span<const TermRangeMerge::Input> _sources;
  std::span<const duckdb::shared_ptr<TermKeys>> _keys;
  std::vector<InputState> _inputs;
  FieldProperties _props;
  PostingsWriter _writer;
  Postings _postings;
  TermPartWriter _terms;
  term_dict::RangeWriter* _ranges;
  std::vector<size_t> _mask;
  bytes_view _current;
  uint64_t _groups = 0;
  bool _open = false;
};

}  // namespace

TermRangeSink::TermRangeSink(Directory& dir, std::string_view segment,
                             IndexFeatures features)
  : _dir{&dir}, _segment{segment} {
  absl::Cleanup remove = [this]() noexcept { Remove(); };
  PostingsWriter::PrepareOutputs(_out, {
                                         .dir = &dir,
                                         .name = _segment,
                                         .doc_count = 0,
                                         .index_features = features,
                                       });
  std::move(remove).Cancel();
}

TermRangeSink::~TermRangeSink() { Remove(); }

void TermRangeSink::Remove() noexcept {
  const auto remove = [&](IndexOutput::ptr& out, std::string_view ext) {
    if (!out) {
      return;
    }
    out.reset();
    _dir->remove(FileName(_segment, ext));
  };
  remove(_out.doc, PostingsWriter::kDocExt);
  remove(_out.pos, PostingsWriter::kPosExt);
  remove(_out.pay, PostingsWriter::kPayExt);
}

struct TermRangeMerge::Part {
  std::unique_ptr<MemoryDirectory> dir;
  bstring terms;
  term_dict::RangeWriter ranges;
  uint64_t count = 0;
  uint64_t total_doc_freq = 0;
  uint64_t total_term_freq = 0;
  uint64_t inline_bytes = 0;
  PostingBases bases;
  bool done = false;
};

TermRangeMerge::TermRangeMerge(field_id field, DictMember member,
                               FieldProperties props, std::vector<Input> inputs,
                               bool sorted, const MergeProgress& progress)
  : _field{field},
    _member{member},
    _props{props},
    _inputs{std::move(inputs)},
    _keys{KeysOf(_inputs)},
    _merge{SourcesOf(_keys)},
    _progress{&progress},
    _sorted{sorted} {
  const auto width = KeyWidth(_inputs);
  _ranged = IsSubsetOf(IndexFeatures::Ranges, _props.index_features) &&
            width != 0 && width <= term_dict::kMaxStride;
}

TermRangeMerge::~TermRangeMerge() = default;

uint64_t TermVolume(const TermReader& terms) noexcept {
  return std::max(terms.total_term_freq(), uint64_t{terms.docs_count()});
}

uint64_t TermRangeMerge::Volume() const {
  return absl::c_accumulate(_inputs, uint64_t{0},
                            [](uint64_t volume, const Input& input) {
                              return volume + TermVolume(*input.terms);
                            });
}

void TermRangeMerge::Partition(size_t parts, const Target& target,
                               duckdb::TaskScheduler& scheduler,
                               size_t threads) {
  _target = target;
  _merge.Partition(parts, true, &scheduler, threads);
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
  _appended = 0;
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
  SDB_ASSERT(_target.sink);
  part.dir = std::make_unique<MemoryDirectory>();
  RangeWriter<Postings> writer{_inputs,
                               _keys,
                               _props,
                               _target,
                               *part.dir,
                               *_progress,
                               _ranged ? &part.ranges : nullptr};
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
  part.inline_bytes = terms.InlineBytes();
  part.terms = std::move(terms).TakeTerms();
  part.terms.shrink_to_fit();
  const auto& docs = writer.Docs();
  absl::MutexLock lock{&_target.sink->Mutex()};
  SDB_ASSERT(docs.words() <= _docs.size());
  for (size_t w = 0; w != docs.words(); ++w) {
    _docs[w] |= docs.data()[w];
  }
  part.done = true;
  for (; _appended != _parts.size() && _parts[_appended]->done; ++_appended) {
    auto& next = *_parts[_appended];
    if (next.count != 0) {
      const auto open = [&](std::string_view ext) -> IndexInput::ptr {
        const auto name = FileName(kPartName, ext);
        bool exists = false;
        if (!next.dir->exists(exists, name) || !exists) {
          return nullptr;
        }
        return next.dir->open(name, IOAdvice::SEQUENTIAL);
      };
      const auto doc = open(PostingsWriter::kDocExt);
      const auto pos = open(PostingsWriter::kPosExt);
      const auto pay = open(PostingsWriter::kPayExt);
      next.bases = AppendPostings(_target.sink->Outputs(), doc.get(), pos.get(),
                                  pay.get());
    }
    next.dir.reset();
  }
  return true;
}

bool TermRangeMerge::Done() const noexcept {
  return _appended == _parts.size();
}

std::vector<TermPart> TermRangeMerge::Parts() const {
  std::vector<TermPart> parts;
  parts.reserve(_parts.size());
  for (const auto& part : _parts) {
    parts.push_back({
      .bases = part->bases,
      .terms = part->terms,
      .ranges = _ranged ? &part->ranges : nullptr,
      .term_count = part->count,
      .total_doc_freq = part->total_doc_freq,
      .total_term_freq = part->total_term_freq,
      .inline_bytes = part->inline_bytes,
    });
  }
  return parts;
}

void TermRangeMerge::ReleaseParts() noexcept {
  for (auto& part : _parts) {
    part->terms.clear();
    part->terms.shrink_to_fit();
    part->ranges.Clear();
  }
  _docs.clear();
  _docs.shrink_to_fit();
}

uint64_t TermRangeMerge::DocCount() const noexcept {
  return math::Popcount(_docs.begin(), _docs.end());
}

}  // namespace irs
