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

#include "iresearch/index/merge/input_norms.hpp"

#include <algorithm>
#include <utility>

#include "iresearch/formats/norm_reader_impl.hpp"
#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/merge/norms_unit.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

using Input = InputNormProvider::Input;

class InputNormReader : public NormReader {
 public:
  InputNormReader(std::span<const Input> inputs,
                  std::vector<NormReader::ptr> readers, score_t avg) noexcept
    : _inputs{inputs}, _readers{std::move(readers)}, _avg{avg} {}

  void Get(std::span<const doc_id_t> docs, std::span<uint32_t> values) final {
    SDB_ASSERT(docs.size() <= values.size());
    for (size_t at = 0; at != docs.size();) {
      const auto i = Find(docs[at]);
      const auto& input = _inputs[i];
      size_t end = at + 1;
      while (end != docs.size() && docs[end] < input.end) {
        ++end;
      }
      const auto out = values.subspan(at, end - at);
      if (!_readers[i]) {
        std::ranges::fill(out, 0);
      } else {
        _docs.resize(out.size());
        for (size_t k = 0; k != out.size(); ++k) {
          _docs[k] = Source(input, docs[at + k]);
        }
        _readers[i]->Get(_docs, out);
      }
      at = end;
    }
  }

  void GetScoreBlock(std::span<const doc_id_t, kScoreBlock> docs,
                     std::span<uint32_t, kScoreBlock> values) final {
    Get(std::span<const doc_id_t>{docs}, std::span<uint32_t>{values});
  }

  void GetPostingBlock(std::span<const doc_id_t, kPostingBlock> docs,
                       std::span<uint32_t, kPostingBlock> values) final {
    Get(std::span<const doc_id_t>{docs}, std::span<uint32_t>{values});
  }

  uint32_t Get(doc_id_t doc) final {
    const auto i = Find(doc);
    if (!_readers[i]) {
      return 0;
    }
    return _readers[i]->Get(Source(_inputs[i], doc));
  }

  score_t GetAvg() const noexcept final { return _avg; }

 private:
  size_t Find(doc_id_t doc) const noexcept {
    const auto it = std::ranges::upper_bound(_inputs, doc, {}, &Input::first);
    SDB_ASSERT(it != _inputs.begin());
    const auto i = static_cast<size_t>(it - _inputs.begin()) - 1;
    SDB_ASSERT(doc < _inputs[i].end);
    return i;
  }

  static doc_id_t Source(const Input& input, doc_id_t doc) noexcept {
    if (input.docs.empty()) {
      return doc - input.first + doc_limits::min();
    }
    return input.docs[doc - input.first];
  }

  std::span<const Input> _inputs;
  std::vector<NormReader::ptr> _readers;
  score_t _avg;
  std::vector<doc_id_t> _docs;
};

}  // namespace

InputNormProvider::InputNormProvider(std::span<const MergeSource> sources,
                                     IResourceManager& memory) {
  _inputs.reserve(sources.size());
  doc_id_t next = doc_limits::min();
  for (const auto& source : sources) {
    SDB_ASSERT(source.remap);
    SDB_ASSERT(!source.remap->id_map.empty() || source.remap->base_id == next);
    const auto end = next + static_cast<doc_id_t>(source.alive_count);
    _inputs.push_back({.source = &source,
                       .first = next,
                       .end = end,
                       .docs = ManagedVector<doc_id_t>{{memory}}});
    next = end;
  }
}

void InputNormProvider::AddField(field_id column, field_id field, uint64_t sum,
                                 uint64_t non_zero) {
  _fields.emplace(
    column, Field{.field = field,
                  .avg = non_zero == 0 ? score_t{}
                                       : static_cast<score_t>(
                                           static_cast<double>(sum) /
                                           static_cast<double>(non_zero))});
}

void InputNormProvider::MapDocs() const {
  for (auto& input : _inputs) {
    const auto& id_map = input.source->remap->id_map;
    if (id_map.empty()) {
      continue;
    }
    input.docs.resize(input.end - input.first);
    for (doc_id_t doc = doc_limits::min(); doc < id_map.size(); ++doc) {
      if (const auto out = id_map[doc]; !doc_limits::eof(out)) {
        input.docs[out - input.first] = doc;
      }
    }
  }
}

NormReader::ptr InputNormProvider::norms(field_id column) const {
  const auto it = _fields.find(column);
  if (it == _fields.end()) {
    return {};
  }
  absl::call_once(_mapped, [this] { MapDocs(); });
  std::vector<NormReader::ptr> readers;
  readers.reserve(_inputs.size());
  for (const auto& input : _inputs) {
    const auto* norms = SourceNormColumn(*input.source, it->second.field);
    readers.emplace_back(norms ? MakePersistedNormReader(*norms)
                               : NormReader::ptr{});
  }
  return memory::make_managed<InputNormReader>(_inputs, std::move(readers),
                                               it->second.avg);
}

}  // namespace irs
