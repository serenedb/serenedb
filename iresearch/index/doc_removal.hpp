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
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/index/document_mask.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/containers/flat_hash_set.hpp"
#include "iresearch/utils/containers/node_hash_map.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct SubReader;

struct AbsorbedStep {
  std::shared_ptr<const DocumentMask> mask;
  doc_id_t rank_begin = 0;
  doc_id_t rank_end = 0;
  doc_id_t base = doc_limits::min();
};

struct AbsorbedSegment {
  uint32_t number = 0;
  AbsorbedStep step;
};

struct DocMap {
  uint32_t source = 0;
  uint32_t target = 0;
  std::vector<AbsorbedStep> steps;
};

using DocMapList = std::vector<std::shared_ptr<const DocMap>>;

struct DocRemoval {
  static constexpr uint32_t kNoSegment = std::numeric_limits<uint32_t>::max();

  static DocRemoval All() { return DocRemoval{.all = true}; }

  static DocRemoval Keys(field_id field, std::vector<bstring> keys) {
    return DocRemoval{.field = field, .keys = std::move(keys)};
  }

  bool Keyed() const noexcept { return field != field_limits::invalid(); }

  size_t Find(uint32_t segment) const noexcept;

  const DocumentMask& Docs(size_t group) const noexcept { return *docs[group]; }

  bool all = false;
  std::vector<uint32_t> segments;
  std::vector<std::shared_ptr<const DocumentMask>> docs;
  std::shared_ptr<const DocMapList> maps;
  field_id field = field_limits::invalid();
  std::vector<bstring> keys;
  uint64_t tick = 0;
};

class DocRemovalBuilder {
 public:
  void Add(uint32_t segment, doc_id_t doc);
  void AddRange(uint32_t segment, doc_id_t first, doc_id_t last);
  void Add(uint32_t segment, const DocumentMask& docs);

  bool Empty() const noexcept { return _groups.empty(); }

  DocRemoval Finish() &&;

 private:
  DocumentMaskBuilder& Group(uint32_t segment);

  std::vector<std::pair<uint32_t, DocumentMaskBuilder>> _groups;
  size_t _last = 0;
};

bool RemoveKeyedDocs(DocRemoval& removal, const SubReader& segment,
                     DocumentMaskBuilder& removed, doc_id_t end);

uint32_t SegmentNumber(std::string_view name) noexcept;

void AbsorbedDocs(std::span<const AbsorbedStep> steps,
                  std::vector<doc_id_t>& docs);

std::string WriteDocMaps(const DocMapList& maps);
DocMapList ReadDocMaps(std::string_view bytes);

std::string WriteDocRemoval(const DocRemoval& removal);
DocRemoval ReadDocRemoval(std::string_view bytes);

class DocRemovalResolver;

class DocMaps {
 public:
  void Absorb(std::span<const DocMap> direct);
  void Committed(uint32_t source, uint64_t seq) noexcept;
  void CopyTo(std::span<const uint32_t> sources, DocMapList& out) const;
  std::span<const std::shared_ptr<const DocMap>> Of(
    uint32_t source) const noexcept;
  void Retire(uint64_t oldest_reader, const DocRemovalResolver& removals);

 private:
  static constexpr uint64_t kUncommitted = std::numeric_limits<uint64_t>::max();

  struct Source {
    uint64_t absorbed_at = kUncommitted;
    bool retiring = false;
    DocMapList maps;
  };

  containers::FlatHashMap<uint32_t, Source> _sources;
};

class DocRemovalResolver {
 public:
  void Prepare(std::span<const DocRemoval* const> removals,
               std::span<const uint32_t> targets, const DocMaps& maps);

  const DocumentMask* PositionalDocs(const DocRemoval& removal,
                                     const SubReader& segment) const;

  bool Used(uint32_t absorbed) const noexcept {
    return _used.contains(absorbed);
  }

 private:
  containers::NodeHashMap<const DocRemoval*, DocRemoval> _resolved;
  containers::FlatHashSet<uint32_t> _used;
};

}  // namespace irs
