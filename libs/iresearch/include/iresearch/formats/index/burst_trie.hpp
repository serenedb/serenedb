////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <memory>
#include <string_view>

#include "iresearch/formats/formats.hpp"

namespace irs {

class IdxWriter;

}  // namespace irs
namespace irs::burst_trie {

enum class Version : int32_t {
  // * Encryption support
  // * Term dictionary stored on disk as fst::fstext::ImmutableFst<...>
  // * Pluggable field features support
  // * WAND support
  Min = 0,
  Max = Min,
};

class FieldWriter final {
 public:
  FieldWriter(PostingsWriter::ptr pw, bool compaction, IResourceManager& rm,
              Version version = Version::Max);
  ~FieldWriter();

  FieldWriter(const FieldWriter&) = delete;
  FieldWriter& operator=(const FieldWriter&) = delete;

  void SetIdxWriter(IdxWriter& idx) noexcept;
  void prepare(const FlushState& state);
  void write(const BasicTermReader& reader);
  void end();

 private:
  class Impl;
  std::unique_ptr<Impl> _impl;
};

class FieldReader final {
 public:
  FieldReader(PostingsReader::ptr pr, IResourceManager& rm);
  ~FieldReader();

  FieldReader(const FieldReader&) = delete;
  FieldReader& operator=(const FieldReader&) = delete;

  uint64_t CountMappedMemory() const;
  void prepare(const ReaderState& state);

  const TermReader* field(field_id id) const;

  std::span<const field_id> field_ids() const noexcept;

  size_t size() const noexcept;

 private:
  class Impl;
  std::unique_ptr<Impl> _impl;
};

}  // namespace irs::burst_trie
