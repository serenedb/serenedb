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

#include "iresearch/formats/formats.hpp"

namespace irs::burst_trie {

enum class Version : int32_t {
  // * Encryption support
  // * Term dictionary stored on disk as fst::fstext::ImmutableFst<...>
  // * Pluggable field features support
  // * WAND support
  Min = 0,
  Max = Min,
};

FieldWriter::ptr MakeWriter(Version version, PostingsWriter::ptr&& writer,
                            bool consolidation,
                            IResourceManager& resource_manager);

FieldReader::ptr MakeReader(PostingsReader::ptr&& reader,
                            IResourceManager& resource_manager);

}  // namespace irs::burst_trie
