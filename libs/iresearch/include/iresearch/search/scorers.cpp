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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "scorers.hpp"

#include "basics/shared.hpp"
// list of statically loaded scorers via init()
#include "basics/register.hpp"
#include "bm25.hpp"
#include "dfi.hpp"
#include "indri_dirichlet.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "lm_dirichlet.hpp"
#include "lm_jelinek_mercer.hpp"
#include "raw_boost.hpp"
#include "raw_dl.hpp"
#include "raw_tf.hpp"
#include "tfidf.hpp"

namespace irs {
namespace {

struct Key {
  explicit Key(std::string_view name, const TypeInfo& args_format)
    : name{name}, args_format{args_format} {}

  bool operator==(const Key& other) const = default;

  template<typename H>
  friend H AbslHashValue(H h, const Key& key) {
    return H::combine(std::move(h), key.args_format.id(), key.name);
  }

  const std::string_view name;
  TypeInfo args_format;
};


class ScorerRegister
  : public TaggedGenericRegister<Key, Scorer::ptr (*)(std::string_view args),
                                 std::string_view, ScorerRegister> {};

}  // namespace

bool scorers::Exists(std::string_view name, const TypeInfo& args_format,
                     bool load_library /*= true*/) {
  return ScorerRegister::instance().get(Key{name, args_format}, load_library);
}

Scorer::ptr scorers::Get(std::string_view name, const TypeInfo& args_format,
                         std::string_view args,
                         bool load_library /*= true*/) noexcept {
  try {
    auto* factory =
      ScorerRegister::instance().get(Key{name, args_format}, load_library);

    return factory ? factory(args) : nullptr;
  } catch (...) {
    SDB_ERROR(IRESEARCH, "Caught exception while getting a scorer instance");
  }

  return nullptr;
}

void scorers::Init() {
  BM25::init();
  TFIDF::init();
  LMJelinekMercer::init();
  LMDirichlet::init();
  IndriDirichlet::init();
  DFI::init();
  RawBoost::init();
  RawTF::init();
  RawDL::init();
}

void scorers::LoadAll(std::string_view path) {
  (void)path;  // plugin loading via .so removed; SereneDB ships no out-of-tree iresearch plugins
}

bool scorers::Visit(
  const std::function<bool(std::string_view, const TypeInfo&)>& visitor) {
  return ScorerRegister::instance().visit(
    [&](const Key& key) { return visitor(key.name, key.args_format); });
}

ScorerRegistrar::ScorerRegistrar(const TypeInfo& type,
                                 const TypeInfo& args_format,
                                 Scorer::ptr (*factory)(std::string_view args),
                                 const char* source /*= nullptr*/) {
  const auto source_ref =
    source ? std::string_view{source} : std::string_view{};
  auto entry =
    ScorerRegister::instance().set(Key{type.name(), args_format}, factory,
                                   IsNull(source_ref) ? nullptr : &source_ref);

  _registered = entry.second;

  if (!_registered && factory != entry.first) {
    auto* registered_source =
      ScorerRegister::instance().tag(Key{type.name(), args_format});

    if (source && registered_source) {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering scorer, "
               "ignoring: type '",
               type.name(), "' from ", source_ref, ", previously from ",
               *registered_source);
    } else if (source) {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering scorer, "
               "ignoring: type '",
               type.name(), "' from ", source_ref);
    } else if (registered_source) {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering scorer, "
               "ignoring: type '",
               type.name(), "', previously from ", *registered_source);
    } else {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering scorer, "
               "ignoring: type '",
               type.name(), "'");
    }
  }
}

}  // namespace irs
