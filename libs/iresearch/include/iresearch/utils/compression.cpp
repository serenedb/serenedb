////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "compression.hpp"

#include "basics/register.hpp"

// list of statically loaded scorers via init()
#include "delta_compression.hpp"
#include "lz4compression.hpp"

namespace irs::compression {
namespace {

struct Value {
  explicit Value(compressor_factory_f compressor_factory = nullptr,
                 decompressor_factory_f decompressor_factory = nullptr)
    : compressor_factory(compressor_factory),
      decompressor_factory(decompressor_factory) {}

  bool Empty() const noexcept {
    return compressor_factory == nullptr || decompressor_factory == nullptr;
  }

  bool operator==(const Value& other) const noexcept {
    return compressor_factory == other.compressor_factory &&
           decompressor_factory == other.decompressor_factory;
  }

  const compressor_factory_f compressor_factory;
  const decompressor_factory_f decompressor_factory;
};


class CompressionRegister
  : public TaggedGenericRegister<std::string_view, Value, std::string_view,
                                 CompressionRegister> {};

struct IdentityCompressor : Compressor {
  bytes_view compress(byte_type* in, size_t size, bstring& /*buf*/) final {
    return {in, size};
  }
};

IdentityCompressor gIdentityCompressor;

}  // namespace

CompressionRegistrar::CompressionRegistrar(
  const TypeInfo& type, compressor_factory_f compressor_factory,
  decompressor_factory_f decompressor_factory,
  const char* source /*= nullptr*/) {
  const auto source_ref =
    source != nullptr ? std::string_view{source} : std::string_view{};
  const auto new_entry = Value(compressor_factory, decompressor_factory);

  auto entry = CompressionRegister::instance().set(
    type.name(), new_entry, IsNull(source_ref) ? nullptr : &source_ref);

  _registered = entry.second;

  if (!_registered && new_entry != entry.first) {
    const auto* registered_source =
      CompressionRegister::instance().tag(type.name());

    if (source != nullptr && registered_source != nullptr) {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering "
               "compression, ignoring: type '",
               type.name(), "' from ", source_ref, ", previously from ",
               *registered_source);
    } else if (source != nullptr) {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering "
               "compression, ignoring: type '",
               type.name(), "' from ", source_ref);
    } else if (registered_source != nullptr) {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering "
               "compression, ignoring: type '",
               type.name(), "' previously from ", *registered_source);
    } else {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering "
               "compression, ignoring: type '",
               type.name(), "'");
    }
  }
}

bool Exists(std::string_view name, bool load_library /*= true*/) {
  return !CompressionRegister::instance().get(name, load_library).Empty();
}

Compressor::ptr GetCompressor(std::string_view name, const Options& opts,
                              bool load_library /*= true*/) noexcept {
  try {
    auto* factory = CompressionRegister::instance()
                      .get(name, load_library)
                      .compressor_factory;

    return factory != nullptr ? factory(opts) : nullptr;
  } catch (...) {
    SDB_ERROR(IRESEARCH, "Caught exception while getting an analyzer instance");
  }

  return nullptr;
}

Decompressor::ptr GetDecompressor(std::string_view name,
                                  bool load_library /*= true*/) noexcept {
  try {
    auto* factory = CompressionRegister::instance()
                      .get(name, load_library)
                      .decompressor_factory;

    return factory != nullptr ? factory() : nullptr;
  } catch (...) {
    SDB_ERROR(IRESEARCH, "Caught exception while getting an analyzer instance");
  }

  return nullptr;
}

void Init() {
  Lz4::init();
  Delta::init();
  None::init();
}

void LoadAll(std::string_view path) {
  (void)path;  // plugin loading via .so removed; SereneDB ships no out-of-tree iresearch plugins
}

bool Visit(const std::function<bool(std::string_view)>& visitor) {
  const CompressionRegister::visitor_t wrapper =
    [&visitor](std::string_view key) -> bool { return visitor(key); };

  return CompressionRegister::instance().visit(wrapper);
}

void None::init() {
  REGISTER_COMPRESSION(None, &None::compressor, &None::decompressor);
}

Compressor::ptr Compressor::identity() noexcept {
  return memory::to_managed<Compressor>(gIdentityCompressor);
}

}  // namespace irs::compression
