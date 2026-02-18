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

#pragma once

#include <functional>
#include <map>

#include "basics/memory.hpp"
#include "basics/noncopyable.hpp"
#include "iresearch/utils/string.hpp"
#include "type_id.hpp"

#define REGISTER_COMPRESSION_IMPL(compression_name, compressor_factory,      \
                                  decompressor_factory, line, source)        \
  static irs::compression::CompressionRegistrar                              \
    compression_registrar##_##line(::irs::Type<compression_name>::get(),     \
                                   compressor_factory, decompressor_factory, \
                                   source)
#define REGISTER_COMPRESSION_EXPANDER(compression_name, compressor_factory, \
                                      decompressor_factory, file, line)     \
  REGISTER_COMPRESSION_IMPL(compression_name, compressor_factory,           \
                            decompressor_factory, line,                     \
                            file ":" IRS_TO_STRING(line))
#define REGISTER_COMPRESSION(compression_name, compressor_factory,    \
                             decompressor_factory)                    \
  REGISTER_COMPRESSION_EXPANDER(compression_name, compressor_factory, \
                                decompressor_factory, __FILE__, __LINE__)

namespace irs {

class DataOutput;
class DataInput;

namespace compression {

struct Options {
  enum class Hint : uint8_t {
    /// @brief use default compressor parameters
    Default = 0,

    /// @brief prefer speed over compression ratio
    Speed,

    /// @brief prefer compression ratio over speed
    Compression,
  };

  Hint hint{Hint::Default};

  Options(Hint hint = Hint::Default) : hint(hint) {}

  bool operator==(const Options& rhs) const noexcept {
    return hint == rhs.hint;
  }
};

struct Compressor : memory::Managed {
  using ptr = memory::managed_ptr<Compressor>;

  /// @note returns a value as it is
  static ptr identity() noexcept;

  /// @note caller is allowed to modify data pointed by 'in' up to 'size'
  virtual bytes_view compress(byte_type* in, size_t size, bstring& buf) = 0;
};

struct Decompressor : memory::Managed {
  using ptr = memory::managed_ptr<Decompressor>;

  /// @note caller is allowed to modify data pointed by 'dst' up to 'dst_size'
  virtual bytes_view decompress(const byte_type* src, size_t src_size,
                                byte_type* dst, size_t dst_size) = 0;

  // FIXME: make sure no Compressor relies the data_input here is the source
  // of src in decompress call.
  virtual bool prepare(DataInput& /*in*/) {
    // NOOP
    return true;
  }
};

using compressor_factory_f =
  irs::compression::Compressor::ptr (*)(const Options&);
using decompressor_factory_f = irs::compression::Decompressor::ptr (*)();

class CompressionRegistrar {
 public:
  CompressionRegistrar(const TypeInfo& type,
                       compressor_factory_f compressor_factory,
                       decompressor_factory_f decompressor_factory,
                       const char* source = nullptr);

  operator bool() const noexcept { return _registered; }

 private:
  bool _registered;
};

////////////////////////////////////////////////////////////////////////////////
/// @brief checks whether an comopression with the specified name is registered
////////////////////////////////////////////////////////////////////////////////
bool Exists(std::string_view name, bool load_library = true);

////////////////////////////////////////////////////////////////////////////////
/// @brief creates a compressor by name, or nullptr if not found
////////////////////////////////////////////////////////////////////////////////
Compressor::ptr GetCompressor(std::string_view name, const Options& opts,
                              bool load_library = true) noexcept;

////////////////////////////////////////////////////////////////////////////////
/// @brief creates a compressor by name, or nullptr if not found
////////////////////////////////////////////////////////////////////////////////
inline Compressor::ptr GetCompressor(const TypeInfo& type, const Options& opts,
                                     bool load_library = true) noexcept {
  return GetCompressor(type.name(), opts, load_library);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief creates a decompressor by name, or nullptr if not found
////////////////////////////////////////////////////////////////////////////////
Decompressor::ptr GetDecompressor(std::string_view name,
                                  bool load_library = true) noexcept;

////////////////////////////////////////////////////////////////////////////////
/// @brief creates a decompressor by name, or nullptr if not found
////////////////////////////////////////////////////////////////////////////////
inline Decompressor::ptr GetDecompressor(const TypeInfo& type,
                                         bool load_library = true) noexcept {
  return GetDecompressor(type.name(), load_library);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief for static lib reference all known compressions in lib
///        for shared lib NOOP
///        no explicit call of fn is required, existence of fn is sufficient
////////////////////////////////////////////////////////////////////////////////
void Init();

////////////////////////////////////////////////////////////////////////////////
/// @brief load all compressions from plugins directory
////////////////////////////////////////////////////////////////////////////////
void LoadAll(std::string_view path);

////////////////////////////////////////////////////////////////////////////////
/// @brief visit all loaded compressions, terminate early if visitor returns
/// false
////////////////////////////////////////////////////////////////////////////////
bool Visit(const std::function<bool(std::string_view)>& visitor);

struct None {
  static constexpr std::string_view type_name() noexcept {
    return "irs::compression::none";
  }

  static void init();

  static compression::Compressor::ptr compressor(const Options& /*opts*/) {
    return nullptr;
  }

  static compression::Decompressor::ptr decompressor() { return nullptr; }
};

}  // namespace compression
}  // namespace irs
