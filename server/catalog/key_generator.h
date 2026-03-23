////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <vpack/slice.h>

#include <memory>

#include "basics/common.h"
#include "basics/error_code.h"

namespace vpack {

class Builder;

}  // namespace vpack
namespace sdb {

// please note that coordinator-based key generators are frequently
// created and discarded, so ctor & dtor need to be very efficient.
// additionally, do not put any state into this object, as for the
// same logical collection the ClusterInfo may create many different
// temporary catalog::Table objects one after the other, which
// will also discard the collection's particular KeyGenerator object!
class KeyGenerator {
 public:
  virtual ~KeyGenerator() = default;

  virtual bool hasDynamicState() const noexcept { return true; }

  // if the returned string is empty, it means no proper key was
  // generated, and the caller must handle the situation
  virtual std::string generate(vpack::Slice input) = 0;

  virtual ErrorCode validate(std::string_view key, vpack::Slice input,
                             bool is_restore) noexcept;

  virtual void track(std::string_view key) noexcept = 0;

  virtual void toVPack(vpack::Builder&) const;

  // initialize key generator state, reading data/state from the
  // state object. state is guaranteed to be a vpack object
  virtual void initState(vpack::Slice state);

  bool allowUserKeys() const noexcept { return _allow_user_keys; }

 protected:
  explicit KeyGenerator(bool allow_user_keys)
    : _allow_user_keys{allow_user_keys} {}

  ErrorCode globalCheck(const char* p, size_t length,
                        bool is_restore) const noexcept;

  const bool _allow_user_keys;
};

struct KeyGeneratorHelper {
  static constexpr size_t kMaxKeyLength = 254;
  static const std::string kLowestKey;
  static const std::string kHighestKey;

  static std::vector<std::string_view> generatorNames();

  static uint64_t decodePadded(const char* p, size_t length) noexcept;
  static std::string encodePadded(uint64_t value);

  static bool validateKey(const char* key, size_t len) noexcept;

  static bool validateId(const char* key, size_t len, size_t& split) noexcept;

  static std::shared_ptr<KeyGenerator> createKeyGenerator(
    uint32_t number_of_shards, vpack::Slice slice);
  static std::shared_ptr<KeyGenerator> createTraditional();
};

void VPackRead(auto ctx, std::shared_ptr<KeyGenerator>& key_gen) {
  // TODO(gnusi): remove first arg
  key_gen = KeyGeneratorHelper::createKeyGenerator(1, ctx.vpack());
}

void VPackWrite(auto ctx, const KeyGenerator& key_gen) {
  auto& b = ctx.vpack();
  b.openObject(false);
  key_gen.toVPack(b);
  b.close();
}

}  // namespace sdb
