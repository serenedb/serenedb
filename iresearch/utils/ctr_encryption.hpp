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

#include "encryption.hpp"

namespace irs {

struct Cipher {
  virtual ~Cipher() = default;

  virtual size_t block_size() const = 0;

  virtual bool Encrypt(byte_type* data) const = 0;

  virtual bool Decrypt(byte_type* data) const = 0;
};

class CtrEncryption : public Encryption {
 public:
  static constexpr size_t kDefaultHeaderLength = 4096;
  static constexpr size_t kMinHeaderLength = sizeof(uint64_t);

  explicit CtrEncryption(const Cipher& cipher) noexcept : _cipher(&cipher) {}

  size_t header_length() noexcept override { return kDefaultHeaderLength; }

  bool create_header(std::string_view filename, byte_type* header) final;

  Stream::ptr create_stream(std::string_view filename, byte_type* header) final;

 private:
  const Cipher* _cipher;
};

}  // namespace irs
