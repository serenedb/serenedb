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

#include "ctr_encryption.hpp"

#include <chrono>

#include "absl/strings/str_cat.h"
#include "basics/shared.hpp"

namespace irs {
namespace {

void DecodeCtrHeader(bytes_view header, size_t block_size,
                     uint64_t& base_counter, bytes_view& iv) {
  SDB_ASSERT(header.size() >= irs::CtrEncryption::kMinHeaderLength &&
             header.size() >= 2 * block_size);

  const auto* begin = header.data();
  base_counter = irs::read<uint64_t>(begin);
  iv = bytes_view(header.data() + block_size, block_size);
}

}  // namespace

class CtrCipherStream : public Encryption::Stream {
 public:
  explicit CtrCipherStream(const Cipher& cipher, bytes_view iv,
                           uint64_t counter_base) noexcept
    : _cipher(&cipher), _iv(iv), _counter_base(counter_base) {}

  size_t block_size() const noexcept final { return _cipher->block_size(); }

  bool Encrypt(uint64_t offset, byte_type* data, size_t size) final;

  bool Decrypt(uint64_t offset, byte_type* data, size_t size) final;

 private:
  // for CTR decryption and encryption are the same
  bool CryptBlock(uint64_t block_index, byte_type* IRS_RESTRICT data,
                  byte_type* IRS_RESTRICT scratch);

  const Cipher* _cipher;
  bstring _iv;
  uint64_t _counter_base;
};

bool CtrCipherStream::Encrypt(uint64_t offset, byte_type* data, size_t size) {
  const auto block_size = this->block_size();
  SDB_ASSERT(block_size);
  uint64_t block_index = offset / block_size;
  size_t block_offset = offset % block_size;

  bstring block_buf;
  bstring scratch(std::max(block_size, sizeof(uint64_t)), 0);

  // encrypt block by block
  while (true) {
    byte_type* block = data;
    const size_t n = std::min(size, block_size - block_offset);

    if (n != block_size) {
      block_buf.resize(block_size);
      block = block_buf.data();
      std::memmove(block + block_offset, data, n);
    }

    if (!CryptBlock(block_index, block, scratch.data())) {
      return false;
    }

    if (block != data) {
      std::memmove(data, block + block_offset, n);
    }

    size -= n;

    if (!size) {
      return true;
    }

    data += n;
    block_offset = 0;
    ++block_index;
  }
}

bool CtrCipherStream::Decrypt(uint64_t offset, byte_type* data, size_t size) {
  const auto block_size = this->block_size();
  SDB_ASSERT(block_size);
  uint64_t block_index = offset / block_size;
  size_t block_offset = offset % block_size;

  bstring block_buf;
  bstring scratch(std::max(block_size, sizeof(uint64_t)), 0);

  // decrypt block by block
  while (true) {
    byte_type* block = data;
    const size_t n = std::min(size, block_size - block_offset);

    if (n != block_size) {
      block_buf.resize(block_size, 0);
      block = block_buf.data();
      std::memmove(block + block_offset, data, n);
    }

    if (!CryptBlock(block_index, block, scratch.data())) {
      return false;
    }

    if (block != data) {
      std::memmove(data, block + block_offset, n);
    }

    size -= n;

    if (!size) {
      return true;
    }

    data += n;
    block_offset = 0;
    ++block_index;
  }
}

bool CtrCipherStream::CryptBlock(uint64_t block_index,
                                 byte_type* IRS_RESTRICT data,
                                 byte_type* IRS_RESTRICT scratch) {
  // init nonce + counter
  const auto block_size = this->block_size();
  std::memmove(scratch, _iv.c_str(), block_size);
  auto* begin = scratch;
  WriteLE<uint64_t>(_counter_base + block_index, begin);

  // encrypt nonce + counter
  if (!_cipher->Encrypt(scratch)) {
    return false;
  }

  // XOR data with ciphertext
  for (size_t i = 0; i < block_size; ++i) {
    data[i] ^= scratch[i];
  }

  return true;
}

bool CtrEncryption::create_header(std::string_view filename,
                                  byte_type* header) {
  SDB_ASSERT(header);

  const auto block_size = _cipher->block_size();

  if (!block_size) {
    SDB_ERROR(
      IRESEARCH,
      absl::StrCat(
        "failed to initialize encryption header with block of size 0, path '",
        filename, "'"));

    return false;
  }

  const auto header_length = this->header_length();

  if (header_length < kMinHeaderLength) {
    SDB_ERROR(IRESEARCH,
              absl::StrCat("failed to initialize encryption header of size ",
                           header_length, ", need at least ", kMinHeaderLength,
                           ", path '", filename, "'"));

    return false;
  }

  if (header_length < 2 * block_size) {
    SDB_ERROR(IRESEARCH,
              absl::StrCat("failed to initialize encryption header of size ",
                           header_length, ", need at least ", 2 * block_size,
                           ", path '", filename, "'"));

    return false;
  }

  const auto duration = std::chrono::system_clock::now().time_since_epoch();
  const auto millis =
    std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
  ::srand(uint32_t(millis));

  std::for_each(header, header + header_length, [](byte_type& b) {
    b = static_cast<byte_type>(::rand() & 0xFF);
  });

  uint64_t base_counter;
  bytes_view iv;
  DecodeCtrHeader(bytes_view(header, header_length), block_size, base_counter,
                  iv);

  // encrypt header starting from 2nd block
  CtrCipherStream stream(*_cipher, iv, base_counter);
  if (!stream.Encrypt(0, header + 2 * block_size,
                      header_length - 2 * block_size)) {
    SDB_ERROR(IRESEARCH,
              absl::StrCat("failed to encrypt header, path '", filename, "'"));

    return false;
  }

  return true;
}

Encryption::Stream::ptr CtrEncryption::create_stream(std::string_view filename,
                                                     byte_type* header) {
  SDB_ASSERT(header);

  const auto block_size = _cipher->block_size();

  if (!block_size) {
    SDB_ERROR(
      IRESEARCH,
      absl::StrCat(
        "failed to instantiate encryption stream with block of size 0, path '",
        filename, "'"));

    return nullptr;
  }

  const auto header_length = this->header_length();

  if (header_length < kMinHeaderLength) {
    SDB_ERROR(IRESEARCH,
              absl::StrCat(
                "failed to instantiate encryption stream with header of size ",
                header_length, ", need at least ", kMinHeaderLength, ", path '",
                filename, "'"));

    return nullptr;
  }

  if (header_length < 2 * block_size) {
    SDB_ERROR(IRESEARCH,
              absl::StrCat(
                "failed to instantiate encryption stream with header of size ",
                header_length, ", need at least ", 2 * block_size, ", path '",
                filename, "'"));

    return nullptr;
  }

  uint64_t base_counter;
  bytes_view iv;
  DecodeCtrHeader(bytes_view(header, header_length), block_size, base_counter,
                  iv);

  // decrypt the encrypted part of the header
  CtrCipherStream stream(*_cipher, iv, base_counter);
  if (!stream.Decrypt(0, header + 2 * block_size,
                      header_length - 2 * block_size)) {
    SDB_ERROR(
      IRESEARCH,
      absl::StrCat("failed to decrypt encryption header for instantiation of "
                   "encryption stream, path '",
                   filename, "'"));

    return nullptr;
  }

  return std::make_unique<CtrCipherStream>(
    *_cipher, bytes_view(iv.data(), block_size), base_counter);
}

}  // namespace irs
