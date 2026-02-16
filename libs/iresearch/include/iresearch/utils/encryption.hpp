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

#include "basics/math_utils.hpp"
#include "basics/noncopyable.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/directory_attributes.hpp"

namespace irs {

/// @brief initialize an encryption header and create corresponding cipher
/// stream
/// @returns true if cipher stream was initialized, false if encryption is not
///          appliclabe
/// @throws index_error in case of error on header or stream creation
bool Encrypt(std::string_view filename, IndexOutput& out, Encryption* enc,
             bstring& header, Encryption::Stream::ptr& cipher);

/// @brief create corresponding cipher stream from a specified encryption header
/// @returns true if cipher stream was initialized, false if encryption is not
///          appliclabe
/// @throws index_error in case of error on cipher stream creation
bool Decrypt(std::string_view filename, IndexInput& in, Encryption* enc,
             Encryption::Stream::ptr& cipher);

////////////////////////////////////////////////////////////////////////////////
///// @brief reasonable default value for a buffer serving encryption
////////////////////////////////////////////////////////////////////////////////
inline constexpr size_t kDefaultEncryptionBufferSize = 1024;

class EncryptedOutput final : public IndexOutput, util::Noncopyable {
 public:
  EncryptedOutput(IndexOutput& out, Encryption::Stream& cipher,
                  size_t num_blocks);

  EncryptedOutput(IndexOutput::ptr&& out, Encryption::Stream& cipher,
                  size_t num_blocks);

  ~EncryptedOutput() final;

  IndexOutput::ptr Release() noexcept { return std::move(_managed_out); }

  size_t BufferSize() const noexcept { return _end - _buf; }

  const IndexOutput& Stream() const noexcept {
    SDB_ASSERT(_out);
    return *_out;
  }

  void Flush() final {
    FlushBuffer();  // TODO(mbkkt) maybe check buffer not empty?
  }

  uint32_t Checksum() final {
    // We don't calculate checksum for unecrypted data
    // Because it will slow down writes
    // TODO(mbkkt) missed Flush()?!
    return _out->Checksum();
  }

  uint64_t CloseImpl() final {
    Flush();
    const auto size = Position();

    _managed_out.reset();
    _out = nullptr;
    _offset = 0;

    return size;
  }

 private:
  friend class BufferedOutput;

  void FlushBuffer();

  void WriteDirect(const byte_type* b, size_t len) final;

  IndexOutput::ptr _managed_out;
  IndexOutput* _out;
  Encryption::Stream* _cipher;
};

class EncryptedInput : public BufferedIndexInput, util::Noncopyable {
 public:
  EncryptedInput(IndexInput& in, Encryption::Stream& cipher, size_t buf_size,
                 size_t padding = 0);

  EncryptedInput(IndexInput::ptr&& in, Encryption::Stream& cipher,
                 size_t buf_size, size_t padding = 0);

  uint64_t Length() const noexcept final { return _length; }
  bool IsEOF() const noexcept final { return Position() >= Length(); }

  IndexInput::ptr Dup() const final;
  IndexInput::ptr Reopen() const final;

  uint32_t Checksum(uint64_t offset) const final;

  size_t BufferSize() const noexcept { return _buf_size; }

  const IndexInput& stream() const noexcept { return *_in; }

  IndexInput::ptr release() noexcept { return std::move(_managed_in); }

 protected:
  void SeekInternal(uint64_t pos) final;

  size_t ReadInternal(byte_type* b, size_t count) final;

  EncryptedInput(const EncryptedInput& rhs, IndexInput::ptr&& in) noexcept;

 private:
  size_t _buf_size;
  std::unique_ptr<byte_type[]> _buf;
  IndexInput::ptr _managed_in;
  IndexInput* _in;
  Encryption::Stream* _cipher;
  const uint64_t _start;
  const uint64_t _length;
};

}  // namespace irs
