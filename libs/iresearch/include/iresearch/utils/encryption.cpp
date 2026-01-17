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

#include "encryption.hpp"

#include <absl/strings/str_cat.h>

#include "basics/crc.hpp"
#include "basics/misc.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/directory_attributes.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs {

bool Encrypt(std::string_view filename, IndexOutput& out, Encryption* enc,
             bstring& header, Encryption::Stream::ptr& cipher) {
  header.resize(enc ? enc->header_length() : 0);

  if (header.empty()) {
    // no encryption
    WriteStr(out, header);
    return false;
  }

  SDB_ASSERT(enc);

  if (!enc->create_header(filename, header.data())) {
    throw IndexError{absl::StrCat(
      "failed to initialize encryption header, path '", filename, "'")};
  }

  // header is encrypted here
  WriteStr(out, header);

  cipher = enc->create_stream(filename, header.data());

  if (!cipher) {
    throw IndexError{absl::StrCat(
      "Failed to instantiate encryption stream, path '", filename, "'")};
  }

  if (!cipher->block_size()) {
    throw IndexError{absl::StrCat(
      "Failed to instantiate encryption stream with block of size 0, path '",
      filename, "'")};
  }

  // header is decrypted here, write checksum
  Crc32c crc;
  crc.process_bytes(header.c_str(), header.size());
  out.WriteV64(crc.checksum());

  return true;
}

bool Decrypt(std::string_view filename, IndexInput& in, Encryption* enc,
             Encryption::Stream::ptr& cipher) {
  auto header = irs::ReadString<bstring>(in);

  if (header.empty()) {
    // no encryption
    return false;
  }

  if (!enc) {
    throw IndexError{absl::StrCat(
      "Failed to open encrypted file without cipher, path '", filename, "'")};
  }

  if (header.size() != enc->header_length()) {
    throw IndexError{
      absl::StrCat("failed to open encrypted file, expect encryption header of "
                   "size ",
                   enc->header_length(), ", got ", header.size(), ", path '",
                   filename, "'")};
  }

  cipher = enc->create_stream(filename, header.data());

  if (!cipher) {
    throw IndexError{
      absl::StrCat("Failed to open encrypted file, path '", filename, "'")};
  }

  const auto block_size = cipher->block_size();

  if (!block_size) {
    throw IndexError{
      absl::StrCat("Invalid block size 0 specified for encrypted file, path '",
                   filename, "'")};
  }

  // header is decrypted here, check checksum
  Crc32c crc;
  crc.process_bytes(header.c_str(), header.size());

  if (crc.checksum() != in.ReadV64()) {
    throw IndexError{
      absl::StrCat("Invalid ecryption header, path '", filename, "'")};
  }

  return true;
}

EncryptedOutput::EncryptedOutput(IndexOutput& out, Encryption::Stream& cipher,
                                 size_t num_blocks)
  : IndexOutput{nullptr, nullptr}, _out{&out}, _cipher{&cipher} {
  const auto block_size = cipher.block_size();
  SDB_ASSERT(block_size);
  // TODO(mbkkt) max probably needed only for tests
  const auto buf_size = block_size * std::max<size_t>(1, num_blocks);
  _buf = std::allocator<byte_type>{}.allocate(buf_size);
  _pos = _buf;
  _end = _buf + buf_size;
}

EncryptedOutput::EncryptedOutput(IndexOutput::ptr&& out,
                                 Encryption::Stream& cipher, size_t num_blocks)
  : EncryptedOutput{*out, cipher, num_blocks} {
  SDB_ASSERT(out);
  _managed_out = std::move(out);
}

EncryptedOutput::~EncryptedOutput() {
  std::allocator<byte_type>{}.deallocate(_buf, BufferSize());
}

void EncryptedOutput::FlushBuffer() {
  const auto len = Length();
  // TODO(mbkkt) we probably can make out_->Position() == offset_,
  // but needs to adjust tests
  // TODO(mbkkt) with encryption::stream interface for rocksdb we allocate
  // buffer inside encrypt and encrypt to it, and then copy it to our own
  // buffer, this is mess, we need to change interace
  if (!_cipher->Encrypt(_out->Position(), _buf, len)) {
    throw IoError{absl::StrCat("Buffer size ", len,
                               " is not multiple of cipher block size ",
                               _cipher->block_size())};
  }
  _out->WriteBytes(_buf, len);
  _offset += len;
  _pos = _buf;
}

void EncryptedOutput::WriteDirect(const byte_type* b, size_t len) {
  Write(*this, b, len);
}

EncryptedInput::EncryptedInput(IndexInput& in, Encryption::Stream& cipher,
                               size_t num_buffers, size_t padding /* = 0*/)
  : _buf_size(cipher.block_size() * std::max(size_t(1), num_buffers)),
    _buf(std::make_unique<byte_type[]>(_buf_size)),
    _in(&in),
    _cipher(&cipher),
    _start(_in->Position()),
    _length(_in->Length() - _start - padding) {
  SDB_ASSERT(cipher.block_size() && _buf_size);
  SDB_ASSERT(_in && _in->Length() >= _in->Position() + padding);
  BufferedIndexInput::reset(_buf.get(), _buf_size, 0);
}

EncryptedInput::EncryptedInput(IndexInput::ptr&& in, Encryption::Stream& cipher,
                               size_t num_buffers, size_t padding /* = 0*/)
  : EncryptedInput(*in, cipher, num_buffers, padding) {
  _managed_in = std::move(in);
}

EncryptedInput::EncryptedInput(const EncryptedInput& rhs,
                               IndexInput::ptr&& in) noexcept
  : _buf_size(rhs._buf_size),
    _buf(std::make_unique<byte_type[]>(_buf_size)),
    _managed_in(std::move(in)),
    _in(_managed_in.get()),
    _cipher(rhs._cipher),
    _start(rhs._start),
    _length(rhs._length) {
  SDB_ASSERT(_cipher->block_size());
  BufferedIndexInput::reset(_buf.get(), _buf_size, rhs.Position());
}

uint32_t EncryptedInput::Checksum(size_t offset) const {
  const auto begin = Position();
  const auto end = (std::min)(begin + offset, this->Length());

  Finally restore_position = [begin, this]() noexcept {
    // FIXME make me noexcept as I'm begin called from within ~finally()
    const_cast<EncryptedInput*>(this)->SeekInternal(begin);
  };

  const_cast<EncryptedInput*>(this)->SeekInternal(begin);

  Crc32c crc;
  byte_type buf[kDefaultEncryptionBufferSize];

  for (auto pos = begin; pos < end;) {
    const auto to_read = (std::min)(end - pos, sizeof buf);
    pos += const_cast<EncryptedInput*>(this)->ReadInternal(buf, to_read);
    crc.process_bytes(buf, to_read);
  }

  return crc.checksum();
}

IndexInput::ptr EncryptedInput::Dup() const {
  auto dup = _in->Dup();

  if (!dup) {
    throw IoError{
      absl::StrCat("Failed to duplicate input file, error: ", errno)};
  }

  return IndexInput::ptr{new EncryptedInput{*this, std::move(dup)}};
}

IndexInput::ptr EncryptedInput::Reopen() const {
  auto reopened = _in->Reopen();

  if (!reopened) {
    throw IoError(absl::StrCat("Failed to reopen input file, error: ", errno));
  }

  return IndexInput::ptr{new EncryptedInput{*this, std::move(reopened)}};
}

void EncryptedInput::SeekInternal(size_t pos) {
  pos += _start;

  if (pos != _in->Position()) {
    _in->Seek(pos);
  }
}

size_t EncryptedInput::ReadInternal(byte_type* b, size_t count) {
  const auto offset = _in->Position();

  const auto read = _in->ReadBytes(b, count);

  if (!_cipher->Decrypt(offset, b, read)) {
    throw IoError{absl::StrCat("Buffer size ", read,
                               " is not multiple of cipher block size ",
                               _cipher->block_size())};
  }

  return read;
}

}  // namespace irs
