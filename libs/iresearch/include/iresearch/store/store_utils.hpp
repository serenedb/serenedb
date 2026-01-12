////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///

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

#include "basics/bit_utils.hpp"
#include "basics/number_utils.h"
#include "basics/shared.hpp"
#include "basics/std.hpp"
#include "data_input.hpp"
#include "data_output.hpp"
#include "directory.hpp"
#include "iresearch/utils/attributes.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/numeric_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

template<typename StringType,
         typename TraitsType = typename StringType::traits_type>
StringType ToString(const byte_type* begin) {
  typedef typename TraitsType::char_type char_type;

  const auto size = irs::vread<uint32_t>(begin);

  return StringType(reinterpret_cast<const char_type*>(begin), size);
}

struct EnumHash {
  template<typename T>
  size_t operator()(T value) const {
    static_assert(std::is_enum_v<T>);
    return static_cast<std::underlying_type_t<T>>(value);
  }
};

IRS_FORCE_INLINE void WriteZV32(DataOutput& out, int32_t v) {
  out.WriteV32(sdb::ZigZagEncode32(v));
}

inline int32_t ReadZV32(DataInput& in) {
  return sdb::ZigZagDecode32(in.ReadV32());
}

IRS_FORCE_INLINE void WriteZV64(DataOutput& out, int64_t v) {
  out.WriteV64(sdb::ZigZagEncode64(v));
}

inline int64_t ReadZV64(DataInput& in) {
  return sdb::ZigZagDecode64(in.ReadV64());
}

IRS_FORCE_INLINE void WriteStr(DataOutput& out, const char* s, size_t len) {
  SDB_ASSERT(len < std::numeric_limits<uint32_t>::max());
  out.WriteV32(static_cast<uint32_t>(len));
  out.WriteBytes(reinterpret_cast<const byte_type*>(s), len);
}

IRS_FORCE_INLINE void WriteStr(DataOutput& out, const byte_type* s,
                               size_t len) {
  SDB_ASSERT(len < std::numeric_limits<uint32_t>::max());
  out.WriteV32(static_cast<uint32_t>(len));
  out.WriteBytes(s, len);
}

template<typename StringType>
IRS_FORCE_INLINE void WriteStr(DataOutput& out, const StringType& str) {
  WriteStr(out, str.data(), str.size());
}

template<typename StringType>
inline StringType ReadString(DataInput& in) {
  const size_t len = in.ReadV32();

  StringType str(len, 0);
  [[maybe_unused]] const auto read =
    in.ReadBytes(reinterpret_cast<byte_type*>(str.data()), str.size());
  SDB_ASSERT(read == str.size());
  return str;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief write to 'out' array of data pointed by 'value' of length 'size'
/// @return bytes written
////////////////////////////////////////////////////////////////////////////////
template<typename OutputIterator, typename T>
size_t WriteBytes(OutputIterator& out, const T* value, size_t size) {
  auto* data = reinterpret_cast<const byte_type*>(value);

  size = sizeof(T) * size;

  // write data out byte-by-byte
  for (auto i = size; i; --i) {
    *out = *data;
    ++out;
    ++data;
  }

  return size;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief write to 'out' raw byte representation of data in 'value'
/// @return bytes written
////////////////////////////////////////////////////////////////////////////////
template<typename OutputIterator, typename T>
size_t WriteBytes(OutputIterator& out, const T& value) {
  return WriteBytes(out, &value, 1);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief read a value of the specified type from 'in'
////////////////////////////////////////////////////////////////////////////////
template<typename T>
T& ReadRef(const byte_type*& in) {
  auto& data = reinterpret_cast<T&>(*in);

  in += sizeof(T);  // increment past value

  return data;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief read an array of the specified type and length of 'size' from 'in'
////////////////////////////////////////////////////////////////////////////////
template<typename T>
T* ReadRef(const byte_type*& in, size_t size) {
  auto* data = reinterpret_cast<T*>(&(*in));

  in += sizeof(T) * size;  // increment past value

  return data;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief read a string + size into a value of type 'StringType' from 'in'
////////////////////////////////////////////////////////////////////////////////
template<typename StringType,
         typename TraitsType = typename StringType::traits_type>
StringType VReadString(const byte_type*& in) {
  typedef typename TraitsType::char_type char_type;
  const auto size = vread<uint64_t>(in);

  return StringType(ReadRef<const char_type>(in, size), size);
}

IRS_FORCE_INLINE uint64_t ShiftPack64(uint64_t val, bool b) noexcept {
  SDB_ASSERT(val <= UINT64_C(0x7FFFFFFFFFFFFFFF));
  return (val << 1) | uint64_t(b);
}

IRS_FORCE_INLINE uint32_t ShiftPack32(uint32_t val, bool b) noexcept {
  SDB_ASSERT(val <= UINT32_C(0x7FFFFFFF));
  return (val << 1) | uint32_t(b);
}

template<typename T = bool, typename U = uint64_t>
IRS_FORCE_INLINE T ShiftUnpack64(uint64_t in, U& out) noexcept {
  out = static_cast<U>(in >> 1);
  return static_cast<T>(in & 1);
}

template<typename T = bool, typename U = uint32_t>
IRS_FORCE_INLINE T ShiftUnpack32(uint32_t in, U& out) noexcept {
  out = static_cast<U>(in >> 1);
  return static_cast<T>(in & 1);
}

class BytesViewInput : public IndexInput {
 public:
  BytesViewInput() = default;
  explicit BytesViewInput(bytes_view data) noexcept
    : _data(data), _pos(_data.data()) {}

  void Skip(size_t size) noexcept final {
    SDB_ASSERT(_pos + size <= _data.data() + _data.size());
    _pos += size;
  }

  void Seek(size_t pos) noexcept override {
    SDB_ASSERT(_data.data() + pos <= _data.data() + _data.size());
    _pos = _data.data() + pos;
  }

  uint64_t Position() const noexcept override {
    return std::distance(_data.data(), _pos);
  }

  uint64_t Length() const noexcept final { return _data.size(); }

  bool IsEOF() const noexcept final {
    return _pos >= _data.data() + _data.size();
  }

  byte_type ReadByte() noexcept final {
    SDB_ASSERT(_pos < _data.data() + _data.size());
    return *_pos++;
  }

  const byte_type* ReadBuffer(size_t offset, size_t size,
                              BufferHint /*hint*/) noexcept override {
    const auto begin = _data.data() + offset;
    const auto end = begin + size;

    if (end <= _data.data() + _data.size()) {
      _pos = end;
      return begin;
    }

    return nullptr;
  }

  const byte_type* ReadBuffer(size_t size, BufferHint /*hint*/) noexcept final {
    const auto* pos = _pos + size;

    if (pos <= _data.data() + _data.size()) {
      std::swap(pos, _pos);
      return pos;
    }

    return nullptr;
  }

  size_t ReadBytes(byte_type* b, size_t size) noexcept final;

  size_t ReadBytes(size_t offset, byte_type* b, size_t size) noexcept override;

  // append to buf
  void ReadBytes(bstring& buf, size_t size);

  void reset(const byte_type* data, size_t size) noexcept {
    _data = bytes_view(data, size);
    _pos = data;
  }

  void reset(bytes_view ref) noexcept { reset(ref.data(), ref.size()); }

  ptr Dup() const override { return std::make_unique<BytesViewInput>(*this); }

  ptr Reopen() const override { return Dup(); }

  int16_t ReadI16() noexcept final { return irs::read<uint16_t>(_pos); }

  int32_t ReadI32() noexcept final { return irs::read<uint32_t>(_pos); }

  int64_t ReadI64() noexcept final { return irs::read<uint64_t>(_pos); }

  uint64_t ReadV64() noexcept final { return irs::vread<uint64_t>(_pos); }

  uint32_t ReadV32() noexcept final { return irs::vread<uint32_t>(_pos); }

  uint32_t Checksum(size_t offset) const override;

 private:
  bytes_view _data;
  const byte_type* _pos{_data.data()};
};

// same as bytes_view_input but with support of adress remapping
// usable when original data offses needs to be persistent
// NOTE: remapped data blocks may have gaps but should not overlap!
class RemappedBytesViewInput : public BytesViewInput {
 public:
  using mapping_value = std::pair<size_t, size_t>;
  using mapping = std::vector<mapping_value>;

  explicit RemappedBytesViewInput(const bytes_view& data, mapping&& mapping)
    : BytesViewInput(data), _mapping{std::move(mapping)} {
    absl::c_sort(_mapping, [](const auto& lhs, const auto& rhs) {
      return lhs.first < rhs.first;
    });
  }

  RemappedBytesViewInput(const RemappedBytesViewInput& other)
    : BytesViewInput(other), _mapping{other._mapping} {}

  uint32_t Checksum(size_t offset) const final {
    return BytesViewInput::Checksum(src_to_internal(offset));
  }

  void Seek(size_t pos) noexcept final {
    BytesViewInput::Seek(src_to_internal(pos));
  }

  uint64_t Position() const noexcept final;

  ptr Dup() const final {
    return std::make_unique<RemappedBytesViewInput>(*this);
  }

  const byte_type* ReadBuffer(size_t offset, size_t size,
                              BufferHint hint) noexcept final {
    return BytesViewInput::ReadBuffer(src_to_internal(offset), size, hint);
  }

  using BytesViewInput::ReadBuffer;
  using BytesViewInput::ReadBytes;

  size_t ReadBytes(size_t offset, byte_type* b, size_t size) noexcept final {
    return BytesViewInput::ReadBytes(src_to_internal(offset), b, size);
  }

 private:
  size_t src_to_internal(size_t t) const noexcept;

  mapping _mapping;
};

namespace encode::delta {

template<typename Iterator>
inline void Decode(Iterator begin, Iterator end) {
  SDB_ASSERT(std::distance(begin, end) > 0);

  typedef typename std::iterator_traits<Iterator>::value_type value_type;
  const auto second = begin + 1;

  std::transform(second, end, begin, second, std::plus<value_type>());
}

template<typename Iterator>
inline void Encode(Iterator begin, Iterator end) {
  SDB_ASSERT(std::distance(begin, end) > 0);

  typedef typename std::iterator_traits<Iterator>::value_type value_type;
  const auto rend = irstd::MakeReverseIterator(begin);
  const auto rbegin = irstd::MakeReverseIterator(end);

  std::transform(
    rbegin + 1, rend, rbegin, rbegin,
    [](const value_type& lhs, const value_type& rhs) { return rhs - lhs; });
}

}  // namespace encode::delta
}  // namespace irs
