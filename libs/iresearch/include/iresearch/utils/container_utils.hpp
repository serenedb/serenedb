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

#pragma once

#include <array>
#include <iresearch/types.hpp>
#include <memory>

#include "basics/memory.hpp"
#include "basics/misc.hpp"
#include "basics/noncopyable.hpp"
#include "basics/resource_manager.hpp"
#include "basics/shared.hpp"

namespace irs::container_utils {

//////////////////////////////////////////////////////////////////////////////
/// @brief compute individual sizes and offsets of exponentially sized buckets
/// @param NumBuckets the number of bucket descriptions to generate
/// @param SkipBits 2^SkipBits is the size of the first bucket, consequently
///        the number of bits from a 'position' value to place into 1st bucket
//////////////////////////////////////////////////////////////////////////////
struct BucketInfo {
  size_t offset = 0;
  size_t size = 0;
};

template<size_t NumBuckets, size_t SkipBits>
struct BucketMeta {
  static_assert(NumBuckets > 0);

  constexpr BucketMeta() noexcept {
    _buckets[0].size = size_t{1} << SkipBits;
    _buckets[0].offset = 0;

    for (size_t i = 1; i < NumBuckets; ++i) {
      _buckets[i].offset = _buckets[i - 1].offset + _buckets[i - 1].size;
      _buckets[i].size = _buckets[i - 1].size << size_t{1};
    }
  }

  constexpr BucketInfo operator[](size_t i) const noexcept {
    return _buckets[i];
  }

 private:
  std::array<BucketInfo, NumBuckets> _buckets;
};

//////////////////////////////////////////////////////////////////////////////
/// @brief a function to calculate the bucket offset of the reqested position
///        for exponentially sized buckets, e.g. as in BucketMeta
/// @param SkipBits 2^SkipBits is the size of the first bucket, consequently
///        the number of bits from a 'position' value to place into 1st bucket
//////////////////////////////////////////////////////////////////////////////
template<size_t SkipBits>
size_t ComputeBucketOffset(size_t position) noexcept {
  // 63 == 64 bits per size_t - 1 for allignment,
  // +1 == align first value to start of bucket
  return size_t{63} - std::countl_zero((position >> SkipBits) + size_t{1});
}

class RawBlockVectorBase : private util::Noncopyable {
 public:
  // TODO(mbkkt) We could store only data pointer,
  // everything else could be computed from position in the buffers_
  struct BufferT {
    // sum of bucket sizes up to but excluding this buffer
    size_t offset{};
    // data is just pointer to speedup vector reallocation
    byte_type* data{};  // pointer at the actual data
    size_t size{};      // total buffer size
  };

  explicit RawBlockVectorBase(IResourceManager& rm) noexcept
    : _alloc{rm}, _buffers{{rm}} {}

  RawBlockVectorBase(RawBlockVectorBase&& rhs) noexcept = default;
  RawBlockVectorBase& operator=(RawBlockVectorBase&& rhs) = delete;

  ~RawBlockVectorBase() { clear(); }

  IRS_FORCE_INLINE size_t buffer_count() const noexcept {
    return _buffers.size();
  }

  IRS_FORCE_INLINE bool empty() const noexcept { return _buffers.empty(); }

  IRS_FORCE_INLINE void clear() noexcept {
    for (auto& buffer : _buffers) {
      _alloc.deallocate(buffer.data, buffer.size);
    }
    _buffers.clear();
  }

  IRS_FORCE_INLINE const BufferT& get_buffer(size_t i) const noexcept {
    return _buffers[i];
  }

#ifdef SDB_GTEST
  void pop_buffer() noexcept {
    const auto& bucket = _buffers.back();
    _alloc.deallocate(bucket.data, bucket.size);
    _buffers.pop_back();
  }
#endif

 protected:
  // TODO(mbkkt) Maybe make it batched for push, clear and dtor
  ManagedTypedAllocator<byte_type> _alloc;
  ManagedVector<BufferT> _buffers;
};

//////////////////////////////////////////////////////////////////////////////
/// @brief a container allowing raw access to internal storage, and
///        using an allocation strategy similar to an std::deque
//////////////////////////////////////////////////////////////////////////////
template<size_t NumBuckets, size_t SkipBits>
class RawBlockVector : public RawBlockVectorBase {
 public:
  static constexpr BucketMeta<NumBuckets, SkipBits> kMeta{};
  static constexpr BucketInfo kLast = kMeta[NumBuckets - 1];

  explicit RawBlockVector(IResourceManager& rm) noexcept
    : RawBlockVectorBase{rm} {}

  IRS_FORCE_INLINE size_t buffer_offset(size_t position) const noexcept {
    return position < kLast.offset
             ? ComputeBucketOffset<SkipBits>(position)
             : (NumBuckets - 1 + (position - kLast.offset) / kLast.size);
  }

  const BufferT& push_buffer() {
    auto v = CreateValue();
    Finally f = [&]() noexcept {
      if (v.data) {
        _alloc.deallocate(v.data, v.size);
      }
    };
    const auto& buffer = _buffers.emplace_back(v);
    v.data = nullptr;
    return buffer;
  }

 private:
  IRS_FORCE_INLINE BufferT CreateValue() {
    if (_buffers.size() < NumBuckets) {
      const auto& bucket = kMeta[_buffers.size()];
      return {bucket.offset, _alloc.allocate(bucket.size), bucket.size};
    }
    const auto& bucket = _buffers.back();
    SDB_ASSERT(bucket.size == kLast.size);
    return {bucket.offset + kLast.size, _alloc.allocate(kLast.size),
            kLast.size};
  }
};

}  // namespace irs::container_utils
