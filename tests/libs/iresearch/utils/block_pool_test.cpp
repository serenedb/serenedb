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

#include "basics/misc.hpp"
#include "iresearch/utils/block_pool.hpp"
#include "iresearch/utils/string.hpp"
#include "tests_shared.hpp"

using namespace irs;

template<typename T, size_t BlockSize>
class BlockPoolTest : public TestBase {
 public:
  typedef BlockPool<T, BlockSize> PoolT;
  typedef typename PoolT::block_type BlockType;
  typedef typename PoolT::inserter InserterT;
  typedef typename PoolT::sliced_inserter SlicedInserterT;
  typedef typename PoolT::sliced_reader SlicedReaderT;
  typedef typename PoolT::sliced_greedy_inserter SlicedGreedyInserterT;
  typedef typename PoolT::sliced_greedy_reader SlicedGreedyReaderT;

  void Ctor() {
    ASSERT_EQ(BlockSize, size_t(BlockType::kSize));
    ASSERT_EQ(0, _pool.block_count());
    ASSERT_EQ(0, _pool.size());
    ASSERT_EQ(0, _pool.begin().pool_offset());
    ASSERT_EQ(0, _pool.end().pool_offset());
    ASSERT_EQ(_pool.begin(), _pool.end());

    // check const iterators
    {
      const auto& cpool = _pool;
      ASSERT_EQ(cpool.begin(), cpool.end());
    }
  }

  void NextBufferClear() {
    ASSERT_EQ(BlockSize, size_t(BlockType::kSize));
    ASSERT_EQ(0, _pool.block_count());
    ASSERT_EQ(0, _pool.value_count());
    ASSERT_EQ(_pool.begin(), _pool.end());

    // add buffer
    _pool.alloc_buffer();
    ASSERT_EQ(1, _pool.block_count());
    ASSERT_EQ(BlockSize, _pool.value_count());
    ASSERT_NE(_pool.begin(), _pool.end());

    // add several buffers
    const size_t count = 15;
    _pool.alloc_buffer(count);
    ASSERT_EQ(1 + count, _pool.block_count());
    ASSERT_EQ(BlockSize * (1 + count), _pool.value_count());
    ASSERT_NE(_pool.begin(), _pool.end());

    // clear buffers
    _pool.clear();
    ASSERT_EQ(0, _pool.block_count());
    ASSERT_EQ(0, _pool.value_count());
    ASSERT_EQ(_pool.begin(), _pool.end());
  }

  void WriteRead(uint32_t max, uint32_t step) {
    InserterT w(_pool.begin());

    for (uint32_t i = 0; max; i += step, --max) {
      irs::WriteVarint<uint32_t>(i, w);
    }

    auto it = _pool.begin();

    for (uint32_t i = 0; max; i += step, --max) {
      ASSERT_EQ(i, irs::vread<uint32_t>(it));
    }
  }

  void SlicedWriteRead(uint32_t max, uint32_t step) {
    InserterT ins(_pool.begin());
    ins.alloc_slice();

    SlicedInserterT w(ins, _pool.begin());

    const char* const payload = "payload";
    const auto len = static_cast<uint32_t>(strlen(payload));  // only 8 chars
    std::string slice_data("test_data");

    int count = max;
    for (uint32_t i = 0; count; i += step, --count) {
      irs::WriteVarint<uint32_t>(i, w);

      if (i % 3 == 0) {  // write data within slice
        w.write(reinterpret_cast<const byte_type*>(slice_data.c_str()),
                slice_data.size());
      }

      if (i % 2) {
        ins.write((const irs::byte_type*)payload, len);
      }
    }

    SlicedReaderT r(_pool, 0, w.pool_offset());

    count = max;
    for (uint32_t i = 0; count; i += step, --count) {
      int res = irs::vread<uint32_t>(r);

      EXPECT_EQ(i, res);

      if (i % 3 == 0) {  // read data within slice
        bstring payload(slice_data.size(), 0);

        size_t size = r.read(payload.data(), slice_data.size());
        EXPECT_TRUE(slice_data.size() == size);
        EXPECT_TRUE(memcmp(slice_data.c_str(), payload.data(),
                           std::min(slice_data.size(), size)) == 0);
      }
    }
  }

  void SliceChunkedReadWrite() {
    decltype(_pool.begin().pool_offset()) slice_chain_begin;
    decltype(slice_chain_begin) slice_chain_end;

    bytes_view data0 = ViewCast<byte_type>(std::string_view("first_payload"));
    bytes_view data1 =
      ViewCast<byte_type>(std::string_view("second_payload_1234"));

    // write data
    {
      InserterT ins(_pool.begin());
      // alloc slice chain
      slice_chain_begin = ins.alloc_slice();

      // insert payload
      {
        T payload[500];
        std::fill(payload, payload + sizeof payload, 20);
        ins.write(payload, sizeof payload);
      }

      SlicedInserterT sliced_ins(ins, slice_chain_begin);
      sliced_ins.write(data0.data(),
                       data0.size());  // fill 1st slice & alloc 2nd slice here
      sliced_ins.write(data1.data(),
                       data1.size());  // fill 2st slice & alloc 3nd slice here

      slice_chain_end = sliced_ins.pool_offset();
    }

    // read data
    {
      SlicedReaderT sliced_rdr(_pool, slice_chain_begin, slice_chain_end);

      // read first data
      {
        byte_type read[100]{};
        sliced_rdr.read(read, data0.size());
        ASSERT_EQ(data0, bytes_view(read, data0.size()));
      }

      // read second data
      {
        byte_type read[100]{};
        sliced_rdr.read(read, data1.size());
        ASSERT_EQ(data1, bytes_view(read, data1.size()));
      }
    }
  }

  void AllocSlice() {
    const size_t offset = 5;

    // check slice format
    for (size_t i = 0; i < irs::detail::kLevelMax; ++i) {
      _pool.alloc_buffer();
      InserterT writer(offset + _pool.begin());
      ASSERT_EQ(offset, writer.pool_offset());
      auto p = writer.alloc_slice(i);
      ASSERT_EQ(offset, p);
      ASSERT_EQ(offset + irs::detail::kLevels[i].size, writer.pool_offset());
      auto begin = _pool.seek(p);
      for (size_t j = 0; j < irs::detail::kLevels[i].size - 1; ++j) {
        ASSERT_EQ(0, *begin);
        ++begin;
      }
      ASSERT_EQ(irs::detail::kLevels[i].next, *begin);
      _pool.clear();
    }
  }

  void SliceBetweenBlocks() {
    // add initial block
    _pool.alloc_buffer();

    decltype(_pool.begin().pool_offset()) slice_chain_begin;
    decltype(slice_chain_begin) slice_chain_end;

    // write phase
    {
      // seek to the 1 item before the end of the first block
      auto begin = _pool.seek(BlockSize - 1);  // begin of the slice chain
      InserterT ins(begin);

      // we align slices the way they never overcome block boundaries
      slice_chain_begin = ins.alloc_slice();
      ASSERT_EQ(BlockSize, slice_chain_begin);
      ASSERT_EQ(BlockSize + irs::detail::kLevels[0].size, ins.pool_offset());

      // slice should be initialized
      ASSERT_TRUE(
        std::all_of(_pool.seek(BlockSize),
                    _pool.seek(BlockSize + irs::detail::kLevels[0].size - 1),
                    [](T val) { return 0 == val; }));

      SlicedInserterT sliced_ins(ins, slice_chain_begin);

      // write single value
      sliced_ins = 5;

      // insert payload
      {
        T payload[BlockSize - 5];
        std::fill(payload, payload + sizeof payload, 20);
        ins.write(payload, sizeof payload);
      }

      // write additional values
      sliced_ins = 6;  // will be moved to the 2nd slice
      sliced_ins = 7;  // will be moved to the 2nd slice
      sliced_ins = 8;  // will be moved to the 2nd slice
      sliced_ins = 9;  // here new block will be allocated and value will be
                       // written in 2nd slice
      ASSERT_EQ(2 * BlockSize + 3 + 1, sliced_ins.pool_offset());

      slice_chain_end = sliced_ins.pool_offset();
    }

    // read phase
    {
      SlicedReaderT sliced_rdr(_pool, slice_chain_begin, slice_chain_end);
      ASSERT_EQ(5, *sliced_rdr);
      ++sliced_rdr;
      ASSERT_FALSE(sliced_rdr.eof());
      ASSERT_EQ(6, *sliced_rdr);
      ++sliced_rdr;
      ASSERT_FALSE(sliced_rdr.eof());
      ASSERT_EQ(7, *sliced_rdr);
      ++sliced_rdr;
      ASSERT_FALSE(sliced_rdr.eof());
      ASSERT_EQ(8, *sliced_rdr);
      ++sliced_rdr;
      ASSERT_FALSE(sliced_rdr.eof());
      ASSERT_EQ(9, *sliced_rdr);
      ++sliced_rdr;
      ASSERT_TRUE(sliced_rdr.eof());
      ASSERT_EQ(slice_chain_end, sliced_rdr.pool_offset());
    }
  }

  void AllocGreedySlice() {
    const size_t offset = 5;

    // check slice format
    // level 0 makes no sense for greedy format
    for (size_t i = 1; i < irs::detail::kLevelMax; ++i) {
      _pool.alloc_buffer();
      InserterT writer(offset + _pool.begin());
      ASSERT_EQ(offset, writer.pool_offset());
      auto p = writer.alloc_greedy_slice(i);
      ASSERT_EQ(offset, p);
      ASSERT_EQ(offset + irs::detail::kLevels[i].size, writer.pool_offset());
      auto begin = _pool.seek(p);
      ASSERT_EQ(i, *begin);
      ++begin;  // slice header
      for (size_t j = 0;
           j < irs::detail::kLevels[i].size - sizeof(uint32_t) - 1; ++j) {
        ASSERT_EQ(0, *begin);
        ++begin;
      }
      // address part
      ASSERT_EQ(irs::detail::kLevels[i].next, *begin);
      ++begin;
      ASSERT_EQ(0, *begin);
      ++begin;
      ASSERT_EQ(0, *begin);
      ++begin;
      ASSERT_EQ(0, *begin);
      _pool.clear();
    }
  }

  void GreedySliceReadWrite() {
    const bytes_view data[]{
      ViewCast<byte_type>(std::string_view("first_payload")),
      ViewCast<byte_type>(std::string_view("second_payload_1234"))};

    std::vector<std::pair<size_t, size_t>> cookies;  // slice_offset + offset

    auto push_cookie = [&cookies](const SlicedGreedyInserterT& writer) {
      cookies.emplace_back(writer.slice_offset(),
                           writer.pool_offset() - writer.slice_offset());
    };

    // write data
    {
      InserterT ins(_pool.begin());
      cookies.emplace_back(ins.alloc_greedy_slice(), 1);  // alloc slice chain

      // insert payload
      {
        T payload[500];
        std::fill(payload, payload + sizeof payload, 20);
        ins.write(payload, sizeof payload);
      }

      SlicedGreedyInserterT sliced_ins(ins, cookies.back().second,
                                       cookies.back().first);
      sliced_ins.write(
        data[0].data(),
        data[0].size());  // fill 1st slice & alloc 2nd slice here
      push_cookie(sliced_ins);
      sliced_ins.write(
        data[1].data(),
        data[1].size());  // fill 2st slice & alloc 3nd slice here

      // insert payload
      {
        T payload[500];
        std::fill(payload, payload + sizeof payload, 20);
        ins.write(payload, sizeof payload);
      }

      for (size_t i = 0; i < 1024; ++i) {
        push_cookie(sliced_ins);
        irs::WriteVarint(i, sliced_ins);
        const auto str = std::to_string(i);
        sliced_ins.write(reinterpret_cast<const irs::byte_type*>(str.c_str()),
                         str.size());
      }
    }

    // read data
    {
      byte_type read[100]{};
      size_t i = cookies.size();

      while (--i > 2) {
        const auto expected = i - 2;
        SlicedGreedyReaderT sliced_rdr(_pool, cookies[i].first,
                                       cookies[i].second);
        ASSERT_EQ(expected, irs::vread<size_t>(sliced_rdr));

        const auto str = std::to_string(expected);
        sliced_rdr.read(read, str.size());

        ASSERT_EQ(
          irs::bytes_view(reinterpret_cast<const byte_type*>(str.c_str()),
                          str.size()),
          irs::bytes_view(read, str.size()));
      }

      while (i--) {
        SlicedGreedyReaderT sliced_rdr(_pool, cookies[i].first,
                                       cookies[i].second);
        sliced_rdr.read(read, data[i].size());
        ASSERT_EQ(data[i], bytes_view(read, data[i].size()));
      }
    }
  }

  void GreedySliceBetweenBlocks() {
    // add initial block
    _pool.alloc_buffer();

    decltype(_pool.begin().pool_offset()) slice_chain_begin;
    decltype(slice_chain_begin) slice_chain_end;

    // write phase
    {
      // seek to the 1 item before the end of the first block
      auto begin = _pool.seek(BlockSize - 1);  // begin of the slice chain
      InserterT ins(begin);

      // we align slices the way they never overcome block boundaries
      slice_chain_begin = ins.alloc_greedy_slice();
      ASSERT_EQ(BlockSize, slice_chain_begin);
      ASSERT_EQ(BlockSize + irs::detail::kLevels[1].size,
                ins.pool_offset());  // level 0 makes no sense for greedy slice

      // slice should be initialized
      ASSERT_TRUE(
        std::all_of(_pool.seek(BlockSize + 1),
                    _pool.seek(BlockSize + irs::detail::kLevels[1].size -
                               sizeof(uint32_t) - 1),
                    [](T val) { return 0 == val; }));

      SlicedGreedyInserterT sliced_ins(ins, slice_chain_begin, 1);

      // write single value
      sliced_ins = 5;

      // insert payload
      {
        T payload[BlockSize - 5];
        std::fill(payload, payload + sizeof payload, 20);
        ins.write(payload, sizeof payload);
      }

      // write additional values
      sliced_ins = 6;   // will be moved to the 2nd slice
      sliced_ins = 7;   // will be moved to the 2nd slice
      sliced_ins = 8;   // will be moved to the 2nd slice
      sliced_ins = 9;   // will be moved to the 2nd slice
      sliced_ins = 10;  // will be moved to the 2nd slice
      sliced_ins = 11;  // will be moved to the 2nd slice
      sliced_ins = 12;  // will be moved to the 2nd slice
      sliced_ins = 13;  // will be moved to the 2nd slice
      ASSERT_EQ(2 * BlockSize + 9 + 1, sliced_ins.pool_offset());

      slice_chain_end = sliced_ins.pool_offset();
    }

    // read phase
    {
      SlicedGreedyReaderT sliced_rdr(_pool, slice_chain_begin, 1);
      ASSERT_EQ(5, *sliced_rdr);
      ++sliced_rdr;
      ASSERT_EQ(6, *sliced_rdr);
      ++sliced_rdr;
      ASSERT_EQ(7, *sliced_rdr);
      ++sliced_rdr;
      ASSERT_EQ(8, *sliced_rdr);
      ++sliced_rdr;
      ASSERT_EQ(9, *sliced_rdr);
      ++sliced_rdr;
      ASSERT_EQ(10, *sliced_rdr);
      ++sliced_rdr;
      ASSERT_EQ(11, *sliced_rdr);
      ++sliced_rdr;
      ASSERT_EQ(12, *sliced_rdr);
      ++sliced_rdr;
      ASSERT_EQ(13, *sliced_rdr);
      ++sliced_rdr;
      ASSERT_EQ(slice_chain_end, sliced_rdr.pool_offset());
    }
  }

 protected:
  irs::BlockPool<T, BlockSize> _pool;
};

typedef BlockPoolTest<irs::byte_type, 32768> ByteBlockPoolTest;

TEST_F(ByteBlockPoolTest, ctor) { Ctor(); }

TEST_F(ByteBlockPoolTest, next_buffer_clear) {
  ByteBlockPoolTest::NextBufferClear();
}

TEST_F(ByteBlockPoolTest, write_read) {
  ByteBlockPoolTest::WriteRead(1000000, 7);
}

TEST_F(ByteBlockPoolTest, sliced_write_read) {
  ByteBlockPoolTest::SlicedWriteRead(1000000, 7);
}

TEST_F(ByteBlockPoolTest, slice_alignment) { SliceBetweenBlocks(); }

TEST_F(ByteBlockPoolTest, slice_alignment_with_reuse) {
  SliceBetweenBlocks();
  SliceBetweenBlocks();  // reuse block_pool from previous run
}

TEST_F(ByteBlockPoolTest, alloc_slice) { AllocSlice(); }

TEST_F(ByteBlockPoolTest, slice_chunked_read_write) { SliceChunkedReadWrite(); }

TEST_F(ByteBlockPoolTest, alloc_greedy_slice) { AllocGreedySlice(); }

TEST_F(ByteBlockPoolTest, greedy_slice_chunked_read_write) {
  GreedySliceReadWrite();
}

TEST_F(ByteBlockPoolTest, greedy_slice_alignment) {
  GreedySliceBetweenBlocks();
}

TEST_F(ByteBlockPoolTest, greedy_slice_alignment_with_reuse) {
  GreedySliceBetweenBlocks();
  GreedySliceBetweenBlocks();  // reuse block_pool from previous run
}
