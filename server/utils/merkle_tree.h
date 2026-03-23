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

#include <absl/synchronization/mutex.h>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace vpack {

class Builder;
class Slice;

}  // namespace vpack
namespace sdb {
namespace containers {

class HashProvider {
 public:
  virtual ~HashProvider() = default;
  virtual uint64_t hash(uint64_t input) const = 0;
};

class FnvHashProvider : public HashProvider {
 public:
  uint64_t hash(uint64_t input) const override;
};

class MerkleTreeBase {
 public:
  enum class BinaryFormat : char {
    // Uncompressed data (all buckets, only use for testing!)
    Testing = '2',
    // Only contains non-empty buckets (efficient for sparse trees)
    Uncompressed = '3',
    // LZ4-compressed data of populated buckets
    // TODO(mbkkt) I think LZ4 not suited here and we should use zstd,
    // because it has entropy encoder
    // TODO(mbkkt) Also there's issue with streaming compression API
    // In general is not oblivious is it ok have shard size == window size
    // And it seems like our allocation pattern doesn't suite.
    // Also maybe in future we decide to deserialize nodes lazy
    LZ4 = '4',
    // placeholder for optimal format, will determine the "best"
    // format automatically, based on heuristics
    Optimal = 'z',
  };

  struct Node {
    uint64_t count;
    uint64_t hash;

    void toVPack(vpack::Builder& output) const;

    bool empty() const noexcept { return count == 0 && hash == 0; }

    bool operator==(const Node& other) const noexcept;
  };
  static constexpr uint64_t kNodeSize = sizeof(Node);
  static_assert(kNodeSize == 16, "Node size assumptions invalid.");

  // an empty dummy node with count=0, hash=0, shared and read-only
  static constexpr Node kEmptyNode{0, 0};

  struct Meta {
    uint64_t range_min;
    uint64_t range_max;
    uint64_t depth;
    uint64_t initial_range_min;
    Node summary;

    // used for older versions. unfortunately needed
    struct Padding {
      uint64_t p0;
      uint64_t p1;
    } padding;

    void serialize(std::string& output, bool add_padding) const;
  };

  static_assert(sizeof(Meta) == 64, "Meta size assumptions invalid.");
  static_assert(sizeof(Meta::Padding) == 16,
                "Meta padding size assumptions invalid.");
  static constexpr uint64_t kMetaSize = sizeof(Meta);

  // size of each shard, in bytes.
  // note: trees with a small depth may only have a single shard which is
  // smaller than this value
  static constexpr uint64_t kShardSize = (1 << 16);
  static_assert(std::has_single_bit(kShardSize));
  static_assert(kShardSize % kNodeSize == 0);

  struct Data {
    using ShardType = std::unique_ptr<Node[]>;

    Meta meta;
    std::vector<ShardType> shards;
    size_t memory_usage = 0;

    void clear() {
      shards.clear();
      memory_usage = 0;
      meta.summary = {0, 0};
    }

    void ensureShard(uint64_t shard, uint64_t shard_size);

    static ShardType buildShard(uint64_t shard_size);
  };
};

// 8 children per internal node
template<typename Hasher, uint64_t BranchingBits = 3>
class MerkleTree : public MerkleTreeBase {
  // A MerkleTree has three parameters which define its semantics:
  //  - rangeMin: lower bound (inclusive) for _rev values it can take
  //  - rangeMax: upper bound (exclusive) for _rev values it can take
  //  - depth: depth of the tree (root plus so many levels below)
  // We call rangeMin-rangeMax the "width" of the tree.
  //
  // We do no longer grow trees in depth, since this would mean a complete
  // rehash or a large growth of width, which can lead to integer overflow,
  // which we must avoid.
  //
  // However, we do grow the width of a tree as needed. Originally, we
  // only grew to the right and rangeMin was constant over the lifetime
  // of the tree. This turned out to be not good enough, since we cannot
  // estimate the smallest _rev value a collection will ever receive
  // (for example, in DC2DC we might need to replicate old data into a
  // newly created collection). Therefore, we can now also grow the width
  // to the left by decreasing rangeMin.
  //
  // Unfortunately, we can only compare two different MerkleTrees, if
  // the difference of their rangeMin values is a multiple of the number
  // of _rev values in a leaf node, which is
  //   (rangeMax-rangeMin)/(1ULL << (BranchingBits*depth)),
  // since (1ULL << (BranchingBits*depth)) is the number of leaves. Therefore
  // we must ensure that trees of replicas of shards which we must be
  // able to compare, remain compatible. Therefore, we pick a magic
  // constant M as the initial value of rangeMin, which is the same for
  // all replicas of a shard, and then maintain the following invariants
  // at all times, for all changes to rangeMin and rangeMax we ever do:
  //
  // 1. rangeMax-rangeMin is a power of two and is a multiple of
  //    the number of leaves in the tree, which is
  //      1ULL << (BranchingBits*depth)
  //    That is, we can only ever grow the width by factors of 2.
  // 2. M - rangeMin is divisible by
  //      (rangeMax-rangeMin)/(1ULL << (BranchingBits*depth))
  //
  // Condition 1. ensures that each leaf is responsible for the same
  // number of _rev values and that we can always grow rangeMax-rangeMin
  // by a factor of 2 without having to rehash everything.
  // Condition 2. ensures that two trees which have started with the same
  // magic M and have the same width are comparable, since the difference
  // of their rangeMin values will always be divisible by the number
  // given in Condition 2.
  //
  // See methods growLeft and growRight for an explanation how we keep
  // these invariants in place on growth.
 public:
  static constexpr uint64_t allocationSize(uint64_t depth) noexcept {
    // summary node is included in MetaSize
    return kMetaSize + (kNodeSize * nodeCountAtDepth(depth));
  }

  static constexpr uint64_t shardSize(uint64_t depth) noexcept {
    uint64_t shard_size = allocationSize(depth) - kMetaSize;
    return std::min(shard_size, kShardSize);
  }

  /**
   * Calculates the number of nodes at the given depth
   *
   * depth The same depth value used for the calculation
   */
  static constexpr uint64_t nodeCountAtDepth(uint64_t depth) noexcept {
    return static_cast<uint64_t>(1) << (BranchingBits * depth);
  }

  static constexpr uint64_t shardForIndex(uint64_t index) noexcept {
    return kNodeSize * index / kShardSize;
  }

  static constexpr uint64_t shardBaseIndex(uint64_t shard) noexcept {
    return shard * kShardSize / kNodeSize;
  }

  /**
   * Chooses the default range width for a tree of a given depth.
   *
   * Most applications should use either this value or some power-of-two
   * mulitple of this value. The default is chosen so that each leaf bucket
   * initially covers a range of 64 keys.
   *
   * depth The same depth value passed to the constructor
   */
  static uint64_t defaultRange(uint64_t depth);

  /**
   * Construct a tree from a buffer containing a serialized tree
   *
   * buffer      A buffer containing a serialized tree
   * Return A newly allocated tree constructed from the input
   */
  static std::unique_ptr<MerkleTree<Hasher, BranchingBits>> fromBuffer(
    std::string_view buffer);

  /**
   * Construct a tree from a buffer containing an uncompressed tree
   *
   * buffer      A buffer containing an uncompressed tree
   * Return A newly allocated tree constructed from the input
   */
  static std::unique_ptr<MerkleTree<Hasher, BranchingBits>> fromTesting(
    std::string_view buffer);

  /**
   * Construct a tree from a buffer containing a LZ4-compressed tree
   *
   * buffer      A buffer containing a LZ4 compressed tree
   * Return A newly allocated tree constructed from the input
   */
  static std::unique_ptr<MerkleTree<Hasher, BranchingBits>> fromLZ4(
    std::string_view buffer);

  /**
   * Construct a tree from a buffer containing only the populated buckets
   *
   * buffer      A buffer containing a series of populated buckets
   * Return A newly allocated tree constructed from the input
   */
  static std::unique_ptr<MerkleTree<Hasher, BranchingBits>> fromUncompressed(
    std::string_view buffer);

  /**
   * Construct a tree from a portable serialized tree
   *
   * slice A slice containing a serialized tree
   * Return A newly allocated tree constructed from the input
   */
  static std::unique_ptr<MerkleTree<Hasher, BranchingBits>> deserialize(
    vpack::Slice slice);

  /**
   * Construct a Merkle tree of a given depth with a given minimum key
   *
   * depth    The depth of the tree. This determines how much memory The
   *                 tree will consume, and how fine-grained the hash is.
   *                 Constructor will throw if a value less than 2 is specified.
   * rangeMin The minimum key that can be stored in the tree.
   *                 An attempt to insert a smaller key will result
   *                 in a growLeft. See above (magic constant M) for a
   *                 sensible choice of initial rangeMin.
   * rangeMax Must be an offset from rangeMin of a multiple of the
   *                 number of leaf nodes. If 0, it will be  chosen using the
   *                 defaultRange method. This is just an initial value to
   *                 prevent immediate resizing; if a key larger than rangeMax
   *                 is inserted into the tree, it will be dynamically resized
   *                 so that a larger rangeMax is chosen, and adjacent nodes
   *                 merged as necessary (growRight).
   * initialRangeMin The initial value of rangeMin when the tree was
   *                 first created and was still empty.
   * @throws std::invalid_argument  If depth is less than 2
   */
  MerkleTree(uint64_t depth, uint64_t range_min, uint64_t range_max = 0,
             uint64_t initial_range_min = 0);

  ~MerkleTree();

  /**
   * Move assignment operator from pointer
   *
   * other Input tree, intended assignment
   */
  MerkleTree& operator=(
    std::unique_ptr<MerkleTree<Hasher, BranchingBits>>&& other);

  /// base memory usage for struct + dynamic memory usage
  uint64_t memoryUsage() const;

  /// only dynamic memory usage (excluding base memory usage)
  uint64_t dynamicMemoryUsage() const;

  /**
   * Returns the number of hashed keys contained in the tree
   */
  uint64_t count() const;

  /**
   * Returns the hash of all values in the tree, equivalently the root
   *        value
   */
  uint64_t rootValue() const;

  /**
   * Returns the current range of the tree
   */
  std::pair<uint64_t, uint64_t> range() const;

  /**
   * Returns the maximum depth of the tree
   */
  uint64_t depth() const;

  /**
   * Returns the number of bytes allocated for the tree
   */
  uint64_t byteSize() const;

  /**
   * Insert a value into the tree. May trigger a resize.
   *
   * key   The key for the item. If it is less than the minimum specified
   *              at construction, then it will trigger an exception. If it is
   *              greater than the current max, it will trigger a (potentially
   *              expensive) growth operation to ensure the key can be inserted.
   * @throws std::out_of_range  If key is less than rangeMin
   */
  void insert(uint64_t key);

  /**
   * Insert a batch of keys (as values) into the tree. May trigger a
   * resize.
   *
   * keys  The keys to be inserted. Each key will be hashed to generate
   *              a value, then inserted as if by the basic single insertion
   *              method. This batch method is considerably more efficient.
   * @throws std::out_of_range  If key is less than rangeMin
   */
  void insert(const std::vector<uint64_t>& keys);

  /**
   * Remove a value from the tree.
   *
   * key   The key for the item. If it is outside the current tree range,
   *              then it will trigger an exception.
   * @throws std::out_of_range  If key is outside current range
   */
  void remove(uint64_t key);

  /**
   * Remove a batch of keys (as values) from the tree.
   *
   * keys  The keys to be removed. Each key will be hashed to generate
   *              a value, then removed as if by the basic single removal
   *              method. This batch method is considerably more efficient.
   * @throws std::out_of_range  If key is less than rangeMin
   * @throws std::invalid_argument  If remove hits a node with 0 count
   */
  void remove(const std::vector<uint64_t>& keys);

#ifdef SDB_FAULT_INJECTION
  /**
   * @brief Remove a batch of keys (as values) from the tree.
   *        This is a special version of remove that processes the keys in
   *        the specified order, without sorting them first.
   *
   * @param keys  The keys to be removed. Each key will be hashed to generate
   *              a value, then removed as if by the basic single removal
   *              method. This batch method is considerably more efficient.
   * @throws std::invalid_argument  If remove hits a node with 0 count
   */
  void removeUnsorted(const std::vector<uint64_t>& keys);
#endif

  /**
   * Remove all values from the tree.
   */
  void clear();

  /**
   * Clone the tree.
   */
  std::unique_ptr<MerkleTree<Hasher, BranchingBits>> clone();

  /**
   * Find the ranges of keys over which two trees differ. Currently,
   * only trees of the same depth can be diffed.
   *
   * other The other tree to compare
   * Return  Vector of (inclusive) ranges of keys over which trees differ
   * @throws std::invalid_argument  If trees different rangeMin
   */
  std::vector<std::pair<uint64_t, uint64_t>> diff(
    MerkleTree<Hasher, BranchingBits>& other);

  /**
   * Convert to a human-readable string for printing
   *
   * full Whether or not to include meta data
   * Return String representing the tree
   */
  std::string toString(bool full) const;

  /**
   * Serialize the tree for transport or storage in portable format
   *
   * output    vpack::Builder for output
   * onlyPopulated  Only return populated buckets
   */
  void serialize(vpack::Builder& output, bool only_populated) const;

  /**
   * Provides a partition of the keyspace
   *
   * Makes best effort attempt to ensure the partitions are as even as possible.
   * That is, to the granularity allowed, it will try to ensure that the number
   * of keys in each partition is roughly the same.
   *
   * count The number of partitions to return
   * Return Vector of (inclusive) ranges that partiion the keyspace
   */
  std::vector<std::pair<uint64_t, uint64_t>> partitionKeys(
    uint64_t count) const;

  /**
   * Serialize the tree for transport or storage in binary format
   */
  void serializeBinary(std::string& output, BinaryFormat format) const;

  /**
   * Checks the consistency of the tree
   *
   * If any inconsistency is found, this function will throw
   */
  void checkConsistency() const;

  uint64_t numberOfShards() const noexcept;

#ifdef SDB_FAULT_INJECTION
  // intentionally corrupts the tree. used for testing only
  void corrupt(uint64_t count, uint64_t hash);
#endif

 protected:
  explicit MerkleTree(std::string_view buffer);
  explicit MerkleTree(Data&& data);
  explicit MerkleTree(const MerkleTree<Hasher, BranchingBits>& other);

  Meta& meta() noexcept;
  const Meta& meta() const noexcept;

  Node& node(uint64_t index);
  const Node& node(uint64_t index) const noexcept;
  bool empty(uint64_t index) const noexcept;

  uint64_t index(uint64_t key) const noexcept;
  void modify(uint64_t key, bool is_insert);
  void modify(const std::vector<uint64_t>& keys, bool is_insert);
  bool modifyLocal(Node& node, uint64_t count, uint64_t value,
                   bool is_insert) noexcept;
  bool modifyLocal(uint64_t key, uint64_t value, bool is_insert);
  void leftCombine(bool with_shift);
  void rightCombine(bool with_shift);
  void growLeft(uint64_t key);
  void growRight(uint64_t key);
  bool equalAtIndex(const MerkleTree<Hasher, BranchingBits>& other,
                    uint64_t index) const noexcept;
  std::pair<uint64_t, uint64_t> chunkRange(uint64_t chunk,
                                           uint64_t depth) const;
  void serializeMeta(std::string& output, bool add_padding) const;
  void serializeNodes(std::string& output, bool all) const;

 private:
  /**
   * Checks the min and max keys for an insert, and grows
   * the tree as necessary
   *
   * If minKey < rangeMin, this will grow the tree to the left
   * If maxKey >= rangeMax, this will grow the tree to the right
   *
   * guard Lock guard (already locked)
   * minKey Minimum key to insert
   * maxKey Maximum key to insert
   */
  void prepareInsertMinMax(std::unique_lock<absl::Mutex>& guard,
                           uint64_t min_key, uint64_t max_key);

  /**
   * Checks the consistency of the tree
   *
   * If any inconsistency is found, this function will throw
   */
  void checkInternalConsistency() const;

 private:
  mutable absl::Mutex _data_lock;
  Data _data;
};

using RevisionTree = MerkleTree<FnvHashProvider, 3>;

}  // namespace containers
}  // namespace sdb
