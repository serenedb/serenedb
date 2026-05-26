#pragma once

#include <absl/container/flat_hash_set.h>

#include <cstddef>

#include "basics/containers/bitset.hpp"
#include "basics/managed_allocator.hpp"
#include "basics/resource_manager.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

enum class DocumentMaskKind {
  None,
  DeletedHashSet,
  DenseBitset,
  AliveHashSet,
};

// Interface for a view of a mask of deleted documents in an index's segment
class DocumentMask {
 public:
  virtual ~DocumentMask() = default;
  virtual bool IsDeleted(doc_id_t doc_id) const = 0;
  virtual size_t DeletedDocCount() const = 0;
  virtual size_t DocCount() const = 0;
  virtual void ForEachDeleted(
    const std::function<void(doc_id_t)>& cb) const = 0;
  virtual void ForEachAlive(const std::function<void(doc_id_t)>& cb) const = 0;
  virtual DocumentMaskKind Kind() const = 0;

  bool operator==(const DocumentMask& other) const {
    if (DeletedDocCount() != other.DeletedDocCount()) {
      return false;
    }
    bool equal = true;
    ForEachDeleted([&equal, &other](doc_id_t doc_id) {
      equal = equal && other.IsDeleted(doc_id);
    });
    return equal;
  }
  bool IsEmpty() const { return DeletedDocCount() == 0; }
  static DocumentMaskKind GetKind(const DocumentMask* mask) {
    return mask ? mask->Kind() : DocumentMaskKind::None;
  }
};

template<bool StoreDeleted = true>
class DocumentHashMask final : public DocumentMask {
 public:
  DocumentHashMask() = default;

  DocumentHashMask(IResourceManager& rm, const DocumentMask& other)
    : stored_docs_{ManagedTypedAllocator<doc_id_t>{rm}},
      doc_count_{other.DocCount()} {
    if constexpr (StoreDeleted) {
      stored_docs_.reserve(other.DeletedDocCount());
      other.ForEachDeleted(
        [this](doc_id_t doc_id) { stored_docs_.insert(doc_id); });
    } else {
      stored_docs_.reserve(other.DocCount() - other.DeletedDocCount());
      other.ForEachAlive(
        [this](doc_id_t doc_id) { stored_docs_.insert(doc_id); });
    }
  }
  DocumentHashMask(IResourceManager& rm, size_t doc_count,
                   size_t deleted_doc_count)
    : stored_docs_{ManagedTypedAllocator<doc_id_t>{rm}}, doc_count_{doc_count} {
    if constexpr (StoreDeleted) {
      stored_docs_.reserve(deleted_doc_count);
    } else {
      stored_docs_.reserve(doc_count - deleted_doc_count);
    }
  }
  IRS_FORCE_INLINE bool IsDeleted(doc_id_t doc_id) const override {
    return doc_limits::min() <= doc_id && doc_id <= doc_count_ &&
           stored_docs_.contains(doc_id) == StoreDeleted;
  }
  size_t DeletedDocCount() const override {
    return StoreDeleted ? stored_docs_.size()
                        : doc_count_ - stored_docs_.size();
  }
  size_t DocCount() const override { return doc_count_; }
  void ForEachDeleted(const std::function<void(doc_id_t)>& cb) const override {
    if constexpr (StoreDeleted) {
      for (auto doc_id : stored_docs_) {
        cb(doc_id);
      }
    } else {
      for (auto doc_id = doc_limits::min(); doc_id <= doc_count_; ++doc_id) {
        if (!stored_docs_.contains(doc_id)) {
          cb(doc_id);
        }
      }
    }
  }
  void ForEachAlive(const std::function<void(doc_id_t)>& cb) const override {
    if constexpr (!StoreDeleted) {
      for (auto doc_id : stored_docs_) {
        cb(doc_id);
      }
    } else {
      for (auto doc_id = doc_limits::min(); doc_id <= doc_count_; ++doc_id) {
        if (!stored_docs_.contains(doc_id)) {
          cb(doc_id);
        }
      }
    }
  }
  DocumentMaskKind Kind() const override {
    return StoreDeleted ? DocumentMaskKind::DeletedHashSet
                        : DocumentMaskKind::AliveHashSet;
  }

  bool Store(doc_id_t doc_id) {
    return stored_docs_.insert(doc_id).second;
  }
  void HintDeletedDocCount(size_t count) {
    if constexpr (StoreDeleted) {
      stored_docs_.reserve(count);
    } else {
      stored_docs_.reserve(doc_count_ - count);
    }
  }

 private:
  absl::flat_hash_set<doc_id_t,
                      absl::container_internal::hash_default_hash<doc_id_t>,
                      absl::container_internal::hash_default_eq<doc_id_t>,
                      ManagedTypedAllocator<doc_id_t>>
    stored_docs_;
  size_t doc_count_ = 0;
};

using DocumentDeletedHashMask = DocumentHashMask</*StoreDeleted*/ true>;
using DocumentAliveHashMask = DocumentHashMask</*StoreDeleted*/ false>;

class DocumentBitMask final : public DocumentMask {
 public:
  DocumentBitMask() = default;
  explicit DocumentBitMask(IResourceManager& rm)
    : is_deleted_{rm}, deleted_doc_count_{0} {}

  DocumentBitMask(IResourceManager& rm, size_t doc_count)
    : is_deleted_{doc_count, rm}, deleted_doc_count_{0} {}
  DocumentBitMask(IResourceManager& rm, size_t doc_count,
                  size_t /*deleted_doc_count*/)
    : is_deleted_{doc_count, rm}, deleted_doc_count_{0} {}

  DocumentBitMask(IResourceManager& rm, const DocumentMask& other)
    : is_deleted_{other.DocCount(), rm},
      deleted_doc_count_{other.DeletedDocCount()} {
    other.ForEachDeleted(
      [this](doc_id_t doc_id) { is_deleted_.set(doc_id - doc_limits::min()); });
  }

  IRS_FORCE_INLINE bool IsDeleted(doc_id_t doc_id) const override {
    const auto idx = doc_id - doc_limits::min();
    return 0 <= idx && idx < is_deleted_.size() && is_deleted_.test(idx);
  }
  size_t DeletedDocCount() const override { return deleted_doc_count_; }
  size_t DocCount() const override { return is_deleted_.size(); }
  void ForEachDeleted(const std::function<void(doc_id_t)>& cb) const override {
    // NB: doc_id here is 0-based, but user of the class expects valid doc_ids
    // to be 1-based
    const auto word_count = is_deleted_.words();
    for (size_t word_idx = 0; word_idx < word_count; ++word_idx) {
      auto word = is_deleted_[word_idx];
      const auto id_base =
        word_idx * BitsRequired<bitset::word_t>() + doc_limits::min();
      while (word) {
        auto lsb = word & -word;
        cb(id_base + std::countr_zero(lsb));
        word ^= lsb;
      }
    }
  }
  void ForEachAlive(const std::function<void(doc_id_t)>& cb) const override {
    const auto doc_count = DocCount();
    const auto word_count =
      std::min(is_deleted_.words(), bitset::bits_to_words(doc_count));
    for (size_t word_idx = 0; word_idx < word_count; ++word_idx) {
      // NB: 1 means Deleted, so invert word to iterate over Alive
      auto word = ~is_deleted_[word_idx];
      const auto id_base =
        word_idx * BitsRequired<bitset::word_t>() + doc_limits::min();
      while (word) {
        auto lsb = word & -word;
        const auto doc_id = id_base + std::countr_zero(lsb);
        if (doc_id > doc_count) [[unlikely]] {
          break;
        }
        cb(doc_id);
        word ^= lsb;
      }
    }
  }
  DocumentMaskKind Kind() const override {
    return DocumentMaskKind::DenseBitset;
  }

  bool MarkDeleted(doc_id_t doc_id) {
    auto ret = is_deleted_.try_set(doc_id - doc_limits::min());
    deleted_doc_count_ += static_cast<size_t>(ret);
    return ret;
  }

  void Merge(const DocumentMask& other) {
    other.ForEachDeleted([this](doc_id_t doc_id) { MarkDeleted(doc_id); });
  }
  void Clear() {
    is_deleted_.clear();
    deleted_doc_count_ = 0;
  }

 private:
  ManagedBitset is_deleted_;
  size_t deleted_doc_count_ =
    0;  // to not run popcount each DeletedDocCount call
};

// Stores pointer with tag and runs bounded dispatching of lookups into document
// mask, allowing compiler to inline function calls and prevent full vtable
// lookup.
class DocumentMaskView {
 public:
  DocumentMaskView() : mask{nullptr}, kind{DocumentMaskKind::None} {}
  explicit DocumentMaskView(const DocumentMask* mask)
    : mask{mask}, kind{DocumentMask::GetKind(mask)} {}
  DocumentMaskView(const DocumentMask* mask, DocumentMaskKind kind)
    : mask{mask}, kind{kind} {
    SDB_ASSERT(DocumentMask::GetKind(mask) == kind);
  }

  IRS_FORCE_INLINE bool IsDeleted(doc_id_t doc_id) const {
    switch (kind) {
      case DocumentMaskKind::None:
        return false;
      case DocumentMaskKind::DeletedHashSet:
        return mask &&
               static_cast<const DocumentDeletedHashMask*>(mask)->IsDeleted(
                 doc_id);
      case DocumentMaskKind::DenseBitset:
        return mask &&
               static_cast<const DocumentBitMask*>(mask)->IsDeleted(doc_id);
      case DocumentMaskKind::AliveHashSet:
        return mask &&
               static_cast<const DocumentAliveHashMask*>(mask)->IsDeleted(
                 doc_id);
    }
  }
  size_t DeletedDocCount() const { return mask ? mask->DeletedDocCount() : 0; }
  bool IsEmpty() const { return mask ? mask->IsEmpty() : true; }
  DocumentMaskKind Kind() const { return kind; }
  const DocumentMask* Mask() const { return mask; }

  bool operator==(const DocumentMaskView& rhs) const {
    if (mask == rhs.mask) {
      return true;
    }
    if ((!mask && rhs.mask->DeletedDocCount() == 0) ||
        (!rhs.mask && mask->DeletedDocCount() == 0)) {
      return true;
    }
    return *mask == *rhs.mask;
  }

 private:
  const DocumentMask* mask;
  DocumentMaskKind kind;
};

inline DocumentMaskKind ChooseImmutableRepresentation(
  size_t doc_count, size_t deleted_doc_count) {
  if (deleted_doc_count == 0) {
    return DocumentMaskKind::None;
  } else if (deleted_doc_count < doc_count / 100) {  // 0 < x <1% of documents
    return DocumentMaskKind::DeletedHashSet;
  } else if (deleted_doc_count <
             99 * doc_count / 100) {  // 1 <= x < 99% of documents
    return DocumentMaskKind::DenseBitset;
  } else {  // 99% <= x <= 100% of documents
    return DocumentMaskKind::AliveHashSet;
  }
}

inline std::shared_ptr<DocumentMask> MakeDocumentMask(IResourceManager& rm,
                                                      DocumentMaskKind kind,
                                                      DocumentMask&& mask) {
  switch (kind) {
    case DocumentMaskKind::None:
      return nullptr;
    case DocumentMaskKind::DeletedHashSet:
      if (mask.Kind() == DocumentMaskKind::DeletedHashSet) {
        return std::make_shared<DocumentDeletedHashMask>(
          static_cast<DocumentDeletedHashMask&&>(mask));
      }
      return std::make_shared<DocumentDeletedHashMask>(rm, mask);
    case DocumentMaskKind::DenseBitset:
      if (mask.Kind() == DocumentMaskKind::DenseBitset) {
        return std::make_shared<DocumentBitMask>(
          static_cast<DocumentBitMask&&>(mask));
      }
      return std::make_shared<DocumentBitMask>(rm, mask);
    case DocumentMaskKind::AliveHashSet:
      if (mask.Kind() == DocumentMaskKind::AliveHashSet) {
        return std::make_shared<DocumentAliveHashMask>(
          static_cast<DocumentAliveHashMask&&>(mask));
      }
      return std::make_shared<DocumentAliveHashMask>(rm, mask);
  }
}

}  // namespace irs
