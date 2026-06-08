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
  virtual DocumentMaskKind Kind() const = 0;

  bool operator==(const DocumentMask& other) const;
  bool IsEmpty() const { return DeletedDocCount() == 0; }
  static DocumentMaskKind GetKind(const DocumentMask* mask) {
    return mask ? mask->Kind() : DocumentMaskKind::None;
  }
};

template<typename Func>
void ForEachDeleted(const DocumentMask& mask, Func&& cb);
template<typename Func>
void ForEachAlive(const DocumentMask& mask, Func&& cb);

template<bool StoreDeleted = true>
class DocumentHashMask final : public DocumentMask {
 public:
  DocumentHashMask() = default;

  DocumentHashMask(IResourceManager& rm, const DocumentMask& other)
    : _stored_docs{ManagedTypedAllocator<doc_id_t>{rm}},
      _doc_count{other.DocCount()} {
    if constexpr (StoreDeleted) {
      _stored_docs.reserve(other.DeletedDocCount());
      irs::ForEachDeleted(
        other, [this](doc_id_t doc_id) { _stored_docs.insert(doc_id); });
    } else {
      _stored_docs.reserve(other.DocCount() - other.DeletedDocCount());
      irs::ForEachAlive(
        other, [this](doc_id_t doc_id) { _stored_docs.insert(doc_id); });
    }
  }
  DocumentHashMask(IResourceManager& rm, size_t doc_count,
                   size_t deleted_doc_count)
    : _stored_docs{ManagedTypedAllocator<doc_id_t>{rm}}, _doc_count{doc_count} {
    if constexpr (StoreDeleted) {
      _stored_docs.reserve(deleted_doc_count);
    } else {
      _stored_docs.reserve(doc_count - deleted_doc_count);
    }
  }
  IRS_FORCE_INLINE bool IsDeleted(doc_id_t doc_id) const override {
    return doc_limits::min() <= doc_id &&
           doc_id < doc_limits::min() + _doc_count &&
           _stored_docs.contains(doc_id) == StoreDeleted;
  }
  size_t DeletedDocCount() const override {
    return StoreDeleted ? _stored_docs.size()
                        : _doc_count - _stored_docs.size();
  }
  size_t DocCount() const override { return _doc_count; }
  DocumentMaskKind Kind() const override {
    return StoreDeleted ? DocumentMaskKind::DeletedHashSet
                        : DocumentMaskKind::AliveHashSet;
  }

  template<typename Func>
  void ForEachDeleted(Func&& cb) const {
    if constexpr (StoreDeleted) {
      for (auto doc_id : _stored_docs) {
        cb(doc_id);
      }
    } else {
      for (auto doc_id = doc_limits::min();
           doc_id < doc_limits::min() + _doc_count; ++doc_id) {
        if (!_stored_docs.contains(doc_id)) {
          cb(doc_id);
        }
      }
    }
  }
  template<typename Func>
  void ForEachAlive(Func&& cb) const {
    if constexpr (!StoreDeleted) {
      for (auto doc_id : _stored_docs) {
        cb(doc_id);
      }
    } else {
      for (auto doc_id = doc_limits::min();
           doc_id < doc_limits::min() + _doc_count; ++doc_id) {
        if (!_stored_docs.contains(doc_id)) {
          cb(doc_id);
        }
      }
    }
  }

  bool Store(doc_id_t doc_id) { return _stored_docs.insert(doc_id).second; }
  void HintDeletedDocCount(size_t count) {
    if constexpr (StoreDeleted) {
      _stored_docs.reserve(count);
    } else {
      _stored_docs.reserve(_doc_count - count);
    }
  }

 private:
  absl::flat_hash_set<doc_id_t,
                      absl::container_internal::hash_default_hash<doc_id_t>,
                      absl::container_internal::hash_default_eq<doc_id_t>,
                      ManagedTypedAllocator<doc_id_t>>
    _stored_docs;
  size_t _doc_count = 0;
};

using DocumentDeletedHashMask = DocumentHashMask</*StoreDeleted*/ true>;
using DocumentAliveHashMask = DocumentHashMask</*StoreDeleted*/ false>;

class DocumentBitMask final : public DocumentMask {
 public:
  DocumentBitMask() = default;
  explicit DocumentBitMask(IResourceManager& rm)
    : _is_deleted{rm}, _deleted_doc_count{0} {}

  DocumentBitMask(IResourceManager& rm, size_t doc_count)
    : _is_deleted{doc_count, rm}, _deleted_doc_count{0} {}
  DocumentBitMask(IResourceManager& rm, size_t doc_count,
                  size_t /*deleted_doc_count*/)
    : _is_deleted{doc_count, rm}, _deleted_doc_count{0} {}

  DocumentBitMask(IResourceManager& rm, const DocumentMask& other)
    : _is_deleted{other.DocCount(), rm},
      _deleted_doc_count{other.DeletedDocCount()} {
    irs::ForEachDeleted(other, [this](doc_id_t doc_id) {
      _is_deleted.set(doc_id - doc_limits::min());
    });
  }

  IRS_FORCE_INLINE bool IsDeleted(doc_id_t doc_id) const override {
    const auto idx = doc_id - doc_limits::min();
    return 0 <= idx && idx < _is_deleted.size() && _is_deleted.test(idx);
  }
  size_t DeletedDocCount() const override { return _deleted_doc_count; }
  size_t DocCount() const override { return _is_deleted.size(); }
  DocumentMaskKind Kind() const override {
    return DocumentMaskKind::DenseBitset;
  }

  template<typename Func>
  void ForEachDeleted(Func&& cb) const {
    // NB: doc_id here is 0-based, but user of the class expects valid doc_ids
    // to be 1-based
    const auto word_count = _is_deleted.words();
    for (size_t word_idx = 0; word_idx < word_count; ++word_idx) {
      auto word = _is_deleted[word_idx];
      const auto id_base =
        word_idx * BitsRequired<bitset::word_t>() + doc_limits::min();
      while (word) {
        auto lsb = word & -word;
        cb(id_base + std::countr_zero(lsb));
        word ^= lsb;
      }
    }
  }
  template<typename Func>
  void ForEachAlive(Func&& cb) const {
    const auto doc_count = DocCount();
    const auto word_count =
      std::min(_is_deleted.words(), bitset::bits_to_words(doc_count));
    for (size_t word_idx = 0; word_idx < word_count; ++word_idx) {
      // NB: 1 means Deleted, so invert word to iterate over Alive
      auto word = ~_is_deleted[word_idx];
      const auto id_base =
        word_idx * BitsRequired<bitset::word_t>() + doc_limits::min();
      while (word) {
        auto lsb = word & -word;
        const auto doc_id = id_base + std::countr_zero(lsb);
        if (doc_id >= doc_limits::min() + doc_count) [[unlikely]] {
          break;
        }
        cb(doc_id);
        word ^= lsb;
      }
    }
  }

  bool MarkDeleted(doc_id_t doc_id) {
    auto ret = _is_deleted.try_set(doc_id - doc_limits::min());
    _deleted_doc_count += static_cast<size_t>(ret);
    return ret;
  }

  void Merge(const DocumentMask& other) {
    irs::ForEachDeleted(other,
                        [this](doc_id_t doc_id) { MarkDeleted(doc_id); });
  }
  void Clear() {
    _is_deleted.clear();
    _deleted_doc_count = 0;
  }

 private:
  ManagedBitset _is_deleted;
  // Maintain a count to not run popcount on each DeletedDocCount call
  size_t _deleted_doc_count = 0;
};

template<typename Func>
void ForEachDeleted(const DocumentMask& mask, Func&& cb) {
  switch (mask.Kind()) {
    case DocumentMaskKind::None:
      break;
    case DocumentMaskKind::DeletedHashSet:
      static_cast<const DocumentDeletedHashMask&>(mask).ForEachDeleted(
        std::forward<Func>(cb));
      break;
    case DocumentMaskKind::DenseBitset:
      static_cast<const DocumentBitMask&>(mask).ForEachDeleted(
        std::forward<Func>(cb));
      break;
    case DocumentMaskKind::AliveHashSet:
      static_cast<const DocumentAliveHashMask&>(mask).ForEachDeleted(
        std::forward<Func>(cb));
      break;
  }
}

template<typename Func>
void ForEachAlive(const DocumentMask& mask, Func&& cb) {
  switch (mask.Kind()) {
    case DocumentMaskKind::None:
      break;
    case DocumentMaskKind::DeletedHashSet:
      static_cast<const DocumentDeletedHashMask&>(mask).ForEachAlive(
        std::forward<Func>(cb));
      break;
    case DocumentMaskKind::DenseBitset:
      static_cast<const DocumentBitMask&>(mask).ForEachAlive(
        std::forward<Func>(cb));
      break;
    case DocumentMaskKind::AliveHashSet:
      static_cast<const DocumentAliveHashMask&>(mask).ForEachAlive(
        std::forward<Func>(cb));
      break;
  }
}

// Stores pointer with tag and runs bounded dispatching of lookups into document
// mask, allowing compiler to inline function calls and prevent full vtable
// lookup.
class DocumentMaskView {
 public:
  DocumentMaskView() : _mask{nullptr}, _kind{DocumentMaskKind::None} {}
  explicit DocumentMaskView(const DocumentMask* mask)
    : _mask{mask}, _kind{DocumentMask::GetKind(mask)} {}
  DocumentMaskView(const DocumentMask* mask, DocumentMaskKind kind)
    : _mask{mask}, _kind{kind} {
    SDB_ASSERT(DocumentMask::GetKind(mask) == kind);
  }

  IRS_FORCE_INLINE bool IsDeleted(doc_id_t doc_id) const {
    switch (_kind) {
      case DocumentMaskKind::None:
        return false;
      case DocumentMaskKind::DeletedHashSet:
        return _mask &&
               static_cast<const DocumentDeletedHashMask*>(_mask)->IsDeleted(
                 doc_id);
      case DocumentMaskKind::DenseBitset:
        return _mask &&
               static_cast<const DocumentBitMask*>(_mask)->IsDeleted(doc_id);
      case DocumentMaskKind::AliveHashSet:
        return _mask &&
               static_cast<const DocumentAliveHashMask*>(_mask)->IsDeleted(
                 doc_id);
    }
  }
  size_t DeletedDocCount() const {
    return _mask ? _mask->DeletedDocCount() : 0;
  }
  bool IsEmpty() const { return _mask ? _mask->IsEmpty() : true; }
  DocumentMaskKind Kind() const { return _kind; }
  const DocumentMask* Mask() const { return _mask; }

  bool operator==(const DocumentMaskView& rhs) const;

 private:
  const DocumentMask* _mask;
  DocumentMaskKind _kind;
};

DocumentMaskKind ChooseImmutableRepresentation(size_t doc_count,
                                               size_t deleted_doc_count);

std::shared_ptr<DocumentMask> MakeDocumentMask(IResourceManager& rm,
                                               DocumentMaskKind kind,
                                               DocumentMask&& mask);

}  // namespace irs
