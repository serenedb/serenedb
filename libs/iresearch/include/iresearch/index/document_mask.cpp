#include "iresearch/index/document_mask.hpp"

namespace irs {

bool DocumentMask::operator==(const DocumentMask& other) const {
  if (DeletedDocCount() != other.DeletedDocCount()) {
    return false;
  }
  bool equal = true;
  ForEachDeleted(*this, [&equal, &other](doc_id_t doc_id) {
    equal = equal && other.IsDeleted(doc_id);
  });
  return equal;
}

bool DocumentMaskView::operator==(const DocumentMaskView& rhs) const {
  if (_mask == rhs._mask) {
    return true;
  }
  if ((!_mask && rhs._mask->DeletedDocCount() == 0) ||
      (!rhs._mask && _mask->DeletedDocCount() == 0)) {
    return true;
  }
  return *_mask == *rhs._mask;
}

DocumentMaskKind ChooseImmutableRepresentation(size_t doc_count,
                                               size_t deleted_doc_count) {
  if (deleted_doc_count == 0) {
    return DocumentMaskKind::None;
  } else if (deleted_doc_count <= doc_count / 100) {  // 0 < x <1% of documents
    return DocumentMaskKind::DeletedHashSet;
  } else if (deleted_doc_count <
             99 * doc_count / 100) {  // 1 <= x < 99% of documents
    return DocumentMaskKind::DenseBitset;
  } else {  // 99% <= x <= 100% of documents
    return DocumentMaskKind::AliveHashSet;
  }
}

std::shared_ptr<DocumentMask> MakeDocumentMask(IResourceManager& rm,
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
