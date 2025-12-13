// Copyright 2005-2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// See www.openfst.org for extensive documentation on this weighted
// finite-state transducer library.
//
// Helper class template useful for attaching an FST interface to its
// implementation, handling reference counting.

#ifndef FST_IMPL_TO_FST_H_
#define FST_IMPL_TO_FST_H_

#include <cstdint>
#include <memory>
#include <string>

#include <fst/fst.h>
#include <fst/impl-to-fst.h>
#include <fst/symbol-table.h>
#include <fst/test-properties.h>

namespace fst {

// This is a helper class template useful for attaching an FST interface to
// its implementation, handling reference counting.
// Thread-unsafe due to Properties (a const function) calling
// Impl::SetProperties. TODO(jrosenstock): Make thread-compatible.
// Impl's copy constructor must produce a thread-safe copy.
template <class I, class FST = Fst<typename I::Arc>>
class ImplToFst : public FST {
 public:
  using Impl = I;
  using Arc = typename Impl::Arc;
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;

  StateId Start() const override { return impl_->Start(); }

  Weight Final(StateId s) const override { return impl_->Final(s); }

  size_t NumArcs(StateId s) const override { return impl_->NumArcs(s); }

  size_t NumInputEpsilons(StateId s) const override {
    return impl_->NumInputEpsilons(s);
  }

  size_t NumOutputEpsilons(StateId s) const override {
    return impl_->NumOutputEpsilons(s);
  }

  // Note that this is a const function, but can set the Impl's properties
  // when test is true.
  uint64_t Properties(uint64_t mask, bool test) const override {
    if (test) {
      uint64_t knownprops,
          testprops = internal::TestProperties(*this, mask, &knownprops);
      // Properties is a const member function, but can set the cached
      // properties. UpdateProperties does this thread-safely via atomics.
      impl_->UpdateProperties(testprops, knownprops);
      return testprops & mask;
    } else {
      return impl_->Properties(mask);
    }
  }

  const std::string &Type() const override { return impl_->Type(); }

  const SymbolTable *InputSymbols() const override {
    return impl_->InputSymbols();
  }

  const SymbolTable *OutputSymbols() const override {
    return impl_->OutputSymbols();
  }

 protected:
  explicit ImplToFst(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}

  // The object is thread-compatible if constructed with safe=true,
  // otherwise thread-unsafe.
  // This constructor presumes there is a copy constructor for the
  // implementation that produces a thread-safe copy.
  ImplToFst(const ImplToFst &fst, bool safe) {
    if (safe) {
      impl_ = std::make_shared<Impl>(*(fst.impl_));
    } else {
      impl_ = fst.impl_;
    }
  }

  ImplToFst() = delete;

  ImplToFst(const ImplToFst &fst) : impl_(fst.impl_) {}

  ImplToFst(ImplToFst &&fst) noexcept : impl_(std::move(fst.impl_)) {
    fst.impl_ = std::make_shared<Impl>();
  }

  ImplToFst &operator=(const ImplToFst &fst) {
    impl_ = fst.impl_;
    return *this;
  }

  ImplToFst &operator=(ImplToFst &&fst) noexcept {
    if (this != &fst) {
      impl_ = std::move(fst.impl_);
      fst.impl_ = std::make_shared<Impl>();
    }
    return *this;
  }

  // Returns raw pointers to the shared object.
  const Impl *GetImpl() const { return impl_.get(); }

  Impl *GetMutableImpl() const { return impl_.get(); }

  // Returns a ref-counted smart poiner to the implementation.
  std::shared_ptr<Impl> GetSharedImpl() const { return impl_; }

  bool Unique() const { return impl_.use_count() == 1; }

  void SetImpl(std::shared_ptr<Impl> impl) { impl_ = std::move(impl); }

 private:
  template <class IFST, class OFST>
  friend void Cast(const IFST &ifst, OFST *ofst);

  std::shared_ptr<Impl> impl_;
};

}  // namespace fst

#endif  // FST_IMPL_TO_FST_H_
