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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/container/node_hash_map.h>
#include <absl/strings/str_cat.h>

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "basics/containers/node_hash_map.h"
#include "basics/logger/logger.h"
#include "basics/singleton.hpp"
#include "basics/so_utils.hpp"

namespace irs {

// Generic class representing globally-stored correspondence
// between object of KeyType and EntryType
template<typename KeyType, typename EntryType, typename RegisterType,
         typename Hash = absl::Hash<KeyType>, typename Pred = std::equal_to<>>
class GenericRegister : public Singleton<RegisterType> {
 public:
  using key_type = KeyType;
  using entry_type = EntryType;
  using hash_type = Hash;
  using pred_type = Pred;
  using visitor_t = std::function<bool(const key_type& key)>;

  virtual ~GenericRegister() = default;

  // @return the entry registered under the key and inf an insertion took place
  std::pair<entry_type, bool> set(const key_type& key,
                                  const entry_type& entry) {
    std::lock_guard lock(_mutex);
    auto itr = _reg_map.emplace(key, entry);
    return std::make_pair(itr.first->second, itr.second);
  }

  entry_type get(const key_type& key, bool load_library) const {
    const entry_type* entry = lookup(key);

    if (entry) {
      return *entry;
    }

    if (load_library) {
      return load_entry_from_so(key);
    }

    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "key not found");

    return entry_type{};
  }

  bool visit(const visitor_t& visitor) {
    std::lock_guard lock(_mutex);

    for (auto& entry : _reg_map) {
      if (!visitor(entry.first)) {
        return false;
      }
    }

    return true;
  }

 protected:
  using protected_visitor_t =
    std::function<bool(const key_type& key, const entry_type& value)>;

  // Should override this in order to initialize with new library handle
  virtual bool add_so_handle(void* /* handle */) { return true; }

  // Should override this in order to load entry from shared object
  virtual entry_type load_entry_from_so(const key_type& key) const {
    const auto file = key_to_filename(key);

    void* handle = LoadLibrary(file.c_str(), 1);

    if (nullptr == handle) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "load failed");
      return entry_type();
    }

    auto* this_ptr = const_cast<
      GenericRegister<KeyType, EntryType, RegisterType, Hash, Pred>*>(this);

    {
      std::lock_guard lock(_mutex);

      this_ptr->_so_handles.emplace_back(
        handle, [](void* h) -> void { irs::FreeLibrary(h); });
    }

    if (!this_ptr->add_so_handle(handle)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("init failed in shared object : ", file));
      return entry_type();
    }

    // Here we assume that shared object constructs global object
    // that performs registration
    const entry_type* entry = lookup(key);
    if (nullptr == entry) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("lookup failed in shared object : ", file));
      return entry_type{};
    }

    return *entry;
  }

  // Should convert key to corresponded shared object file name
  virtual std::string key_to_filename(const key_type& /* key */) const {
    return {};
  }

  virtual const entry_type* lookup(const key_type& key) const {
    std::lock_guard lock(_mutex);
    auto it = _reg_map.find(key);
    return _reg_map.end() == it ? nullptr : &it->second;
  }

  bool visit(const protected_visitor_t& visitor) {
    std::lock_guard lock(_mutex);

    for (auto& entry : _reg_map) {
      if (!visitor(entry.first, entry.second)) {
        return false;
      }
    }

    return true;
  }

 private:
  mutable absl::Mutex _mutex{absl::kConstInit};
  sdb::containers::NodeHashMap<key_type, entry_type, hash_type, pred_type>
    _reg_map;
  std::vector<std::unique_ptr<void, std::function<void(void*)>>> _so_handles;
};

// A generic_registrar capable of storing an associated tag for each entry
template<typename KeyType, typename EntryType, typename TagType,
         typename RegisterType, typename Hash = absl::Hash<KeyType>,
         typename Pred = std::equal_to<>>
class TaggedGenericRegister
  : public GenericRegister<KeyType, EntryType, RegisterType, Hash, Pred> {
 public:
  using tag_type = TagType;

  using parent_type =
    GenericRegister<KeyType, EntryType, RegisterType, Hash, Pred>;
  using entry_type = typename parent_type::entry_type;
  using hash_type = typename parent_type::hash_type;
  using key_type = typename parent_type::key_type;
  using pred_type = typename parent_type::pred_type;

  // @return the entry registered under the key and if an insertion took place
  std::pair<entry_type, bool> set(const key_type& key, const entry_type& entry,
                                  const tag_type* tag = nullptr) {
    auto itr = parent_type::set(key, entry);

    if (tag && itr.second) {
      std::lock_guard lock(_mutex);
      _tag_map.emplace(key, *tag);
    }

    return itr;
  }

  const tag_type* tag(const key_type& key) const {
    std::lock_guard lock(_mutex);
    auto itr = _tag_map.find(key);

    return itr == _tag_map.end() ? nullptr : &itr->second;
  }

 private:
  mutable absl::Mutex _mutex{absl::kConstInit};
  sdb::containers::NodeHashMap<key_type, tag_type, hash_type, pred_type>
    _tag_map;
};

}  // namespace irs
