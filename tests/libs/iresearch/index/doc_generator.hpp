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

#include <unicode/locid.h>

#include <atomic>
#include <boost/iterator/iterator_facade.hpp>
#include <filesystem>
#include <fstream>
#include <functional>

#include "basics/down_cast.h"
#include "insert_field.hpp"
#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/iterator.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class DataOutput;
class Tokenizer;

}  // namespace irs
namespace tests {

// Canonical mapping from JSON / test field-name string to a stable
// `irs::field_id`. Test fixtures index by id (`SegmentWriter::index(field_id,
// ...)`, `SubReader::field(field_id)`); the on-disk format never carries the
// name. Shared JSON factories (`GenericJsonFieldFactory`, ...) and per-test
// fixtures route their fields through this helper so every site that touches
// the same logical column lands on the same id.
//
// Ids below the FNV fallback range are stable per-name and small (so they
// stay distinct from catalog-allocated ids when both coexist). `NextId()`
// never returns these literal values. Unknown names are hashed via FNV-1a;
// the hash is biased away from `field_limits::invalid()`.
//
// Two flavors are provided:
//   - `FieldIdFor`: constexpr; folds at compile time for string-literal call
//     sites (per-test fixtures with hard-coded names).
//   - `FieldIdForRuntime`: defined in the .cpp; backed by a single
//     `absl::flat_hash_map<std::string_view, irs::field_id>` populated once
//     so the JSON factories (per doc / per field hot path) pay O(1) hash
//     instead of O(N) string compares. Returns the same id as the constexpr
//     form for every input.
//
// Convention for per-test bench/fixture constants:
//   When a test file declares its own `constexpr irs::field_id kFoo = 1;`
//   for a synthetic field, prefer `tests::FieldIdFor("<unique-name>")`
//   instead so that:
//     - the id is guaranteed not to collide with any other test fixture's
//       hard-coded id once the files end up linked into the same binary;
//     - downstream tests that grep test data by name can join on the same
//       shared mapping.
//   Bare-int sentinels (1, 2, ...) are harmless when the test file is
//   self-contained, but new tests should pick `FieldIdFor("...")` to
//   avoid future collision when files cross-link.
irs::field_id FieldIdForRuntime(std::string_view name);

inline constexpr irs::field_id FieldIdFor(std::string_view name) noexcept {
  if (name == "seq") {
    return 1;
  }
  if (name == "name") {
    return 2;
  }
  if (name == "same") {
    return 3;
  }
  if (name == "duplicated") {
    return 4;
  }
  if (name == "value") {
    return 5;
  }
  if (name == "field") {
    return 6;
  }
  if (name == "phrase") {
    return 7;
  }
  if (name == "name_anl") {
    return 8;
  }
  if (name == "name_anl_pay") {
    return 9;
  }
  if (name == "prefix") {
    return 10;
  }
  if (name == "title") {
    return 11;
  }
  if (name == "title_anl") {
    return 12;
  }
  if (name == "title_anl_pay") {
    return 13;
  }
  if (name == "body") {
    return 14;
  }
  if (name == "body_anl") {
    return 15;
  }
  if (name == "body_anl_pay") {
    return 16;
  }
  if (name == "date") {
    return 17;
  }
  if (name == "datestr") {
    return 18;
  }
  if (name == "id") {
    return 19;
  }
  if (name == "idstr") {
    return 20;
  }
  if (name == "test-field") {
    return 21;
  }
  if (name == "test") {
    return 22;
  }
  if (name == "name1") {
    return 23;
  }
  if (name == "phrase_anl") {
    return 24;
  }
  if (name == "foo") {
    return 25;
  }
  if (name == "doc_bytes") {
    return 26;
  }
  if (name == "doc_double") {
    return 27;
  }
  if (name == "doc_float") {
    return 28;
  }
  if (name == "doc_int") {
    return 29;
  }
  if (name == "doc_long") {
    return 30;
  }
  if (name == "doc_string") {
    return 31;
  }
  if (name == "doc_text") {
    return 32;
  }
  if (name == "another_column") {
    return 33;
  }
  if (name == "label") {
    return 34;
  }
  if (name == "updated") {
    return 35;
  }
  // 64-bit FNV-1a fallback for names not in the table above.
  uint64_t h = 14695981039346656037ull;
  for (char c : name) {
    h ^= static_cast<uint64_t>(static_cast<unsigned char>(c));
    h *= 1099511628211ull;
  }
  // Set bit 63 so the result is disjoint from literal ids (1..35). A name
  // whose lower 63 bits are all-ones would still hash to all-ones after
  // the OR (= field_limits::invalid()); guard against that explicitly.
  h |= (1ull << 63);
  if (h == std::numeric_limits<uint64_t>::max()) {
    h -= 1;
  }
  return h;
}

//////////////////////////////////////////////////////////////////////////////
/// @class ifield
/// @brief base interface for all fields
//////////////////////////////////////////////////////////////////////////////
struct Ifield {
  using ptr = std::shared_ptr<Ifield>;
  virtual ~Ifield() = default;

  virtual irs::IndexFeatures GetIndexFeatures() const = 0;
  virtual irs::analysis::Tokenizer& GetTokens() const {
    SDB_ASSERT(false);
    static irs::StringTokenizer kNoTokens;
    return kNoTokens;
  }
  virtual std::string_view Value() const {
    SDB_ASSERT(false);
    return {};
  }
  // Typed fields override with a block-native insert; the default drives
  // GetTokens() through the driver onto the public block API.
  virtual bool InsertBlockInto(const irs::IndexWriter::Document& doc) const {
    return InsertFieldTokens(doc, *this);
  }
  // Expected-index model terms for block-native fields (engaged, possibly
  // empty); disengaged means the model tokenizes GetTokens().
  virtual std::optional<std::vector<irs::bstring>> BlockTerms() const {
    return std::nullopt;
  }
  // Field identity; the on-disk storage key.
  virtual irs::field_id Id() const = 0;
  virtual std::string_view Name() const = 0;
  virtual bool Write(irs::DataOutput& out) const = 0;
};

//////////////////////////////////////////////////////////////////////////////
/// @class field
/// @brief base class for field implementations
//////////////////////////////////////////////////////////////////////////////
class FieldBase : public Ifield {
 public:
  FieldBase() = default;

  FieldBase(FieldBase&& rhs) noexcept = default;
  FieldBase& operator=(FieldBase&& rhs) noexcept = default;
  FieldBase(const FieldBase&) = default;
  FieldBase& operator=(const FieldBase&) = default;

  irs::IndexFeatures GetIndexFeatures() const noexcept final {
    return index_features;
  }

  irs::field_id Id() const noexcept final { return id; }

  std::string_view Name() const noexcept final { return name; }

  void Name(std::string name) noexcept { this->name = std::move(name); }

  std::string name;
  irs::field_id id{irs::field_limits::invalid()};
  irs::IndexFeatures index_features{irs::IndexFeatures::None};
};

//////////////////////////////////////////////////////////////////////////////
/// @class long_field
/// @brief provides capabilities for storing & indexing int64_t values
//////////////////////////////////////////////////////////////////////////////
class LongField : public FieldBase {
 public:
  typedef int64_t value_t;

  LongField() = default;

  bool InsertBlockInto(const irs::IndexWriter::Document& doc) const final;
  std::optional<std::vector<irs::bstring>> BlockTerms() const final;
  void value(value_t value) { _value = value; }
  value_t value() const { return _value; }
  bool Write(irs::DataOutput& out) const final;

 private:
  int64_t _value{};
};

//////////////////////////////////////////////////////////////////////////////
/// @class long_field
/// @brief provides capabilities for storing & indexing int32_t values
//////////////////////////////////////////////////////////////////////////////
class IntField : public FieldBase {
 public:
  typedef int32_t value_t;

  explicit IntField(
    std::string_view name = "", int32_t value = 0,
    irs::IndexFeatures extra_features = irs::IndexFeatures::None)
    : _value{value} {
    this->Name(std::string{name});
    this->index_features |= extra_features;
  }
  IntField(IntField&& other) = default;

  bool InsertBlockInto(const irs::IndexWriter::Document& doc) const final;
  std::optional<std::vector<irs::bstring>> BlockTerms() const final;
  void value(value_t value) { _value = value; }
  value_t value() const { return _value; }
  bool Write(irs::DataOutput& out) const final;

 private:
  int32_t _value{};
};

//////////////////////////////////////////////////////////////////////////////
/// @class double_field
/// @brief provides capabilities for storing & indexing double_t values
//////////////////////////////////////////////////////////////////////////////
class DoubleField : public FieldBase {
 public:
  typedef double_t value_t;

  DoubleField() = default;

  bool InsertBlockInto(const irs::IndexWriter::Document& doc) const final;
  std::optional<std::vector<irs::bstring>> BlockTerms() const final;
  void value(value_t value) { _value = value; }
  value_t value() const { return _value; }
  bool Write(irs::DataOutput& out) const final;

 private:
  double_t _value{};
};

//////////////////////////////////////////////////////////////////////////////
/// @class float_field
/// @brief provides capabilities for storing & indexing double_t values
//////////////////////////////////////////////////////////////////////////////
class FloatField : public FieldBase {
 public:
  typedef float_t value_t;

  FloatField() = default;

  bool InsertBlockInto(const irs::IndexWriter::Document& doc) const final;
  std::optional<std::vector<irs::bstring>> BlockTerms() const final;
  void value(value_t value) { _value = value; }
  value_t value() const { return _value; }
  bool Write(irs::DataOutput& out) const final;

 private:
  float_t _value{};
};

//////////////////////////////////////////////////////////////////////////////
/// @class binary_field
/// @brief provides capabilities for storing & indexing binary values
//////////////////////////////////////////////////////////////////////////////
class BinaryField : public FieldBase {
 public:
  BinaryField() = default;

  irs::analysis::Tokenizer& GetTokens() const final;
  std::string_view Value() const final {
    return irs::ViewCast<char, irs::byte_type>(irs::bytes_view{_value});
  }
  const irs::bstring& value() const { return _value; }
  void value(irs::bytes_view value) { _value = value; }
  void value(irs::bstring&& value) { _value = std::move(value); }

  template<typename Iterator>
  void value(Iterator first, Iterator last) {
    _value = bytes(first, last);
  }

  bool Write(irs::DataOutput& out) const final;

 private:
  mutable irs::StringTokenizer _stream;
  irs::bstring _value;
};

namespace detail {

template<typename Ptr>
struct ExtractElementType {
  using value_type = typename Ptr::element_type;
  using reference = typename Ptr::element_type&;
  using pointer = typename Ptr::element_type*;
};

template<typename Ptr>
struct ExtractElementType<const Ptr> {
  using value_type = const typename Ptr::element_type;
  using reference = const typename Ptr::element_type&;
  using pointer = const typename Ptr::element_type*;
};

template<typename Ptr>
struct ExtractElementType<Ptr*> {
  using value_type = Ptr;
  using reference = Ptr&;
  using pointer = Ptr*;
};

}  // namespace detail

//////////////////////////////////////////////////////////////////////////////
/// @class const_ptr_iterator
/// @brief iterator adapter for containers with the smart pointers
//////////////////////////////////////////////////////////////////////////////
template<typename IteratorImpl>
class PtrIterator
  : public ::boost::iterator_facade<
      PtrIterator<IteratorImpl>,
      typename detail::ExtractElementType<
        std::remove_reference_t<typename IteratorImpl::reference>>::value_type,
      ::boost::random_access_traversal_tag> {
 private:
  using element_type = detail::ExtractElementType<
    std::remove_reference_t<typename IteratorImpl::reference>>;

  using base_element_type = typename element_type::value_type;

  using base =
    ::boost::iterator_facade<PtrIterator<IteratorImpl>, base_element_type,
                             ::boost::random_access_traversal_tag>;

  using iterator_facade = typename base::iterator_facade_;

  template<typename T>
  struct AdjustConst : irs::irstd::AdjustConst<base_element_type, T> {};

 public:
  using reference = typename iterator_facade::reference;
  using difference_type = typename iterator_facade::difference_type;

  PtrIterator(const IteratorImpl& it) : _it(it) {}

  //////////////////////////////////////////////////////////////////////////////
  /// @brief returns downcasted reference to the iterator's value
  //////////////////////////////////////////////////////////////////////////////
  template<typename T>
  typename AdjustConst<T>::reference as() const {
    static_assert(std::is_base_of_v<base_element_type, T>);
    return sdb::basics::downCast<T>(dereference());
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief returns downcasted pointer to the iterator's value
  ///        or nullptr if there is no available conversion
  //////////////////////////////////////////////////////////////////////////////
  template<typename T>
  typename AdjustConst<T>::pointer safe_as() const {
    static_assert(std::is_base_of_v<base_element_type, T>);
    reference it = dereference();
    return dynamic_cast<typename AdjustConst<T>::pointer>(&it);
  }

  bool is_null() const noexcept { return *_it == nullptr; }

 private:
  friend class ::boost::iterator_core_access;

  reference dereference() const {
    SDB_ASSERT(*_it);
    return **_it;
  }
  void advance(difference_type n) { _it += n; }
  difference_type distance_to(const PtrIterator& rhs) const {
    return rhs._it - _it;
  }
  void increment() { ++_it; }
  void decrement() { --_it; }
  bool equal(const PtrIterator& rhs) const { return _it == rhs._it; }

  IteratorImpl _it;
};

/* -------------------------------------------------------------------
 * document
 * ------------------------------------------------------------------*/

class Particle : irs::util::Noncopyable {
 public:
  typedef std::vector<Ifield::ptr> fields_t;
  typedef PtrIterator<fields_t::const_iterator> const_iterator;
  typedef PtrIterator<fields_t::iterator> iterator;

  Particle() = default;
  Particle(Particle&& rhs) noexcept;
  Particle& operator=(Particle&& rhs) noexcept;
  virtual ~Particle() = default;

  size_t size() const { return _fields.size(); }
  void clear() { _fields.clear(); }
  void reserve(size_t size) { _fields.reserve(size); }
  void push_back(const Ifield::ptr& fld) { _fields.emplace_back(fld); }

  Ifield& back() const { return *_fields.back(); }
  bool contains(const std::string_view& name) const;
  std::vector<Ifield::ptr> find(const std::string_view& name) const;

  // Id-based equivalents. Field tests bind a stable `irs::field_id` via
  // `tests::FieldIdFor` / catalog counters; when the caller already has the
  // id (typical for JSON-factory routes) prefer these to avoid a per-field
  // string compare.
  bool contains_by_id(irs::field_id id) const noexcept;
  Ifield* get_by_id(irs::field_id id) const noexcept;
  bool remove_by_id(irs::field_id id) noexcept;

  template<typename T>
  T& back() const {
    typedef
      typename std::enable_if<std::is_base_of<tests::Ifield, T>::value, T>::type
        type;

    return static_cast<type&>(*_fields.back());
  }

  Ifield* get(const std::string_view& name) const;

  template<typename T>
  T& get(size_t i) const {
    typedef
      typename std::enable_if<std::is_base_of<tests::Ifield, T>::value, T>::type
        type;

    return static_cast<type&>(*_fields[i]);
  }

  template<typename T>
  T* get(const std::string_view& name) const {
    typedef
      typename std::enable_if<std::is_base_of<tests::Ifield, T>::value, T>::type
        type;

    return static_cast<type*>(get(name));
  }

  template<typename T>
  T* get_by_id(irs::field_id id) const noexcept {
    typedef
      typename std::enable_if<std::is_base_of<tests::Ifield, T>::value, T>::type
        type;

    return static_cast<type*>(get_by_id(id));
  }

  void remove(const std::string_view& name);

  iterator begin() { return iterator(_fields.begin()); }
  iterator end() { return iterator(_fields.end()); }

  const_iterator begin() const { return const_iterator(_fields.begin()); }
  const_iterator end() const { return const_iterator(_fields.end()); }

 protected:
  fields_t _fields;
};

struct Document : irs::util::Noncopyable {
  Document() = default;
  Document(Document&& rhs) noexcept;
  virtual ~Document() = default;

  void insert(const Ifield::ptr& field, bool indexed = true,
              bool stored = true) {
    if (indexed) {
      this->indexed.push_back(field);
    }
    if (stored) {
      this->stored.push_back(field);
    }
  }

  void reserve(size_t size) {
    indexed.reserve(size);
    stored.reserve(size);
  }

  void clear() {
    indexed.clear();
    stored.clear();
  }

  Particle indexed;
  Particle stored;
  Ifield::ptr sorted;
};

struct DocGeneratorBase {
  using ptr = std::unique_ptr<DocGeneratorBase>;

  virtual ~DocGeneratorBase() = default;

  virtual const tests::Document* next() = 0;
  virtual void reset() = 0;
};

class LimitingDocGenerator : public DocGeneratorBase {
 public:
  LimitingDocGenerator(DocGeneratorBase& gen, size_t offset, size_t limit)
    : _gen(&gen), _begin(offset), _end(offset + limit) {}

  const tests::Document* next() final {
    while (_pos < _begin) {
      if (!_gen->next()) {
        // exhausted
        _pos = _end;
        return nullptr;
      }

      ++_pos;
    }

    if (_pos < _end) {
      auto* doc = _gen->next();
      if (!doc) {
        // exhausted
        _pos = _end;
        return nullptr;
      }
      ++_pos;
      return doc;
    }

    return nullptr;
  }

  void reset() final {
    _pos = 0;
    _gen->reset();
  }

 private:
  DocGeneratorBase* _gen;
  size_t _pos{0};
  const size_t _begin;
  const size_t _end;
};

/* Generates documents from UTF-8 encoded file
 * with strings of the following format:
 * <title>;<date>:<body> */
class DelimDocGenerator : public DocGeneratorBase {
 public:
  struct DocTemplate : Document {
    virtual void init() = 0;
    virtual void value(size_t idx, const std::string& value) = 0;
    virtual void end() {}
    virtual void reset() {}
  };

  DelimDocGenerator(const std::filesystem::path& file, DocTemplate& doc,
                    uint32_t delim = 0x0009);

  const tests::Document* next() final;
  void reset() final;

 private:
  std::string _str;
  std::ifstream _ifs;
  DocTemplate* _doc;
  uint32_t _delim;
};

// Generates documents from a CSV file
class CsvDocGenerator : public DocGeneratorBase {
 public:
  struct DocTemplate : Document {
    virtual void init() = 0;
    virtual void value(size_t idx, const std::string_view& value) = 0;
    virtual void end() {}
    virtual void reset() {}
  };

  CsvDocGenerator(const std::filesystem::path& file, DocTemplate& doc);
  const tests::Document* next() final;
  void reset() final;
  bool skip();  // skip a single document, return if anything was skiped, false
                // == EOF

 private:
  DocTemplate& _doc;
  std::ifstream _ifs;
  std::string _line;
  irs::analysis::Tokenizer::ptr _stream;
};

/* Generates documents from json file based on type of JSON value */
class JsonDocGenerator : public DocGeneratorBase {
 public:
  enum class ValueType {
    NIL,
    BOOL,
    INT,
    UINT,
    INT64,
    UINT64,
    DBL,
    STRING,
    RAWNUM
  };

  // an std::string_view for union inclusion without a user-defined constructor
  // and non-trivial default constructor for compatibility with MSVC 2013
  struct JsonString {
    const char* data;
    size_t size;

    JsonString& operator=(std::string_view ref) {
      data = ref.data();
      size = ref.size();
      return *this;
    }

    operator std::string_view() const { return std::string_view(data, size); }
    operator std::string() const { return std::string(data, size); }
  };

  struct JsonValue {
    union {
      bool b;
      int i;
      unsigned ui;
      int64_t i64;
      uint64_t ui64;
      double_t dbl;
      JsonString str;
    };

    ValueType vt{ValueType::NIL};

    JsonValue() noexcept {}

    bool is_bool() const noexcept { return ValueType::BOOL == vt; }

    bool is_null() const noexcept { return ValueType::NIL == vt; }

    bool is_string() const noexcept { return ValueType::STRING == vt; }

    bool is_number() const noexcept {
      return ValueType::INT == vt || ValueType::INT64 == vt ||
             ValueType::UINT == vt || ValueType::UINT64 == vt ||
             ValueType::DBL == vt;
    }

    template<typename T>
    T as_number() const noexcept {
      SDB_ASSERT(is_number());

      switch (vt) {
        case ValueType::NIL:
          break;
        case ValueType::BOOL:
          break;
        case ValueType::INT:
          return static_cast<T>(i);
        case ValueType::UINT:
          return static_cast<T>(ui);
        case ValueType::INT64:
          return static_cast<T>(i64);
        case ValueType::UINT64:
          return static_cast<T>(ui64);
        case ValueType::DBL:
          return static_cast<T>(dbl);
        case ValueType::STRING:
          break;
        case ValueType::RAWNUM:
          break;
      }

      SDB_ASSERT(false);
      return T(0.);
    }
  };

  typedef std::function<void(tests::Document&, const std::string&,
                             const JsonValue&)>
    factory_f;

  JsonDocGenerator(const std::filesystem::path& file, const factory_f& factory);

  JsonDocGenerator(const char* data, const factory_f& factory);

  JsonDocGenerator(JsonDocGenerator&& rhs) noexcept;

  const tests::Document* next() final;
  void reset() final;

 private:
  JsonDocGenerator(const JsonDocGenerator&) = delete;

  std::vector<Document> _docs;
  std::vector<Document>::const_iterator _prev;
  std::vector<Document>::const_iterator _next;
};

// Construct the "text" analyzer with locale=C and an empty (explicit) stopword
// list. Mirrors the legacy registry call `tests::LegacyGetAnalyzer("text",
// Json,
// "{\"locale\":\"C\", \"stopwords\":[]}")`.
inline irs::analysis::Tokenizer::ptr MakeDocGenTextTokenizer() {
  irs::analysis::TextTokenizer::Options opts;
  opts.locale = icu::Locale::createFromName("C");
  opts.explicit_stopwords_set = true;
  return irs::analysis::TextTokenizer::Make(std::move(opts));
}

// field which uses text analyzer for tokenization and stemming
template<typename T>
class TextField : public tests::FieldBase {
 public:
  TextField(const std::string& name, bool /*payload*/ = false,
            irs::IndexFeatures extra_features = irs::IndexFeatures::None)
    : _token_stream(MakeDocGenTextTokenizer()) {
    this->Name(name);
    index_features = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                     irs::IndexFeatures::Offs | extra_features;
  }

  TextField(const std::string& name, const T& value, bool /*payload*/ = false,
            irs::IndexFeatures extra_features = irs::IndexFeatures::None)
    : _token_stream(MakeDocGenTextTokenizer()), _value(value) {
    this->Name(name);
    index_features = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                     irs::IndexFeatures::Offs | extra_features;
  }

  TextField(TextField&& other) = default;

  std::string_view value() const { return _value; }
  void value(const T& value) { _value = value; }
  void value(T&& value) { _value = std::move(value); }

  irs::analysis::Tokenizer& GetTokens() const final { return *_token_stream; }
  std::string_view Value() const final { return _value; }

 private:
  bool Write(irs::DataOutput&) const final { return false; }

  irs::analysis::Tokenizer::ptr _token_stream;
  T _value;
};

// field which uses simple analyzer without tokenization
class StringField : public tests::FieldBase {
 public:
  StringField(std::string_view name, irs::IndexFeatures extra_index_features =
                                       irs::IndexFeatures::None);
  StringField(
    std::string_view name, std::string_view value,
    irs::IndexFeatures extra_index_features = irs::IndexFeatures::None);

  void value(std::string_view str);
  std::string_view value() const { return _value; }

  irs::analysis::Tokenizer& GetTokens() const final;
  std::string_view Value() const final { return _value; }
  bool Write(irs::DataOutput& out) const final;

 private:
  mutable irs::StringTokenizer _stream;
  std::string _value;
};

// field which uses simple analyzer without tokenization
class StringViewField : public tests::FieldBase {
 public:
  StringViewField(const std::string& name,
                  irs::IndexFeatures index_features = irs::IndexFeatures::Freq |
                                                      irs::IndexFeatures::Pos);
  StringViewField(const std::string& name, const std::string_view& value,
                  irs::IndexFeatures index_features = irs::IndexFeatures::Freq |
                                                      irs::IndexFeatures::Pos);

  void value(std::string_view str);
  std::string_view value() const { return _value; }

  irs::analysis::Tokenizer& GetTokens() const final;
  std::string_view Value() const final { return _value; }
  bool Write(irs::DataOutput& out) const final;

 private:
  mutable irs::StringTokenizer _stream;
  std::string_view _value;
};

// document template for europarl.subset.text
class EuroparlDocTemplate : public DelimDocGenerator::DocTemplate {
 public:
  using text_ref_field = TextField<std::string_view>;

  void init() override;
  void value(size_t idx, const std::string& value) final;
  void end() final;
  void reset() final;

 private:
  std::string _title;  // current title
  std::string _body;   // current body
  irs::doc_id_t _idval = 0;
};

void GenericJsonFieldFactory(tests::Document& doc, const std::string& name,
                             const JsonDocGenerator::JsonValue& data);

void PayloadedJsonFieldFactory(tests::Document& doc, const std::string& name,
                               const JsonDocGenerator::JsonValue& data);

void NormalizedStringJsonFieldFactory(tests::Document& doc,
                                      const std::string& name,
                                      const JsonDocGenerator::JsonValue& data);

void NormStringJsonFieldFactory(tests::Document& doc, const std::string& name,
                                const JsonDocGenerator::JsonValue& data);

template<typename Indexed>
bool Insert(irs::IndexWriter& writer, Indexed ibegin, Indexed iend) {
  auto ctx = writer.GetBatch();
  if (!tests::InsertFields(ctx.Insert(), ibegin, iend)) {
    return false;
  }
  ctx.Commit();
  return true;
}

template<typename Doc>
bool Insert(irs::IndexWriter& writer, const Doc& doc, size_t count = 1) {
  for (; count; --count) {
    if (!Insert(writer, std::begin(doc.indexed), std::end(doc.indexed))) {
      return false;
    }
  }
  return true;
}

template<typename DocGenerator, typename ExpectedSegment>
bool InsertBatch(irs::IndexWriter& writer, DocGenerator& gen,
                 ExpectedSegment& segment, size_t batch_size) {
  auto ctx = writer.GetBatch();

  const tests::Document* src = nullptr;
  do {
    auto doc = ctx.Insert(false, batch_size);
    const auto current_last_doc_id = writer.BufferedDocs();

    size_t inserted_docs = 0;

    while (inserted_docs < batch_size && (src = gen.next()) != nullptr) {
      inserted_docs++;
      segment.insert(*src);
      if (!tests::InsertFields(doc, src->indexed.begin(), src->indexed.end())) {
        return false;
      };
      doc.NextDocument();
    }
    while (inserted_docs < batch_size) {
      SDB_ASSERT(src == nullptr);
      doc.Writer().remove(current_last_doc_id - batch_size + inserted_docs++);
    }
  } while (src != nullptr);
  ctx.Commit();
  return true;
}

template<typename Indexed>
bool Update(irs::IndexWriter& writer, const irs::Filter& filter, Indexed ibegin,
            Indexed iend) {
  auto ctx = writer.GetBatch();
  if (!tests::InsertFields(ctx.Replace(filter), ibegin, iend)) {
    return false;
  }
  ctx.Commit();
  return true;
}

template<typename Indexed>
bool Update(irs::IndexWriter& writer, irs::Filter::ptr&& filter, Indexed ibegin,
            Indexed iend) {
  auto ctx = writer.GetBatch();
  if (!tests::InsertFields(ctx.Replace(std::move(filter)), ibegin, iend)) {
    return false;
  }
  ctx.Commit();
  return true;
}

template<typename Indexed>
bool Update(irs::IndexWriter& writer,
            const std::shared_ptr<irs::Filter>& filter, Indexed ibegin,
            Indexed iend) {
  auto ctx = writer.GetBatch();
  if (!tests::InsertFields(ctx.Replace(filter), ibegin, iend)) {
    return false;
  }
  ctx.Commit();
  return true;
}

// One-shot removal: opens a batch, queues the filter and commits it.
// Equivalent to `auto b = writer.GetBatch(); b.Remove(filter); b.Commit();`.
template<bool TickBound = true, typename Filter>
void Remove(irs::IndexWriter& writer, Filter&& filter) {
  auto trx = writer.GetBatch();
  trx.template Remove<TickBound>(std::forward<Filter>(filter));
  trx.Commit();
}

}  // namespace tests
