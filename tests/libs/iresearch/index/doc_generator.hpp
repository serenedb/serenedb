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

#include <atomic>
#include <boost/iterator/iterator_facade.hpp>
#include <filesystem>
#include <fstream>
#include <functional>

#include "basics/down_cast.h"
#include "iresearch/analysis/analyzers.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/iterator.hpp"

namespace irs {

class DataOutput;
class Tokenizer;

}  // namespace irs

namespace tests {

//////////////////////////////////////////////////////////////////////////////
/// @class ifield
/// @brief base interface for all fields
//////////////////////////////////////////////////////////////////////////////
struct Ifield {
  using ptr = std::shared_ptr<Ifield>;
  virtual ~Ifield() = default;

  virtual irs::IndexFeatures GetIndexFeatures() const = 0;
  virtual irs::Tokenizer& GetTokens() const = 0;
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

  std::string_view Name() const noexcept final { return name; }

  void Name(std::string name) noexcept { this->name = std::move(name); }

  std::string name;
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

  irs::Tokenizer& GetTokens() const final;
  void value(value_t value) { _value = value; }
  value_t value() const { return _value; }
  bool Write(irs::DataOutput& out) const final;

 private:
  mutable irs::NumericTokenizer _stream;
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
    : _stream(std::make_shared<irs::NumericTokenizer>()), _value{value} {
    this->Name(std::string{name});
    this->index_features |= extra_features;
  }
  IntField(IntField&& other) = default;

  irs::Tokenizer& GetTokens() const final;
  void value(value_t value) { _value = value; }
  value_t value() const { return _value; }
  bool Write(irs::DataOutput& out) const final;

 private:
  mutable std::shared_ptr<irs::NumericTokenizer> _stream;
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

  irs::Tokenizer& GetTokens() const final;
  void value(value_t value) { _value = value; }
  value_t value() const { return _value; }
  bool Write(irs::DataOutput& out) const final;

 private:
  mutable irs::NumericTokenizer _stream;
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

  irs::Tokenizer& GetTokens() const final;
  void value(value_t value) { _value = value; }
  value_t value() const { return _value; }
  bool Write(irs::DataOutput& out) const final;

 private:
  mutable irs::NumericTokenizer _stream;
  float_t _value{};
};

//////////////////////////////////////////////////////////////////////////////
/// @class binary_field
/// @brief provides capabilities for storing & indexing binary values
//////////////////////////////////////////////////////////////////////////////
class BinaryField : public FieldBase {
 public:
  BinaryField() = default;

  irs::Tokenizer& GetTokens() const final;
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
    if (indexed)
      this->indexed.push_back(field);
    if (stored)
      this->stored.push_back(field);
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
  irs::analysis::Analyzer::ptr _stream;
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

// stream wrapper which sets payload equal to term value
class TokenizerPayload final : public irs::Tokenizer {
 public:
  explicit TokenizerPayload(irs::Tokenizer* impl);
  bool next() final;

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final;

 private:
  const irs::TermAttr* _term;
  irs::PayAttr _pay;
  irs::Tokenizer* _impl;
};

// field which uses text analyzer for tokenization and stemming
template<typename T>
class TextField : public tests::FieldBase {
 public:
  TextField(const std::string& name, bool payload = false,
            irs::IndexFeatures extra_features = irs::IndexFeatures::None)
    : _token_stream(irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"C\", \"stopwords\":[]}")) {
    if (payload) {
      if (!_token_stream->reset(_value)) {
        throw irs::IllegalState{"Failed to reset stream."};
      }
      _pay_stream.reset(new TokenizerPayload(_token_stream.get()));
    }
    this->Name(name);
    index_features = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                     irs::IndexFeatures::Offs | extra_features;
  }

  TextField(const std::string& name, const T& value, bool payload = false,
            irs::IndexFeatures extra_features = irs::IndexFeatures::None)
    : _token_stream(irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"C\", \"stopwords\":[]}")),
      _value(value) {
    if (payload) {
      if (!_token_stream->reset(_value)) {
        throw irs::IllegalState{"Failed to reset stream."};
      }
      _pay_stream.reset(new TokenizerPayload(_token_stream.get()));
    }
    this->Name(name);
    index_features = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                     irs::IndexFeatures::Offs | extra_features;
  }

  TextField(TextField&& other) = default;

  std::string_view value() const { return _value; }
  void value(const T& value) { _value = value; }
  void value(T&& value) { _value = std::move(value); }

  irs::Tokenizer& GetTokens() const final {
    _token_stream->reset(_value);

    return _pay_stream ? static_cast<irs::Tokenizer&>(*_pay_stream)
                       : *_token_stream;
  }

 private:
  bool Write(irs::DataOutput&) const final { return false; }

  std::unique_ptr<TokenizerPayload> _pay_stream;
  irs::analysis::Analyzer::ptr _token_stream;
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

  irs::Tokenizer& GetTokens() const final;
  bool Write(irs::DataOutput& out) const final;

 private:
  mutable irs::StringTokenizer _stream;
  std::string _value;
};

// field which uses simple analyzer without tokenization
class StringViewField : public tests::FieldBase {
 public:
  StringViewField(
    const std::string& name,
    irs::IndexFeatures extra_index_features = irs::IndexFeatures::None);
  StringViewField(const std::string& name, const std::string_view& value,
                  irs::IndexFeatures index_features = irs::IndexFeatures::None);

  void value(std::string_view str);
  std::string_view value() const { return _value; }

  irs::Tokenizer& GetTokens() const final;
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
  auto doc = ctx.Insert();

  return doc.Insert<irs::Action::INDEX>(ibegin, iend);
}

template<typename Indexed, typename Stored>
bool Insert(irs::IndexWriter& writer, Indexed ibegin, Indexed iend,
            Stored sbegin, Stored send) {
  auto ctx = writer.GetBatch();
  auto doc = ctx.Insert();

  return doc.Insert<irs::Action::INDEX>(ibegin, iend) &&
         doc.Insert<irs::Action::STORE>(sbegin, send);
}

template<typename Indexed, typename Stored, typename Sorted>
bool Insert(irs::IndexWriter& writer, Indexed ibegin, Indexed iend,
            Stored sbegin, Stored send, Sorted sorted = nullptr) {
  auto ctx = writer.GetBatch();
  auto doc = ctx.Insert();

  if (sorted && !doc.Insert<irs::Action::StoreSorted>(*sorted)) {
    return false;
  }

  return doc.Insert<irs::Action::INDEX>(ibegin, iend) &&
         doc.Insert<irs::Action::STORE>(sbegin, send);
}

template<typename Doc>
bool Insert(irs::IndexWriter& writer, const Doc& doc, size_t count = 1,
            bool has_sort = false) {
  for (; count; --count) {
    if (!Insert(writer, std::begin(doc.indexed), std::end(doc.indexed),
                std::begin(doc.stored), std::end(doc.stored),
                has_sort ? doc.sorted.get() : nullptr)) {
      return false;
    }
  }
  return true;
}

template<typename Doc>
bool InsertStoreBatch(irs::IndexWriter& writer, std::span<const Doc*> docs) {
  auto ctx = writer.GetBatch();
  auto doc = ctx.Insert(false, docs.size());

  for (const auto d : docs) {
    if (!doc.template Insert<irs::Action::INDEX>(d->indexed.begin(),
                                                 d->indexed.end())) {
      return false;
    };
    doc.NextDocument();
  }
  doc.NextFieldBatch();
  for (const auto d : docs) {
    if (!doc.template Insert<irs::Action::STORE>(d->stored.begin(),
                                                 d->stored.end())) {
      return false;
    };
    doc.NextDocument();
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
    // will need this to truncate not fully written batch as generator can not
    // say how many docs are left
    const auto current_last_doc_id = writer.BufferedDocs();

    size_t inserted_docs = 0;

    while (inserted_docs < batch_size && (src = gen.next()) != nullptr) {
      inserted_docs++;
      segment.insert(*src);
      if (src->sorted) {
        if (!doc.template Insert<irs::Action::StoreSorted>(*src->sorted)) {
          return false;
        }
      }
      if (!doc.template Insert<irs::Action::INDEX>(src->indexed.begin(),
                                                   src->indexed.end())) {
        return false;
      };
      if (!doc.template Insert<irs::Action::STORE>(src->stored.begin(),
                                                   src->stored.end())) {
        return false;
      };
      doc.NextDocument();
    }
    while (inserted_docs < batch_size) {
      SDB_ASSERT(src == nullptr);
      // truncate not fully used batch
      doc.Writer().remove(current_last_doc_id - batch_size + inserted_docs++);
    }
  } while (src != nullptr);
  return true;
}

template<typename Indexed>
bool Update(irs::IndexWriter& writer, const irs::Filter& filter, Indexed ibegin,
            Indexed iend) {
  auto ctx = writer.GetBatch();
  auto doc = ctx.Replace(filter);

  return doc.Insert<irs::Action::INDEX>(ibegin, iend);
}

template<typename Indexed, typename Stored>
bool Update(irs::IndexWriter& writer, const irs::Filter& filter, Indexed ibegin,
            Indexed iend, Stored sbegin, Stored send) {
  auto ctx = writer.GetBatch();
  auto doc = ctx.Replace(filter);

  return doc.Insert<irs::Action::INDEX>(ibegin, iend) &&
         doc.Insert<irs::Action::STORE>(sbegin, send);
}

template<typename Indexed>
bool Update(irs::IndexWriter& writer, irs::Filter::ptr&& filter, Indexed ibegin,
            Indexed iend) {
  auto ctx = writer.GetBatch();
  auto doc = ctx.Replace(std::move(filter));

  return doc.Insert<irs::Action::INDEX>(ibegin, iend);
}

template<typename Indexed, typename Stored>
bool Update(irs::IndexWriter& writer, irs::Filter::ptr&& filter, Indexed ibegin,
            Indexed iend, Stored sbegin, Stored send) {
  auto ctx = writer.GetBatch();
  auto doc = ctx.Replace(std::move(filter));

  return doc.Insert<irs::Action::INDEX>(ibegin, iend) &&
         doc.Insert<irs::Action::STORE>(sbegin, send);
}

template<typename Indexed>
bool Update(irs::IndexWriter& writer,
            const std::shared_ptr<irs::Filter>& filter, Indexed ibegin,
            Indexed iend) {
  auto ctx = writer.GetBatch();
  auto doc = ctx.Replace(filter);

  return doc.Insert<irs::Action::INDEX>(ibegin, iend);
}

template<typename Indexed, typename Stored>
bool Update(irs::IndexWriter& writer,
            const std::shared_ptr<irs::Filter>& filter, Indexed ibegin,
            Indexed iend, Stored sbegin, Stored send) {
  auto ctx = writer.GetBatch();
  auto doc = ctx.Replace(filter);

  return doc.Insert<irs::Action::INDEX>(ibegin, iend) &&
         doc.Insert<irs::Action::STORE>(sbegin, send);
}

}  // namespace tests
