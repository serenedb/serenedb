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

#include "doc_generator.hpp"

#include <rapidjson/istreamwrapper.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/reader.h>
#include <utf8.h>

#include <iomanip>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/index/field_data.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/store/store_utils.hpp>
#include <numeric>
#include <sstream>

#include "basics/file_utils_ext.hpp"
#include "utils/write_helpers.hpp"

namespace utf8 {
namespace unchecked {

template<typename OctetIterator>
class BreakIterator {
 public:
  using Utf8iterator = unchecked::iterator<OctetIterator>;

  using IteratorCategory = std::forward_iterator_tag;
  using ValueType = std::string;
  using Pointer = ValueType*;
  using Reference = ValueType&;
  using DifferenceType = void;

  BreakIterator(utf8::uint32_t delim, const OctetIterator& begin,
                const OctetIterator& end)
    : _delim(delim), _wbegin(begin), _wend(begin), _end(end) {
    if (!Done()) {
      Next();
    }
  }

  explicit BreakIterator(const OctetIterator& end)
    : _wbegin(end), _wend(end), _end(end) {}

  const std::string& operator*() const { return _res; }

  const std::string* operator->() const { return &_res; }

  bool operator==(const BreakIterator& rhs) const {
    SDB_ASSERT(_end == rhs._end);
    return (_wbegin == rhs._wbegin && _wend == rhs._wend);
  }

  bool Done() const { return _wbegin == _end; }

  BreakIterator& operator++() {
    Next();
    return *this;
  }

  BreakIterator operator++(int) {
    BreakIterator tmp(_delim, _wbegin, _end);
    Next();
    return tmp;
  }

 private:
  void Next() {
    _wbegin = _wend;
    _wend = std::find(_wbegin, _end, _delim);
    if (_wend != _end) {
      _res.assign(_wbegin.base(), _wend.base());
      ++_wend;
    } else {
      _res.assign(_wbegin.base(), _end.base());
    }
  }

  utf8::uint32_t _delim;
  std::string _res;
  Utf8iterator _wbegin;
  Utf8iterator _wend;
  Utf8iterator _end;
};

}  // namespace unchecked
}  // namespace utf8

namespace tests {

Document::Document(Document&& rhs) noexcept
  : indexed(std::move(rhs.indexed)),
    stored(std::move(rhs.stored)),
    sorted(std::move(rhs.sorted)) {}

irs::Tokenizer& LongField::GetTokens() const {
  _stream.reset(_value);
  return _stream;
}

bool LongField::Write(irs::DataOutput& out) const {
  irs::WriteZV64(out, _value);
  return true;
}

irs::Tokenizer& IntField::GetTokens() const {
  _stream->reset(_value);
  return *_stream;
}

bool IntField::Write(irs::DataOutput& out) const {
  irs::WriteZV32(out, _value);
  return true;
}

irs::Tokenizer& DoubleField::GetTokens() const {
  _stream.reset(_value);
  return _stream;
}

bool DoubleField::Write(irs::DataOutput& out) const {
  tests::WriteZvdouble(out, _value);
  return true;
}

irs::Tokenizer& FloatField::GetTokens() const {
  _stream.reset(_value);
  return _stream;
}

bool FloatField::Write(irs::DataOutput& out) const {
  tests::WriteZvfloat(out, _value);
  return true;
}

irs::Tokenizer& BinaryField::GetTokens() const {
  _stream.reset(irs::ViewCast<char, irs::byte_type>(_value));
  return _stream;
}

bool BinaryField::Write(irs::DataOutput& out) const {
  irs::WriteStr(out, _value);
  return true;
}

Particle::Particle(Particle&& rhs) noexcept : _fields(std::move(rhs._fields)) {}

Particle& Particle::operator=(Particle&& rhs) noexcept {
  if (this != &rhs) {
    _fields = std::move(rhs._fields);
  }

  return *this;
}

bool Particle::contains(const std::string_view& name) const {
  return absl::c_any_of(
    _fields, [&name](const Ifield::ptr& fld) { return name == fld->Name(); });
}

std::vector<Ifield::ptr> Particle::find(const std::string_view& name) const {
  std::vector<Ifield::ptr> fields;
  absl::c_for_each(_fields, [&fields, &name](Ifield::ptr fld) {
    if (name == fld->Name()) {
      fields.emplace_back(fld);
    }
  });

  return fields;
}

Ifield* Particle::get(const std::string_view& name) const {
  auto it = std::find_if(
    _fields.begin(), _fields.end(),
    [&name](const Ifield::ptr& fld) { return name == fld->Name(); });

  return _fields.end() == it ? nullptr : it->get();
}

void Particle::remove(const std::string_view& name) {
  std::erase_if(
    _fields, [&name](const Ifield::ptr& fld) { return name == fld->Name(); });
}

DelimDocGenerator::DelimDocGenerator(const std::filesystem::path& file,
                                     DocTemplate& doc,
                                     uint32_t delim /* = 0x0009 */)
  : _ifs(file.native(), std::ifstream::in | std::ifstream::binary),
    _doc(&doc),
    _delim(delim) {
  _doc->init();
  _doc->reset();
}

const Document* DelimDocGenerator::next() {
  if (!getline(_ifs, _str)) {
    return nullptr;
  }

  {
    const std::string::const_iterator end =
      utf8::find_invalid(_str.begin(), _str.end());
    if (end != _str.end()) {
      /* invalid utf8 string */
      return nullptr;
    }
  }

  using WordIterator =
    utf8::unchecked::BreakIterator<std::string::const_iterator>;

  const WordIterator end(_str.end());
  WordIterator begin(_delim, _str.begin(), _str.end());
  for (size_t i = 0; begin != end; ++begin, ++i) {
    _doc->value(i, *begin);
  }
  _doc->end();
  return _doc;
}

void DelimDocGenerator::reset() {
  _ifs.clear();
  _ifs.seekg(_ifs.beg);
  _doc->reset();
}

CsvDocGenerator::CsvDocGenerator(const std::filesystem::path& file,
                                 DocTemplate& doc)
  : _doc(doc),
    _ifs(file.native(), std::ifstream::in | std::ifstream::binary),
    _stream(irs::analysis::analyzers::Get(
      "delimiter", irs::Type<irs::text_format::Text>::get(), ",")) {
  _doc.init();
  _doc.reset();
}

const Document* CsvDocGenerator::next() {
  if (!getline(_ifs, _line) || !_stream) {
    return nullptr;
  }

  auto* term = irs::get<irs::TermAttr>(*_stream);

  if (!term || !_stream->reset(_line)) {
    return nullptr;
  }

  for (size_t i = 0; _stream->next(); ++i) {
    _doc.value(i, irs::ViewCast<char>(term->value));
  }

  return &_doc;
}

void CsvDocGenerator::reset() {
  _ifs.clear();
  _ifs.seekg(_ifs.beg);
  _doc.reset();
}

bool CsvDocGenerator::skip() { return false == !getline(_ifs, _line); }

//////////////////////////////////////////////////////////////////////////////
/// @class parse_json_handler
/// @brief rapdijson campatible visitor for
///        JSON document-derived column value types
//////////////////////////////////////////////////////////////////////////////
class ParseJsonHandler : irs::util::Noncopyable {
 public:
  typedef std::vector<Document> DocumentsT;

  ParseJsonHandler(const JsonDocGenerator::factory_f& factory, DocumentsT& docs)
    : _factory(factory), _docs(docs) {}

  bool Null() {
    _val.vt = JsonDocGenerator::ValueType::NIL;
    AddField();
    return true;
  }

  bool Bool(bool b) {
    _val.vt = JsonDocGenerator::ValueType::BOOL;
    _val.b = b;
    AddField();
    return true;
  }

  bool Int(int i) {
    _val.vt = JsonDocGenerator::ValueType::INT;
    _val.i = i;
    AddField();
    return true;
  }

  bool Uint(unsigned u) {
    _val.vt = JsonDocGenerator::ValueType::UINT;
    _val.ui = u;
    AddField();
    return true;
  }

  bool Int64(int64_t i) {
    _val.vt = JsonDocGenerator::ValueType::INT64;
    _val.i64 = i;
    AddField();
    return true;
  }

  bool Uint64(uint64_t u) {
    _val.vt = JsonDocGenerator::ValueType::UINT64;
    _val.ui64 = u;
    AddField();
    return true;
  }

  bool Double(double d) {
    _val.vt = JsonDocGenerator::ValueType::DBL;
    _val.dbl = d;
    AddField();
    return true;
  }

  bool RawNumber(const char* str, rapidjson::SizeType length, bool /*copy*/) {
    _val.vt = JsonDocGenerator::ValueType::RAWNUM;
    _val.str = std::string_view(str, length);
    AddField();
    return true;
  }

  bool String(const char* str, rapidjson::SizeType length, bool /*copy*/) {
    _val.vt = JsonDocGenerator::ValueType::STRING;
    _val.str = std::string_view(str, length);
    AddField();
    return true;
  }

  bool StartObject() {
    if (1 == _level) {
      _docs.emplace_back();
    }

    ++_level;
    return true;
  }

  bool StartArray() {
    ++_level;
    return true;
  }

  bool Key(const char* str, rapidjson::SizeType length, bool) {
    if (_level - 1 > _path.size()) {
      _path.emplace_back(str, length);
    } else {
      _path.back().assign(str, length);
    }
    return true;
  }

  bool EndObject(rapidjson::SizeType /*memberCount*/) {
    --_level;

    if (!_path.empty()) {
      _path.pop_back();
    }
    return true;
  }

  bool EndArray(rapidjson::SizeType element_count) {
    return EndObject(element_count);
  }

 private:
  void AddField() { _factory(_docs.back(), _path.back(), _val); }

  const JsonDocGenerator::factory_f& _factory;
  DocumentsT& _docs;
  std::vector<std::string> _path;
  size_t _level{};
  JsonDocGenerator::JsonValue _val;
};

JsonDocGenerator::JsonDocGenerator(const std::filesystem::path& file,
                                   const JsonDocGenerator::factory_f& factory) {
  std::ifstream input(std::filesystem::path(file).string().c_str(),
                      std::ios::in | std::ios::binary);
  SDB_ASSERT(input);

  rapidjson::IStreamWrapper stream(input);
  ParseJsonHandler handler(factory, _docs);
  rapidjson::Reader reader;

  [[maybe_unused]] const auto res = reader.Parse(stream, handler);
  SDB_ASSERT(!res.IsError());

  _next = _docs.begin();
}

JsonDocGenerator::JsonDocGenerator(const char* data,
                                   const JsonDocGenerator::factory_f& factory) {
  SDB_ASSERT(data);

  rapidjson::StringStream stream(data);
  ParseJsonHandler handler(factory, _docs);
  rapidjson::Reader reader;

  [[maybe_unused]] const auto res = reader.Parse(stream, handler);
  SDB_ASSERT(!res.IsError());

  _next = _docs.begin();
}

JsonDocGenerator::JsonDocGenerator(JsonDocGenerator&& rhs) noexcept
  : _docs(std::move(rhs._docs)),
    _prev(std::move(rhs._prev)),
    _next(std::move(rhs._next)) {}

const Document* JsonDocGenerator::next() {
  if (_docs.end() == _next) {
    return nullptr;
  }

  _prev = _next, ++_next;
  return &*_prev;
}

void JsonDocGenerator::reset() { _next = _docs.begin(); }

TokenizerPayload::TokenizerPayload(irs::Tokenizer* impl) : _impl(impl) {
  SDB_ASSERT(_impl);
  _term = irs::get<irs::TermAttr>(*_impl);
  SDB_ASSERT(_term);
}

irs::Attribute* TokenizerPayload::GetMutable(irs::TypeInfo::type_id type) {
  if (irs::Type<irs::PayAttr>::id() == type) {
    return &_pay;
  }

  return _impl->GetMutable(type);
}

bool TokenizerPayload::next() {
  if (_impl->next()) {
    _pay.value = _term->value;
    return true;
  }
  _pay.value = {};
  return false;
}

StringField::StringField(std::string_view name,
                         irs::IndexFeatures index_features) {
  this->index_features =
    (irs::IndexFeatures::Freq | irs::IndexFeatures::Pos) | index_features;
  this->name = name;
}

StringField::StringField(std::string_view name, std::string_view value,
                         irs::IndexFeatures index_features)
  : _value(value) {
  this->index_features =
    (irs::IndexFeatures::Freq | irs::IndexFeatures::Pos) | index_features;
  this->name = name;
}

// reject too long terms
void StringField::value(std::string_view str) {
  const auto size_len =
    irs::bytes_io<uint32_t>::vsize(irs::byte_block_pool::block_type::kSize);
  const auto max_len = std::min<size_t>(
    str.size(), irs::byte_block_pool::block_type::kSize - size_len);
  auto begin = str.begin();
  auto end = str.begin() + max_len;
  _value.assign(begin, end);
}

bool StringField::Write(irs::DataOutput& out) const {
  irs::WriteStr(out, _value);
  return true;
}

irs::Tokenizer& StringField::GetTokens() const {
  _stream.reset(_value);
  return _stream;
}

StringViewField::StringViewField(const std::string& name,
                                 irs::IndexFeatures extra_index_features) {
  this->index_features =
    (irs::IndexFeatures::Freq | irs::IndexFeatures::Pos) | extra_index_features;
  this->name = name;
}

StringViewField::StringViewField(const std::string& name,
                                 const std::string_view& value,
                                 irs::IndexFeatures extra_index_features)
  : _value(value) {
  this->index_features =
    (irs::IndexFeatures::Freq | irs::IndexFeatures::Pos) | extra_index_features;
  this->name = name;
}

// truncate very long terms
void StringViewField::value(std::string_view str) {
  const auto size_len =
    irs::bytes_io<uint32_t>::vsize(irs::byte_block_pool::block_type::kSize);
  const auto max_len = std::min<size_t>(
    str.size(), irs::byte_block_pool::block_type::kSize - size_len);

  _value = std::string_view(str.data(), max_len);
}

bool StringViewField::Write(irs::DataOutput& out) const {
  irs::WriteStr(out, _value);
  return true;
}

irs::Tokenizer& StringViewField::GetTokens() const {
  _stream.reset(_value);
  return _stream;
}

void EuroparlDocTemplate::init() {
  clear();
  indexed.push_back(std::make_shared<StringField>("title"));
  indexed.push_back(std::make_shared<text_ref_field>("title_anl", false));
  indexed.push_back(std::make_shared<text_ref_field>("title_anl_pay", true));
  indexed.push_back(std::make_shared<text_ref_field>("body_anl", false));
  indexed.push_back(std::make_shared<text_ref_field>("body_anl_pay", true));
  {
    insert(std::make_shared<LongField>());
    auto& field = static_cast<LongField&>(indexed.back());
    field.Name("date");
  }
  insert(std::make_shared<StringField>("datestr"));
  insert(std::make_shared<StringField>("body"));
  {
    insert(std::make_shared<IntField>());
    auto& field = static_cast<IntField&>(indexed.back());
    field.Name("id");
  }
  insert(std::make_shared<StringField>("idstr"));
}

void EuroparlDocTemplate::value(size_t idx, const std::string& value) {
  static auto gEtTime = [](const std::string& src) {
    std::istringstream ss(src);
    std::tm tmb{};
    char c;
    ss >> tmb.tm_year >> c >> tmb.tm_mon >> c >> tmb.tm_mday;
    return std::mktime(&tmb);
  };

  switch (idx) {
    case 0:  // title
      _title = value;
      indexed.get<StringField>("title")->value(_title);
      indexed.get<text_ref_field>("title_anl")->value(_title);
      indexed.get<text_ref_field>("title_anl_pay")->value(_title);
      break;
    case 1:  // date
      indexed.get<LongField>("date")->value(gEtTime(value));
      indexed.get<StringField>("datestr")->value(value);
      break;
    case 2:  // body
      _body = value;
      indexed.get<StringField>("body")->value(_body);
      indexed.get<text_ref_field>("body_anl")->value(_body);
      indexed.get<text_ref_field>("body_anl_pay")->value(_body);
      break;
  }
}

void EuroparlDocTemplate::end() {
  ++_idval;
  indexed.get<IntField>("id")->value(_idval);
  indexed.get<StringField>("idstr")->value(std::to_string(_idval));
}

void EuroparlDocTemplate::reset() { _idval = 0; }

void GenericJsonFieldFactory(Document& doc, const std::string& name,
                             const JsonDocGenerator::JsonValue& data) {
  if (JsonDocGenerator::ValueType::STRING == data.vt) {
    doc.insert(std::make_shared<StringField>(name, data.str));
  } else if (JsonDocGenerator::ValueType::NIL == data.vt) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.value(
      irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
  } else if (JsonDocGenerator::ValueType::BOOL == data.vt && data.b) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.value(
      irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_true()));
  } else if (JsonDocGenerator::ValueType::BOOL == data.vt && !data.b) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.value(
      irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_true()));
  } else if (data.is_number()) {
    // 'value' can be interpreted as a double
    doc.insert(std::make_shared<DoubleField>());
    auto& field = (doc.indexed.end() - 1).as<DoubleField>();
    field.Name(name);
    field.value(data.as_number<double_t>());
  }
}

void PayloadedJsonFieldFactory(Document& doc, const std::string& name,
                               const JsonDocGenerator::JsonValue& data) {
  using TextField = TextField<std::string>;

  if (JsonDocGenerator::ValueType::STRING == data.vt) {
    // Analyzed field with payload
    doc.indexed.push_back(std::make_shared<TextField>(
      std::string(name.c_str()) + "_anl_pay", data.str, true));

    // Analyzed field
    doc.indexed.push_back(std::make_shared<TextField>(
      std::string(name.c_str()) + "_anl", data.str));

    // Not analyzed field
    doc.insert(std::make_shared<StringField>(name, data.str));
  } else if (JsonDocGenerator::ValueType::NIL == data.vt) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.value(
      irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
  } else if (JsonDocGenerator::ValueType::BOOL == data.vt && data.b) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.value(
      irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_true()));
  } else if (JsonDocGenerator::ValueType::BOOL == data.vt && !data.b) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.value(
      irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_false()));
  } else if (data.is_number()) {
    // 'value' can be interpreted as a double
    doc.insert(std::make_shared<DoubleField>());
    auto& field = (doc.indexed.end() - 1).as<DoubleField>();
    field.Name(name);
    field.value(data.as_number<double_t>());
  }
}

void NormalizedStringJsonFieldFactory(Document& doc, const std::string& name,
                                      const JsonDocGenerator::JsonValue& data) {
  if (JsonDocGenerator::ValueType::STRING == data.vt) {
    doc.insert(
      std::make_shared<StringField>(name, data.str, irs::IndexFeatures::Norm));
  } else {
    GenericJsonFieldFactory(doc, name, data);
  }
}

void NormStringJsonFieldFactory(Document& doc, const std::string& name,
                                const JsonDocGenerator::JsonValue& data) {
  return NormalizedStringJsonFieldFactory(doc, name, data);
}

}  // namespace tests
