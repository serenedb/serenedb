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

#include <absl/container/flat_hash_map.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/reader.h>
#include <utf8.h>

#include <cassert>
#include <iomanip>
#include <numeric>
#include <sstream>

#include "basics/file_utils_ext.hpp"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/index/field_data.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/store/store_utils.hpp"
#include "utils/write_helpers.hpp"

namespace tests {

// Hashmap-backed runtime equivalent of the constexpr `FieldIdFor` if-chain in
// the header. The JSON factories below call this per doc per field, so an
// O(1) hash lookup wins over walking 35 string compares. The entries must
// stay in lockstep with the constexpr table; the debug assert on every hit
// keeps the two honest.
irs::field_id FieldIdForRuntime(std::string_view name) {
  static const absl::flat_hash_map<std::string_view, irs::field_id> kTable = {
    {"seq", 1},
    {"name", 2},
    {"same", 3},
    {"duplicated", 4},
    {"value", 5},
    {"field", 6},
    {"phrase", 7},
    {"name_anl", 8},
    {"name_anl_pay", 9},
    {"prefix", 10},
    {"title", 11},
    {"title_anl", 12},
    {"title_anl_pay", 13},
    {"body", 14},
    {"body_anl", 15},
    {"body_anl_pay", 16},
    {"date", 17},
    {"datestr", 18},
    {"id", 19},
    {"idstr", 20},
    {"test-field", 21},
    {"test", 22},
    {"name1", 23},
    {"phrase_anl", 24},
    {"foo", 25},
    {"doc_bytes", 26},
    {"doc_double", 27},
    {"doc_float", 28},
    {"doc_int", 29},
    {"doc_long", 30},
    {"doc_string", 31},
    {"doc_text", 32},
    {"another_column", 33},
    {"label", 34},
    {"updated", 35},
  };
  if (auto it = kTable.find(name); it != kTable.end()) {
    // Sanity: hashmap and constexpr table agree.
    SDB_ASSERT(it->second == FieldIdFor(name));
    return it->second;
  }
  // Unknown name -- fall through to the constexpr FNV fallback so the two
  // code paths produce identical ids.
  return FieldIdFor(name);
}

}  // namespace tests
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

bool Particle::contains_by_id(irs::field_id id) const noexcept {
  return absl::c_any_of(
    _fields, [id](const Ifield::ptr& fld) { return id == fld->Id(); });
}

Ifield* Particle::get_by_id(irs::field_id id) const noexcept {
  auto it =
    std::find_if(_fields.begin(), _fields.end(),
                 [id](const Ifield::ptr& fld) { return id == fld->Id(); });

  return _fields.end() == it ? nullptr : it->get();
}

bool Particle::remove_by_id(irs::field_id id) noexcept {
  return std::erase_if(_fields, [id](const Ifield::ptr& fld) {
           return id == fld->Id();
         }) > 0;
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
    _stream(std::make_unique<irs::analysis::DelimitedTokenizer>(",")) {
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

irs::Attribute* TokenizerPayload::GetMutable(
  irs::TypeInfo::type_id type) noexcept {
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
                                 irs::IndexFeatures index_features) {
  this->index_features = index_features;
  this->name = name;
}

StringViewField::StringViewField(const std::string& name,
                                 const std::string_view& value,
                                 irs::IndexFeatures index_features)
  : _value(value) {
  this->index_features = index_features;
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
  // SegmentWriter buckets fields by `field.Id()`; without a valid id every
  // field collides on `field_limits::invalid()` and `index(...)` rejects the
  // doc on the IsSubsetOf-features check. Assign a stable id per name via
  // the canonical mapping.
  {
    auto f = std::make_shared<StringField>("title");
    f->id = tests::FieldIdFor("title");
    indexed.push_back(std::move(f));
  }
  {
    auto f = std::make_shared<text_ref_field>("title_anl", false);
    f->id = tests::FieldIdFor("title_anl");
    indexed.push_back(std::move(f));
  }
  {
    auto f = std::make_shared<text_ref_field>("title_anl_pay", true);
    f->id = tests::FieldIdFor("title_anl_pay");
    indexed.push_back(std::move(f));
  }
  {
    auto f = std::make_shared<text_ref_field>("body_anl", false);
    f->id = tests::FieldIdFor("body_anl");
    indexed.push_back(std::move(f));
  }
  {
    auto f = std::make_shared<text_ref_field>("body_anl_pay", true);
    f->id = tests::FieldIdFor("body_anl_pay");
    indexed.push_back(std::move(f));
  }
  {
    insert(std::make_shared<LongField>());
    auto& field = static_cast<LongField&>(indexed.back());
    field.Name("date");
    field.id = tests::FieldIdFor("date");
  }
  {
    auto f = std::make_shared<StringField>("datestr");
    f->id = tests::FieldIdFor("datestr");
    insert(std::move(f));
  }
  {
    auto f = std::make_shared<StringField>("body");
    f->id = tests::FieldIdFor("body");
    insert(std::move(f));
  }
  {
    insert(std::make_shared<IntField>());
    auto& field = static_cast<IntField&>(indexed.back());
    field.Name("id");
    field.id = tests::FieldIdFor("id");
  }
  {
    auto f = std::make_shared<StringField>("idstr");
    f->id = tests::FieldIdFor("idstr");
    insert(std::move(f));
  }
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
      indexed.get_by_id<StringField>(tests::FieldIdFor("title"))->value(_title);
      indexed.get_by_id<text_ref_field>(tests::FieldIdFor("title_anl"))
        ->value(_title);
      indexed.get_by_id<text_ref_field>(tests::FieldIdFor("title_anl_pay"))
        ->value(_title);
      break;
    case 1:  // date
      indexed.get_by_id<LongField>(tests::FieldIdFor("date"))
        ->value(gEtTime(value));
      indexed.get_by_id<StringField>(tests::FieldIdFor("datestr"))
        ->value(value);
      break;
    case 2:  // body
      _body = value;
      indexed.get_by_id<StringField>(tests::FieldIdFor("body"))->value(_body);
      indexed.get_by_id<text_ref_field>(tests::FieldIdFor("body_anl"))
        ->value(_body);
      indexed.get_by_id<text_ref_field>(tests::FieldIdFor("body_anl_pay"))
        ->value(_body);
      break;
  }
}

void EuroparlDocTemplate::end() {
  ++_idval;
  indexed.get_by_id<IntField>(tests::FieldIdFor("id"))->value(_idval);
  indexed.get_by_id<StringField>(tests::FieldIdFor("idstr"))
    ->value(std::to_string(_idval));
}

void EuroparlDocTemplate::reset() { _idval = 0; }

void GenericJsonFieldFactory(Document& doc, const std::string& name,
                             const JsonDocGenerator::JsonValue& data) {
  // Every JSON-driven field must carry a stable id so the writer can bucket
  // it (SegmentWriter indexes by `field.Id()`). Look up the canonical id
  // for `name`; unknown names hash via the FNV-1a fallback inside
  // `tests::FieldIdForRuntime`. The runtime variant is hashmap-backed for
  // O(1) lookup on this per-doc, per-field hot path.
  const auto id = tests::FieldIdForRuntime(name);
  if (JsonDocGenerator::ValueType::STRING == data.vt) {
    auto f = std::make_shared<StringField>(name, data.str);
    f->id = id;
    doc.insert(std::move(f));
  } else if (JsonDocGenerator::ValueType::NIL == data.vt) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.id = id;
    field.value(
      irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
  } else if (JsonDocGenerator::ValueType::BOOL == data.vt && data.b) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.id = id;
    field.value(
      irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_true()));
  } else if (JsonDocGenerator::ValueType::BOOL == data.vt && !data.b) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.id = id;
    field.value(
      irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_false()));
  } else if (data.is_number()) {
    // 'value' can be interpreted as a double
    doc.insert(std::make_shared<DoubleField>());
    auto& field = (doc.indexed.end() - 1).as<DoubleField>();
    field.Name(name);
    field.id = id;
    field.value(data.as_number<double_t>());
  }
}

void PayloadedJsonFieldFactory(Document& doc, const std::string& name,
                               const JsonDocGenerator::JsonValue& data) {
  using TextField = TextField<std::string>;

  if (JsonDocGenerator::ValueType::STRING == data.vt) {
    // Analyzed field with payload
    const auto anl_pay_name = std::string(name.c_str()) + "_anl_pay";
    auto anl_pay = std::make_shared<TextField>(anl_pay_name, data.str, true);
    anl_pay->id = tests::FieldIdForRuntime(anl_pay_name);
    doc.indexed.push_back(std::move(anl_pay));

    // Analyzed field
    const auto anl_name = std::string(name.c_str()) + "_anl";
    auto anl = std::make_shared<TextField>(anl_name, data.str);
    anl->id = tests::FieldIdForRuntime(anl_name);
    doc.indexed.push_back(std::move(anl));

    // Not analyzed field
    auto plain = std::make_shared<StringField>(name, data.str);
    plain->id = tests::FieldIdForRuntime(name);
    doc.insert(std::move(plain));
  } else if (JsonDocGenerator::ValueType::NIL == data.vt) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.id = tests::FieldIdForRuntime(name);
    field.value(
      irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
  } else if (JsonDocGenerator::ValueType::BOOL == data.vt && data.b) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.id = tests::FieldIdForRuntime(name);
    field.value(
      irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_true()));
  } else if (JsonDocGenerator::ValueType::BOOL == data.vt && !data.b) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.id = tests::FieldIdForRuntime(name);
    field.value(
      irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_false()));
  } else if (data.is_number()) {
    // 'value' can be interpreted as a double
    doc.insert(std::make_shared<DoubleField>());
    auto& field = (doc.indexed.end() - 1).as<DoubleField>();
    field.Name(name);
    field.id = tests::FieldIdForRuntime(name);
    field.value(data.as_number<double_t>());
  }
}

void NormalizedStringJsonFieldFactory(Document& doc, const std::string& name,
                                      const JsonDocGenerator::JsonValue& data) {
  if (JsonDocGenerator::ValueType::STRING == data.vt) {
    auto f =
      std::make_shared<StringField>(name, data.str, irs::IndexFeatures::Norm);
    f->id = tests::FieldIdForRuntime(name);
    doc.insert(std::move(f));
  } else {
    GenericJsonFieldFactory(doc, name, data);
  }
}

// Short-name alias for NormalizedStringJsonFieldFactory; both names are used
// by different test suites.
void NormStringJsonFieldFactory(Document& doc, const std::string& name,
                                const JsonDocGenerator::JsonValue& data) {
  return NormalizedStringJsonFieldFactory(doc, name, data);
}

}  // namespace tests
