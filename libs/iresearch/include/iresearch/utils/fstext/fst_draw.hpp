// See www.openfst.org for extensive documentation on this weighted
// finite-state transducer library.
//
// Class to draw a binary FST by producing a text file in dot format, a helper
// class to fstdraw.cc.

#pragma once

#include <absl/strings/internal/ostringstream.h>
#include <fst/fst.h>
#include <fst/symbol-table.h>
#include <fst/util.h>

#include <ostream>
#include <sstream>
#include <string>

namespace fst {

template<typename Arc>
struct LabelToString {
  std::string operator()(const Arc& arc, typename Arc::Label label,
                         std::string_view) const {
    std::string str;
    absl::strings_internal::OStringStream{&str} << arc;
    return str;
  }
};

// Print a binary FST in GraphViz textual format (helper class for fstdraw.cc).
// WARNING: Stand-alone use not recommend.
template<class Arc, class LabelToString>
class FstDrawer {
 public:
  using Label = typename Arc::Label;
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;

  FstDrawer(const Fst<Arc>& fst, const LabelToString& label_to_string,
            const SymbolTable* isyms, const SymbolTable* osyms,
            const SymbolTable* ssyms, bool accep, const std::string& title,
            float width, float height, bool portrait, bool vertical,
            float ranksep, float nodesep, int fontsize, int precision,
            const std::string& float_format, bool show_weight_one)
    : _fst(fst),
      _isyms(isyms),
      _osyms(osyms),
      _ssyms(ssyms),
      _accep(accep && fst.Properties(kAcceptor, true)),
      _ostrm(nullptr),
      _title(title),
      _width(width),
      _height(height),
      _portrait(portrait),
      _vertical(vertical),
      _ranksep(ranksep),
      _nodesep(nodesep),
      _fontsize(fontsize),
      _precision(precision),
      _float_format(float_format),
      _show_weight_one(show_weight_one) {}

  // Draws FST to an output buffer.
  void Draw(std::ostream* strm, const std::string& dest) {
    _ostrm = strm;
    SetStreamState(_ostrm);
    _dest = dest;
    StateId start = _fst.Start();
    if (start == kNoStateId) {
      return;
    }
    PrintString("digraph FST {\n");
    if (_vertical) {
      PrintString("rankdir = BT;\n");
    } else {
      PrintString("rankdir = LR;\n");
    }
    PrintString("size = \"");
    Print(_width);
    PrintString(",");
    Print(_height);
    PrintString("\";\n");
    if (!_title.empty()) {
      PrintString("label = \"" + _title + "\";\n");
    }
    PrintString("center = 1;\n");
    if (_portrait) {
      PrintString("orientation = Portrait;\n");
    } else {
      PrintString("orientation = Landscape;\n");
    }
    PrintString("ranksep = \"");
    Print(_ranksep);
    PrintString("\";\n");
    PrintString("nodesep = \"");
    Print(_nodesep);
    PrintString("\";\n");
    // Initial state first.
    DrawState(start);
    for (StateIterator<Fst<Arc>> siter(_fst); !siter.Done(); siter.Next()) {
      const auto s = siter.Value();
      if (s != start) {
        DrawState(s);
      }
    }
    PrintString("}\n");
  }

 private:
  void SetStreamState(std::ostream* strm) const {
    strm->precision(_precision);
    if (_float_format == "e") {
      strm->setf(std::ios_base::scientific, std::ios_base::floatfield);
    }
    if (_float_format == "f") {
      strm->setf(std::ios_base::fixed, std::ios_base::floatfield);
    }
    // O.w. defaults to "g" per standard lib.
  }

  void PrintString(const std::string& str) const { *_ostrm << str; }

  // Escapes backslash and double quote if these occur in the string. Dot
  // will not deal gracefully with these if they are not escaped.
  static std::string Escape(const std::string& str) {
    std::string ns;
    for (char c : str) {
      if (c == '\\' || c == '"') {
        ns.push_back('\\');
      }
      ns.push_back(c);
    }
    return ns;
  }

  void PrintId(StateId id, const SymbolTable* syms, const char* name) const {
    if (syms) {
      auto symbol = syms->Find(id);
      if (symbol.empty()) {
        FSTERROR() << "FstDrawer: Integer " << id
                   << " is not mapped to any textual symbol"
                   << ", symbol table = " << syms->Name()
                   << ", destination = " << _dest;
        symbol = "?";
      }
      PrintString(Escape(symbol));
    } else {
      PrintString(std::to_string(id));
    }
  }

  void PrintStateId(StateId s) const { PrintId(s, _ssyms, "state ID"); }

  void PrintILabel(const Arc& arc) const {
    PrintLabel(arc, arc.ilabel, _isyms, "arc input label");
  }

  void PrintOLabel(const Arc& arc) const {
    PrintLabel(arc, arc.olabel, _osyms, "arc output label");
  }

  void PrintWeight(Weight w) const {
    // Weight may have double quote characters in it, so escape it.
    PrintString(Escape(ToString(w)));
  }

  template<class T>
  void Print(T t) const {
    *_ostrm << t;
  }

  template<class T>
  void PrintLabel(const Arc& arc, T label, const SymbolTable* syms,
                  std::string_view name) const {
    if (syms) {
      if (auto symbol = syms->Find(label); !symbol.empty()) {
        return PrintString(Escape(symbol));
      }
    }
    PrintString(_label_to_string(arc, label, name));
  }

  template<class T>
  std::string ToString(T t) const {
    std::string ss_str;
    absl::strings_internal::OStringStream ss{&ss_str};
    SetStreamState(&ss);
    ss << t;
    return ss_str;
  }

  void DrawState(StateId s) const {
    Print(s);
    PrintString(" [label = \"");
    PrintStateId(s);
    const auto weight = _fst.Final(s);
    if (weight != Weight::Zero()) {
      if (_show_weight_one || (weight != Weight::One())) {
        PrintString("/");
        PrintWeight(weight);
      }
      PrintString("\", shape = doublecircle,");
    } else {
      PrintString("\", shape = circle,");
    }
    if (s == _fst.Start()) {
      PrintString(" style = bold,");
    } else {
      PrintString(" style = solid,");
    }
    PrintString(" fontsize = ");
    Print(_fontsize);
    PrintString("]\n");
    for (ArcIterator<Fst<Arc>> aiter(_fst, s); !aiter.Done(); aiter.Next()) {
      const auto& arc = aiter.Value();
      PrintString("\t");
      Print(s);
      PrintString(" -> ");
      Print(arc.nextstate);
      PrintString(" [label = \"");
      PrintILabel(arc);
      if (!_accep) {
        PrintString(":");
        PrintOLabel(arc);
      }
      if (_show_weight_one || (arc.weight != Weight::One())) {
        PrintString("/");
        PrintWeight(arc.weight);
      }
      PrintString("\", fontsize = ");
      Print(_fontsize);
      PrintString("];\n");
    }
  }

  const Fst<Arc>& _fst;
  LabelToString _label_to_string;
  const SymbolTable* _isyms;  // ilabel symbol table.
  const SymbolTable* _osyms;  // olabel symbol table.
  const SymbolTable* _ssyms;  // slabel symbol table.
  bool _accep;                // Print as acceptor when possible.
  std::ostream* _ostrm;       // Drawn FST destination.
  std::string _dest;          // Drawn FST destination name.

  std::string _title;
  float _width;
  float _height;
  bool _portrait;
  bool _vertical;
  float _ranksep;
  float _nodesep;
  int _fontsize;
  int _precision;
  std::string _float_format;
  bool _show_weight_one;

  FstDrawer(const FstDrawer&) = delete;
  FstDrawer& operator=(const FstDrawer&) = delete;
};

template<typename Fst,
         typename LabelToString = fst::LabelToString<typename Fst::Arc>>
inline void drawFst(
  const Fst& fst, std::ostream& strm, const LabelToString& label_to_string = {},
  const std::string& dest = "", const SymbolTable* isyms = nullptr,
  const SymbolTable* osyms = nullptr, const SymbolTable* ssyms = nullptr,
  bool accep = false, const std::string& title = "", float width = 11,
  float height = 8.5, bool partrait = true, bool vertical = false,
  float randsep = 0.4, float nodesep = 0.25, int fontsize = 14,
  int precision = 5, const std::string& float_format = "g",
  bool show_weight_one = false) {
  FstDrawer<typename Fst::Arc, LabelToString> drawer(
    fst, label_to_string, isyms, osyms, ssyms, accep, title, width, height,
    partrait, vertical, randsep, nodesep, fontsize, precision, float_format,
    show_weight_one);

  drawer.Draw(&strm, dest);
}

template<typename Fst,
         typename LabelToString = fst::LabelToString<typename Fst::Arc>>
inline bool drawFst(const Fst& fst, const std::string& dest,
                    const LabelToString& label_to_string = {},
                    const SymbolTable* isyms = nullptr,
                    const SymbolTable* osyms = nullptr,
                    const SymbolTable* ssyms = nullptr, bool accep = false,
                    const std::string& title = "", float width = 11,
                    float height = 8.5, bool partrait = true,
                    bool vertical = false, float randsep = 0.4,
                    float nodesep = 0.25, int fontsize = 14, int precision = 5,
                    const std::string& float_format = "g",
                    bool show_weight_one = false) {
  std::fstream stream;
  stream.open(dest, std::fstream::binary | std::fstream::out);
  if (!stream) {
    return false;
  }

  fst::drawFst(fst, stream, label_to_string, dest, isyms, osyms, ssyms, accep,
               title, width, height, partrait, vertical, randsep, nodesep,
               fontsize, precision, float_format, show_weight_one);

  return true;
}

}  // namespace fst
