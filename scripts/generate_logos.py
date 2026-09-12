#!/usr/bin/env python3
"""Generate the SereneDB logo family in resources/images/.

Every sub-brand logo is the same artwork: the SereneDB mark and wordmark from
resources/images/serenedb-{dark,light}.svg, a vertical divider, and one word
set in DM Sans Light. This script builds them all from that one base so the
mark, palette, height, divider geometry and type are identical by construction
rather than by hand-matching.

The sub-brand word is converted to outline paths, not left as a <text> element.
An SVG that names a font renders differently wherever that font is missing --
in the assets this replaced, the word silently disappeared in any renderer
without DM Sans installed, and fell back to a different sans in browsers.
Outlines make the file self-contained, so the font is only needed here.

DM Sans is downloaded from Google Fonts into memory on each run. Nothing is
written outside resources/images/. Pass --font to use a local copy instead.

Run from anywhere:

    python3 scripts/generate_logos.py
    python3 scripts/generate_logos.py --font /path/to/DMSans.ttf

Requires fonttools:

    sudo apt install python3-fonttools
"""

import argparse
import io
import pathlib
import re
import sys
import urllib.request

try:
    from fontTools.misc.transform import Transform
    from fontTools.pens.svgPathPen import SVGPathPen
    from fontTools.pens.transformPen import TransformPen
    from fontTools.ttLib import TTFont
    from fontTools.varLib import instancer
except ImportError as exc:
    sys.exit(f"{exc}\n\nInstall it with:\n    sudo apt install python3-fonttools")

ROOT = pathlib.Path(__file__).resolve().parent.parent
IMAGES = ROOT / "resources" / "images"

FONT_URL = (
    "https://raw.githubusercontent.com/google/fonts/main/ofl/dmsans/"
    "DMSans%5Bopsz%2Cwght%5D.ttf"
)
FONT_AXES = {"opsz": 9, "wght": 300}

# Geometry shared by every sub-brand logo. The base artwork is 303x60 with the
# wordmark ending near x=303, so the divider sits at 316 and the word at 328.
FONT_SIZE = 47.0
LETTER_SPACING = -0.5
TEXT_X = 328.0
BASELINE = 47.0
DIVIDER_X = 316.0
DIVIDER_TOP = 14.0
DIVIDER_BOTTOM = 46.0
DIVIDER_OPACITY = "0.25"
HEIGHT = 60

# Ink per variant: dark logos go on dark backgrounds, so the type is white.
INK = {"dark": "white", "light": "black"}

# Output stem -> the word after the divider.
SUB_BRANDS = {
    "examples": "Examples",
    "iresearch": "IResearch",
    "serene-ui": "SereneUI",
    "serene-docs-search": "SereneDocsSearch",
}


def load_font(local):
    """Return DM Sans Light as a TTFont. Nothing is written to disk."""
    if local:
        blob = pathlib.Path(local).read_bytes()
    else:
        print(f"downloading {FONT_URL}")
        with urllib.request.urlopen(FONT_URL, timeout=60) as response:
            blob = response.read()

    font = TTFont(io.BytesIO(blob))
    if "fvar" in font:
        font = instancer.instantiateVariableFont(font, FONT_AXES, inplace=False)
    return font


def kern_pairs(font):
    """Map (left glyph, right glyph) -> x-advance adjustment from GPOS 'kern'.

    The wordmarks are short enough that pair kerning is the only shaping that
    matters, so reading it straight out of GPOS avoids depending on a shaper.
    """
    gpos = font.get("GPOS")
    if not gpos:
        return {}

    table = gpos.table
    lookups = set()
    for record in table.FeatureList.FeatureRecord:
        if record.FeatureTag == "kern":
            lookups.update(record.Feature.LookupListIndex)

    pairs = {}
    for index in sorted(lookups):
        lookup = table.LookupList.Lookup[index]
        for subtable in lookup.SubTable:
            # Lookup type 9 wraps the real subtable to reach past 16-bit offsets.
            if lookup.LookupType == 9:
                subtable = subtable.ExtSubTable
            if subtable.__class__.__name__ != "PairPos":
                continue
            if subtable.Format == 1:
                for left, pair_set in zip(subtable.Coverage.glyphs, subtable.PairSet):
                    for record in pair_set.PairValueRecord:
                        value = getattr(record.Value1, "XAdvance", 0) or 0
                        if value:
                            pairs[(left, record.SecondGlyph)] = value
            elif subtable.Format == 2:
                classes1 = subtable.ClassDef1.classDefs
                classes2 = subtable.ClassDef2.classDefs
                for left in subtable.Coverage.glyphs:
                    class1 = classes1.get(left, 0)
                    if class1 >= len(subtable.Class1Record):
                        continue
                    records = subtable.Class1Record[class1].Class2Record
                    for right, class2 in classes2.items():
                        if class2 >= len(records):
                            continue
                        value = getattr(records[class2].Value1, "XAdvance", 0) or 0
                        if value:
                            pairs[(left, right)] = value
    return pairs


def outline(font, kerning, word):
    """Lay `word` out as outline paths and return (path data, end x)."""
    glyph_set = font.getGlyphSet()
    cmap = font.getBestCmap()
    hmtx = font["hmtx"]
    scale = FONT_SIZE / font["head"].unitsPerEm

    names = []
    for char in word:
        if ord(char) not in cmap:
            sys.exit(f"font has no glyph for {char!r}")
        names.append(cmap[ord(char)])

    pen_x = TEXT_X
    commands = []
    for index, name in enumerate(names):
        # Fonts are y-up, SVG is y-down, so the transform flips y and lands the
        # glyph on the baseline.
        transform = Transform(scale, 0, 0, -scale, pen_x, BASELINE)
        pen = SVGPathPen(glyph_set)
        glyph_set[name].draw(TransformPen(pen, transform))
        data = pen.getCommands()
        if data:
            commands.append(data)
        advance = hmtx[name][0]
        if index + 1 < len(names):
            advance += kerning.get((name, names[index + 1]), 0)
        pen_x += advance * scale + LETTER_SPACING

    return " ".join(commands), pen_x - LETTER_SPACING


def build(font, kerning, word, variant):
    base = (IMAGES / f"serenedb-{variant}.svg").read_text()
    ink = INK[variant]
    path_data, end_x = outline(font, kerning, word)
    width = int(round(end_x))

    svg = re.sub(
        rf'^<svg width="\d+" height="{HEIGHT}" viewBox="0 0 \d+ {HEIGHT}"',
        f'<svg width="{width}" height="{HEIGHT}" viewBox="0 0 {width} {HEIGHT}"',
        base,
        count=1,
    )
    if svg == base:
        sys.exit(f"serenedb-{variant}.svg does not have the expected <svg> header")

    addition = (
        f'<line x1="{DIVIDER_X}" y1="{DIVIDER_TOP}"'
        f' x2="{DIVIDER_X}" y2="{DIVIDER_BOTTOM}"'
        f' stroke="{ink}" stroke-opacity="{DIVIDER_OPACITY}" stroke-width="1"/>\n'
        f'<path d="{path_data}" fill="{ink}"/>\n'
    )
    return svg.replace("</svg>", addition + "</svg>")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--font", help="use this font file instead of downloading")
    args = parser.parse_args()

    font = load_font(args.font)
    kerning = kern_pairs(font)

    for stem, word in SUB_BRANDS.items():
        for variant in INK:
            target = IMAGES / f"{stem}-{variant}.svg"
            target.write_text(build(font, kerning, word, variant))
            print(f"  {target.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
