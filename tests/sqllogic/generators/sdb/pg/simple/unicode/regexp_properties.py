from sqllogic_gen import Test, lit

t = Test("Every Unicode property of ICU's property tables, with its values, "
         "aliases and malformed spellings, matched by regexp_full_match against "
         "every code point: how many match, the lowest and highest, and sums that "
         "pin the exact set.")

BINARY = [
    ('Alphabetic', 'Alpha'), ('ASCII_Hex_Digit', 'AHex'), ('Bidi_Control', 'Bidi_C'),
    ('Bidi_Mirrored', 'Bidi_M'), ('Dash', 'Dash'), ('Default_Ignorable_Code_Point', 'DI'),
    ('Deprecated', 'Dep'), ('Diacritic', 'Dia'), ('Extender', 'Ext'),
    ('Full_Composition_Exclusion', 'Comp_Ex'), ('Grapheme_Base', 'Gr_Base'),
    ('Grapheme_Extend', 'Gr_Ext'), ('Grapheme_Link', 'Gr_Link'), ('Hex_Digit', 'Hex'),
    ('Hyphen', 'Hyphen'), ('ID_Continue', 'IDC'), ('ID_Start', 'IDS'), ('Ideographic', 'Ideo'),
    ('IDS_Binary_Operator', 'IDSB'), ('IDS_Trinary_Operator', 'IDST'), ('Join_Control', 'Join_C'),
    ('Logical_Order_Exception', 'LOE'), ('Lowercase', 'Lower'), ('Math', 'Math'),
    ('Noncharacter_Code_Point', 'NChar'), ('Quotation_Mark', 'QMark'), ('Radical', 'Radical'),
    ('Soft_Dotted', 'SD'), ('Terminal_Punctuation', 'Term'), ('Unified_Ideograph', 'UIdeo'),
    ('Uppercase', 'Upper'), ('space', 'WSpace'), ('XID_Continue', 'XIDC'), ('XID_Start', 'XIDS'),
    ('Case_Sensitive', 'Sensitive'), ('Sentence_Terminal', 'STerm'), ('Variation_Selector', 'VS'),
    ('NFD_Inert', 'nfdinert'), ('NFKD_Inert', 'nfkdinert'), ('NFC_Inert', 'nfcinert'),
    ('NFKC_Inert', 'nfkcinert'), ('Segment_Starter', 'segstart'), ('Pattern_Syntax', 'Pat_Syn'),
    ('Pattern_White_Space', 'Pat_WS'), ('alnum', 'alnum'), ('blank', 'blank'), ('graph', 'graph'),
    ('print', 'print'), ('xdigit', 'xdigit'), ('Cased', 'Cased'), ('Case_Ignorable', 'CI'),
    ('Changes_When_Lowercased', 'CWL'), ('Changes_When_Uppercased', 'CWU'),
    ('Changes_When_Titlecased', 'CWT'), ('Changes_When_Casefolded', 'CWCF'),
    ('Changes_When_Casemapped', 'CWCM'), ('Changes_When_NFKC_Casefolded', 'CWKCF'),
    ('Emoji', 'Emoji'), ('Emoji_Presentation', 'EPres'), ('Emoji_Modifier', 'EMod'),
    ('Emoji_Modifier_Base', 'EBase'), ('Emoji_Component', 'EComp'), ('Regional_Indicator', 'RI'),
    ('Prepended_Concatenation_Mark', 'PCM'), ('Extended_Pictographic', 'ExtPict'),
    ('Basic_Emoji', 'Basic_Emoji'), ('Emoji_Keycap_Sequence', 'Emoji_Keycap_Sequence'),
    ('RGI_Emoji_Modifier_Sequence', 'RGI_Emoji_Modifier_Sequence'),
    ('RGI_Emoji_Flag_Sequence', 'RGI_Emoji_Flag_Sequence'),
    ('RGI_Emoji_Tag_Sequence', 'RGI_Emoji_Tag_Sequence'),
    ('RGI_Emoji_ZWJ_Sequence', 'RGI_Emoji_ZWJ_Sequence'), ('RGI_Emoji', 'RGI_Emoji'),
    ('IDS_Unary_Operator', 'IDSU'), ('ID_Compat_Math_Start', 'ID_Compat_Math_Start'),
    ('ID_Compat_Math_Continue', 'ID_Compat_Math_Continue'), ('Modifier_Combining_Mark', 'MCM'),
]

ENUMS = [
    ('bc', [
        'L', 'R', 'EN', 'ES', 'ET', 'AN', 'CS', 'B', 'S', 'WS', 'ON', 'LRE', 'LRO', 'AL', 'RLE',
        'RLO', 'PDF', 'NSM', 'BN', 'FSI', 'LRI', 'RLI', 'PDI',
    ]),
    ('blk', [
        'NB', 'Greek', 'Bengali', 'Sinhala', 'Cherokee', 'Punctuation', 'Misc_Technical',
        'Dingbats', 'Bopomofo', 'Yi_Syllables', 'Alphabetic_PF', 'Old_Italic', 'Tags',
        'Sup_Arrows_B', 'Tai_Le', 'Ugaritic', 'Arabic_Sup', 'Glagolitic', 'Tifinagh', 'Cuneiform',
        'Cyrillic_Ext_B', 'Carian', 'Lisu', 'Meetei_Mayek', 'Rumi', 'Ethiopic_Ext_A', 'Alchemical',
        'Miao', 'Diacriticals_Ext', 'Linear_A', 'Old_North_Arabian', 'Siddham', 'CJK_Ext_E',
        'Bhaiksuki', 'Tangut', 'Zanabazar_Square', 'Mayan_Numerals', 'Ottoman_Siyaq_Numbers',
        'Khitan_Small_Script', 'Kana_Ext_B', 'Znamenny_Music', 'CJK_Ext_I',
        'Symbols_For_Legacy_Computing_Sup', 'Tai_Yo',
    ]),
    ('ccc', [
        'NR', 'OV', 'HANR', 'NK', 'KV', 'VR', 'CCC10', 'CCC11', 'CCC12', 'CCC13', 'CCC14', 'CCC15',
        'CCC16', 'CCC17', 'CCC18', 'CCC19', 'CCC20', 'CCC21', 'CCC22', 'CCC23', 'CCC24', 'CCC25',
        'CCC26', 'CCC27', 'CCC28', 'CCC29', 'CCC30', 'CCC31', 'CCC32', 'CCC33', 'CCC34', 'CCC35',
        'CCC36', 'CCC84', 'CCC91', 'CCC103', 'CCC107', 'CCC118', 'CCC122', 'CCC129', 'CCC130',
        'CCC132', 'CCC133', 'ATBL', 'ATB', 'ATA', 'ATAR', 'BL', 'B', 'BR', 'L', 'R', 'AL', 'A',
        'AR', 'DB', 'DA', 'IS',
    ]),
    ('dt', [
        'None', 'Can', 'Com', 'Enc', 'Fin', 'Font', 'Fra', 'Init', 'Iso', 'Med', 'Nar', 'Nb', 'Sml',
        'Sqr', 'Sub', 'Sup', 'Vert', 'Wide',
    ]),
    ('ea', [
        'N', 'A', 'H', 'F', 'Na', 'W',
    ]),
    ('gc', [
        'Cn', 'Lu', 'Ll', 'Lt', 'Lm', 'Lo', 'Mn', 'Me', 'Mc', 'Nd', 'Nl', 'No', 'Zs', 'Zl', 'Zp',
        'Cc', 'Cf', 'Co', 'Cs', 'Pd', 'Ps', 'Pe', 'Pc', 'Po', 'Sm', 'Sc', 'Sk', 'So', 'Pi', 'Pf',
    ]),
    ('jg', [
        'No_Joining_Group', 'Alaph', 'Beh', 'Dal', 'E', 'Final_Semkath', 'Gamal',
        'Teh_Marbuta_Goal', 'Heh', 'Heth', 'Kaph', 'Lam', 'Meem', 'Noon', 'Pe', 'Qaph',
        'Reversed_Pe', 'Sadhe', 'Semkath', 'Swash_Kaf', 'Tah', 'Teh_Marbuta', 'Waw', 'Yeh_Barree',
        'Yudh', 'Zain', 'Khaph', 'Burushaski_Yeh_Barree', 'Nya', 'Manichaean_Aleph',
        'Manichaean_Beth', 'Manichaean_Dhamedh', 'Manichaean_Gimel', 'Manichaean_Hundred',
        'Manichaean_Lamedh', 'Manichaean_Nun', 'Manichaean_Pe', 'Manichaean_Resh',
        'Manichaean_Samekh', 'Manichaean_Ten', 'Manichaean_Thamedh', 'Manichaean_Waw',
        'Manichaean_Zayin', 'African_Feh', 'African_Qaf', 'Malayalam_Ja', 'Malayalam_Llla',
        'Malayalam_Nna', 'Malayalam_Nya', 'Malayalam_Ssa', 'Hanifi_Rohingya_Kinna_Ya', 'Thin_Yeh',
        'Kashmiri_Yeh',
    ]),
    ('jt', [
        'U', 'C', 'D', 'L', 'R', 'T',
    ]),
    ('lb', [
        'XX', 'AI', 'AL', 'B2', 'BA', 'BB', 'BK', 'CB', 'CL', 'CM', 'CR', 'EX', 'GL', 'HY', 'ID',
        'IN', 'IS', 'LF', 'NS', 'NU', 'OP', 'PO', 'PR', 'QU', 'SA', 'SG', 'SP', 'SY', 'ZW', 'NL',
        'WJ', 'H2', 'H3', 'JL', 'JT', 'JV', 'CP', 'CJ', 'HL', 'RI', 'EB', 'EM', 'ZWJ', 'AK', 'AP',
        'AS', 'VF', 'VI', 'HH',
    ]),
    ('nt', [
        'None', 'De', 'Di', 'Nu',
    ]),
    ('sc', [
        'Zyyy', 'Bopo', 'Deva', 'Gujr', 'Hira', 'Latn', 'Ital', 'Taml', 'Cans', 'Tagb', 'Osma',
        'Bugi', 'Tfng', 'Brah', 'Egyh', 'Hmng', 'Latf', 'Maya', 'Phag', 'Syre', 'Visp', 'Jpan',
        'Rjng', 'Mtei', 'Kthi', 'Prti', 'Bamu', 'Dupl', 'Mend', 'Sind', 'Nshu', 'Wole', 'Mahj',
        'Pauc', 'Newa', 'Gonm', 'Maka', 'Elym', 'Diak', 'Tnsa', 'Aran', 'Sunu', 'Tayo',
    ]),
    ('hst', [
        'NA', 'L', 'V', 'T', 'LV', 'LVT',
    ]),
    ('NFD_QC', [
        'N', 'Y',
    ]),
    ('NFKD_QC', [
        'N', 'Y',
    ]),
    ('NFC_QC', [
        'N', 'Y', 'M',
    ]),
    ('NFKC_QC', [
        'N', 'Y', 'M',
    ]),
    ('lccc', [
        'NR', 'OV', 'HANR', 'NK', 'KV', 'VR', 'CCC10', 'CCC11', 'CCC12', 'CCC13', 'CCC14', 'CCC15',
        'CCC16', 'CCC17', 'CCC18', 'CCC19', 'CCC20', 'CCC21', 'CCC22', 'CCC23', 'CCC24', 'CCC25',
        'CCC26', 'CCC27', 'CCC28', 'CCC29', 'CCC30', 'CCC31', 'CCC32', 'CCC33', 'CCC34', 'CCC35',
        'CCC36', 'CCC84', 'CCC91', 'CCC103', 'CCC107', 'CCC118', 'CCC122', 'CCC129', 'CCC130',
        'CCC132', 'CCC133', 'ATBL', 'ATB', 'ATA', 'ATAR', 'BL', 'B', 'BR', 'L', 'R', 'AL', 'A',
        'AR', 'DB', 'DA', 'IS',
    ]),
    ('tccc', [
        'NR', 'OV', 'HANR', 'NK', 'KV', 'VR', 'CCC10', 'CCC11', 'CCC12', 'CCC13', 'CCC14', 'CCC15',
        'CCC16', 'CCC17', 'CCC18', 'CCC19', 'CCC20', 'CCC21', 'CCC22', 'CCC23', 'CCC24', 'CCC25',
        'CCC26', 'CCC27', 'CCC28', 'CCC29', 'CCC30', 'CCC31', 'CCC32', 'CCC33', 'CCC34', 'CCC35',
        'CCC36', 'CCC84', 'CCC91', 'CCC103', 'CCC107', 'CCC118', 'CCC122', 'CCC129', 'CCC130',
        'CCC132', 'CCC133', 'ATBL', 'ATB', 'ATA', 'ATAR', 'BL', 'B', 'BR', 'L', 'R', 'AL', 'A',
        'AR', 'DB', 'DA', 'IS',
    ]),
    ('GCB', [
        'XX', 'CN', 'CR', 'EX', 'L', 'LF', 'LV', 'LVT', 'T', 'V', 'SM', 'PP', 'RI', 'EB', 'EBG',
        'EM', 'GAZ', 'ZWJ',
    ]),
    ('SB', [
        'XX', 'AT', 'CL', 'FO', 'LO', 'NU', 'LE', 'SE', 'SP', 'ST', 'UP', 'CR', 'EX', 'LF', 'SC',
    ]),
    ('WB', [
        'XX', 'LE', 'FO', 'KA', 'ML', 'MN', 'NU', 'EX', 'CR', 'Extend', 'LF', 'MB', 'NL', 'RI',
        'HL', 'SQ', 'DQ', 'EB', 'EBG', 'EM', 'GAZ', 'ZWJ', 'WSegSpace',
    ]),
    ('bpt', [
        'n', 'o', 'c',
    ]),
    ('InPC', [
        'NA', 'Bottom', 'Bottom_And_Left', 'Bottom_And_Right', 'Left', 'Left_And_Right',
        'Overstruck', 'Right', 'Top', 'Top_And_Bottom', 'Top_And_Bottom_And_Right', 'Top_And_Left',
        'Top_And_Left_And_Right', 'Top_And_Right', 'Visual_Order_Left', 'Top_And_Bottom_And_Left',
    ]),
    ('InSC', [
        'Other', 'Avagraha', 'Bindu', 'Brahmi_Joining_Number', 'Cantillation_Mark', 'Consonant',
        'Consonant_Dead', 'Consonant_Final', 'Consonant_Head_Letter', 'Consonant_Initial_Postfixed',
        'Consonant_Killer', 'Consonant_Medial', 'Consonant_Placeholder',
        'Consonant_Preceding_Repha', 'Consonant_Prefixed', 'Consonant_Subjoined',
        'Consonant_Succeeding_Repha', 'Consonant_With_Stacker', 'Gemination_Mark',
        'Invisible_Stacker', 'Joiner', 'Modifying_Letter', 'Non_Joiner', 'Nukta', 'Number',
        'Number_Joiner', 'Pure_Killer', 'Register_Shifter', 'Syllable_Modifier', 'Tone_Letter',
        'Tone_Mark', 'Virama', 'Visarga', 'Vowel', 'Vowel_Dependent', 'Vowel_Independent',
        'Reordering_Killer',
    ]),
    ('vo', [
        'R', 'Tr', 'Tu', 'U',
    ]),
    ('ID_Status', [
        'Restricted', 'Allowed',
    ]),
    ('InCB', [
        'None', 'Consonant', 'Extend', 'Linker',
    ]),
    ('gcm', [
        'Cn', 'Lu', 'Ll', 'Lt', 'LC', 'Lm', 'Lo', 'L', 'Mn', 'Me', 'Mc', 'M', 'Nd', 'Nl', 'No', 'N',
        'Zs', 'Zl', 'Zp', 'Z', 'Cc', 'Cf', 'Co', 'Cs', 'C', 'Pd', 'Ps', 'Pe', 'Pc', 'Po', 'Sm',
        'Sc', 'Sk', 'So', 'S', 'Pi', 'Pf', 'P',
    ]),
    ('ID_Type', [
        'Not_Character', 'Deprecated', 'Default_Ignorable', 'Not_NFKC', 'Not_XID', 'Exclusion',
        'Obsolete', 'Technical', 'Uncommon_Use', 'Limited_Use', 'Inclusion', 'Recommended',
    ]),
]

SCRIPTS = [
    ('Zyyy', 'Common'), ('Bopo', 'Bopomofo'), ('Deva', 'Devanagari'), ('Gujr', 'Gujarati'),
    ('Hira', 'Hiragana'), ('Latn', 'Latin'), ('Ital', 'Old_Italic'), ('Taml', 'Tamil'),
    ('Cans', 'Canadian_Aboriginal'), ('Tagb', 'Tagbanwa'), ('Osma', 'Osmanya'),
    ('Bugi', 'Buginese'), ('Tfng', 'Tifinagh'), ('Brah', 'Brahmi'), ('Egyh', 'Egyh'),
    ('Hmng', 'Pahawh_Hmong'), ('Latf', 'Latf'), ('Maya', 'Maya'), ('Phag', 'Phags_Pa'),
    ('Syre', 'Syre'), ('Visp', 'Visp'), ('Jpan', 'Jpan'), ('Rjng', 'Rejang'),
    ('Mtei', 'Meetei_Mayek'), ('Kthi', 'Kaithi'), ('Prti', 'Inscriptional_Parthian'),
    ('Bamu', 'Bamum'), ('Dupl', 'Duployan'), ('Mend', 'Mende_Kikakui'), ('Sind', 'Khudawadi'),
    ('Nshu', 'Nushu'), ('Wole', 'Wole'), ('Mahj', 'Mahajani'), ('Pauc', 'Pau_Cin_Hau'),
    ('Newa', 'Newa'), ('Gonm', 'Masaram_Gondi'), ('Maka', 'Makasar'), ('Elym', 'Elymaic'),
    ('Diak', 'Dives_Akuru'), ('Tnsa', 'Tangsa'), ('Aran', 'Aran'), ('Sunu', 'Sunuwar'),
    ('Tayo', 'Tai_Yo'),
]

LATE_ENUMS = [
    ('Identifier_Type', [
        'Not_Character', 'Deprecated', 'Default_Ignorable', 'Not_NFKC', 'Not_XID', 'Exclusion',
        'Obsolete', 'Technical', 'Uncommon_Use', 'Limited_Use', 'Inclusion', 'Recommended',
    ]),
    ('Age', [
        '1.1', '2.0', '3.2', '4.1', '5.2', '6.0', '7.0', '9.0', '11.0', '13.0', '15.0', '15.1',
        '16.0', '17.0', '3', '3.2.0.0', ' 4 . 1 ', 'V3_2',
    ]),
    ('nv', [
        '0', '1', '0.5', '-0.5', '1/2', '1000000000000', '12', '-123456789', '0x10', '1e3', 'abc',
    ]),
]

EXTRA = [
    '\\p{ccc=230}', '\\p{ccc= 230}', '\\p{ccc=230 }', '\\p{ccc=0x10}', '\\p{ccc=2.3e2}',
    '\\p{ccc=256}', '\\p{ccc=-1}', '\\p{ccc=1.5}', '\\p{ccc=Above}', '\\p{L}', '\\p{Lu}', '\\p{LC}',
    '\\p{C}', '\\p{Cn}', '\\p{Co}', '\\p{Cs}', '\\p{Letter}', '\\p{Uppercase_Letter}',
    '\\p{Cased_Letter}', '\\p{punct}', '\\p{digit}', '\\p{cntrl}', '\\p{Latn}', '\\p{Greek}',
    '\\p{Zyyy}', '\\p{Inherited}', '\\p{Unknown}', '\\p{Hrkt}', '\\p{Any}', '\\p{ASCII}',
    '\\p{Assigned}', '\\p{Lower}', '\\p{Upper}', '\\p{White_Space}', '\\p{l_u}', '\\p{ lu }',
    '\\p{L-U}', '\\p{UPPERCASE_LETTER}', '\\p{gc=L}', '\\p{General_Category=Letter}',
    '\\p{Script=Latin}', '\\p{sc=latn}', '\\p{blk=ASCII}', '\\p{Block=Basic Latin}',
    '\\p{Name=LATIN SMALL LETTER A}', '\\p{na1=X}', '\\p{InPC}', '\\p{bmg=X}', '\\p{Script}',
    '\\p{Script=}', '\\p{=L}', '\\p{L=}', '\\p{Alphabetic=Lu}', '\\p{Alphabetic=Y}',
    '\\p{Alphabetic=True}', '\\p{nonsense}', '\\p{L#}', '\\p{L@}', '\\p{L$}', '\\p{Llé}',
    '\\p{InGreek}', '\\p{isL}', '\\p{Is_Alphabetic}',
]

SYNTAX = [
    '\\pL', '\\PL', '\\p{^L}', '\\P{^L}', '[\\p{L}\\p{N}]', '[^\\p{L}]', '\\pN',
]

FOLDED = [
    '(?i)\\p{Lu}',
]

FULL = ("SELECT count(*) AS n, min(c) AS lo, max(c) AS hi, sum(c) AS total, "
        "sum(c::BIGINT * c) AS squares FROM rp_cp WHERE regexp_full_match(s, '{}');")
SHORT = ("SELECT count(*) AS n, min(c) AS lo, max(c) AS hi, sum(c) AS total "
         "FROM rp_cp WHERE regexp_full_match(s, '{}');")
COUNT = "SELECT count(*) AS n FROM rp_cp WHERE regexp_full_match(s, '{}');"
PART = 250

patterns = [p for name, alias in BINARY for p in (f"\\p{{{name}}}", f"\\p{{{alias}=No}}")]
patterns += [f"\\p{{{prop}={value}}}" for prop, values in ENUMS for value in values]
patterns += [p for short, name in SCRIPTS for p in (f"\\p{{scx={short}}}", f"\\p{{{name}}}")]
patterns += [f"\\p{{{prop}={value}}}" for prop, values in LATE_ENUMS for value in values]
patterns += EXTRA
checks = [(FULL, p, ["n", "lo", "hi", "total", "squares"]) for p in patterns]
checks += [(SHORT, p, ["n", "lo", "hi", "total"]) for p in SYNTAX]
checks += [(COUNT, p, ["n"]) for p in FOLDED]

t.statement('CREATE TABLE rp_cp AS\n    SELECT c::INTEGER AS c, chr(c::INTEGER) AS s FROM range(0, 55296) t(c)\n    UNION ALL\n    SELECT c::INTEGER AS c, chr(c::INTEGER) AS s FROM range(57344, 1114112) t(c);')
for chunk in t.parts(checks, PART):
    for sql, pattern, columns in chunk:
        t.query(sql.format(lit(pattern)), columns)
    t.statement("DROP TABLE rp_cp;")

t.write()
