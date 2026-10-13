CREATE MACRO zeroblob(n) AS unhex(repeat('00', n));
CREATE MACRO iif(c, a, b) AS CASE WHEN CAST(c AS BOOLEAN) THEN a ELSE b END;
WITH RECURSIVE hex(int, blob) AS (
    SELECT 0 AS int, X'00' AS blob UNION SELECT 1 AS int, X'01' AS blob UNION SELECT 2 AS int, X'02' AS blob UNION SELECT 3 AS int, X'03' AS blob UNION SELECT 4 AS int, X'04' AS blob UNION SELECT 5 AS int, X'05' AS blob UNION SELECT 6 AS int, X'06' AS blob UNION SELECT 7 AS int, X'07' AS blob UNION SELECT 8 AS int, X'08' AS blob UNION SELECT 9 AS int, X'09' AS blob UNION SELECT 10 AS int, X'0a' AS blob UNION SELECT 11 AS int, X'0b' AS blob UNION SELECT 12 AS int, X'0c' AS blob UNION SELECT 13 AS int, X'0d' AS blob UNION SELECT 14 AS int, X'0e' AS blob UNION SELECT 15 AS int, X'0f' AS blob UNION SELECT 16 AS int, X'10' AS blob UNION SELECT 17 AS int, X'11' AS blob UNION SELECT 18 AS int, X'12' AS blob UNION SELECT 19 AS int, X'13' AS blob UNION SELECT 20 AS int, X'14' AS blob UNION SELECT 21 AS int, X'15' AS blob UNION SELECT 22 AS int, X'16' AS blob UNION SELECT 23 AS int, X'17' AS blob UNION SELECT 24 AS int, X'18' AS blob UNION SELECT 25 AS int, X'19' AS blob UNION SELECT 26 AS int, X'1a' AS blob UNION SELECT 27 AS int, X'1b' AS blob UNION SELECT 28 AS int, X'1c' AS blob UNION SELECT 29 AS int, X'1d' AS blob UNION SELECT 30 AS int, X'1e' AS blob UNION SELECT 31 AS int, X'1f' AS blob UNION SELECT 32 AS int, X'20' AS blob UNION SELECT 33 AS int, X'21' AS blob UNION SELECT 34 AS int, X'22' AS blob UNION SELECT 35 AS int, X'23' AS blob UNION SELECT 36 AS int, X'24' AS blob UNION SELECT 37 AS int, X'25' AS blob UNION SELECT 38 AS int, X'26' AS blob UNION SELECT 39 AS int, X'27' AS blob UNION SELECT 40 AS int, X'28' AS blob UNION SELECT 41 AS int, X'29' AS blob UNION SELECT 42 AS int, X'2a' AS blob UNION SELECT 43 AS int, X'2b' AS blob UNION SELECT 44 AS int, X'2c' AS blob UNION SELECT 45 AS int, X'2d' AS blob UNION SELECT 46 AS int, X'2e' AS blob UNION SELECT 47 AS int, X'2f' AS blob UNION SELECT 48 AS int, X'30' AS blob UNION SELECT 49 AS int, X'31' AS blob UNION SELECT 50 AS int, X'32' AS blob UNION SELECT 51 AS int, X'33' AS blob UNION SELECT 52 AS int, X'34' AS blob UNION SELECT 53 AS int, X'35' AS blob UNION SELECT 54 AS int, X'36' AS blob UNION SELECT 55 AS int, X'37' AS blob UNION SELECT 56 AS int, X'38' AS blob UNION SELECT 57 AS int, X'39' AS blob UNION SELECT 58 AS int, X'3a' AS blob UNION SELECT 59 AS int, X'3b' AS blob UNION SELECT 60 AS int, X'3c' AS blob UNION SELECT 61 AS int, X'3d' AS blob UNION SELECT 62 AS int, X'3e' AS blob UNION SELECT 63 AS int, X'3f' AS blob UNION SELECT 64 AS int, X'40' AS blob UNION SELECT 65 AS int, X'41' AS blob UNION SELECT 66 AS int, X'42' AS blob UNION SELECT 67 AS int, X'43' AS blob UNION SELECT 68 AS int, X'44' AS blob UNION SELECT 69 AS int, X'45' AS blob UNION SELECT 70 AS int, X'46' AS blob UNION SELECT 71 AS int, X'47' AS blob UNION SELECT 72 AS int, X'48' AS blob UNION SELECT 73 AS int, X'49' AS blob UNION SELECT 74 AS int, X'4a' AS blob UNION SELECT 75 AS int, X'4b' AS blob UNION SELECT 76 AS int, X'4c' AS blob UNION SELECT 77 AS int, X'4d' AS blob UNION SELECT 78 AS int, X'4e' AS blob UNION SELECT 79 AS int, X'4f' AS blob UNION SELECT 80 AS int, X'50' AS blob UNION SELECT 81 AS int, X'51' AS blob UNION SELECT 82 AS int, X'52' AS blob UNION SELECT 83 AS int, X'53' AS blob UNION SELECT 84 AS int, X'54' AS blob UNION SELECT 85 AS int, X'55' AS blob UNION SELECT 86 AS int, X'56' AS blob UNION SELECT 87 AS int, X'57' AS blob UNION SELECT 88 AS int, X'58' AS blob UNION SELECT 89 AS int, X'59' AS blob UNION SELECT 90 AS int, X'5a' AS blob UNION SELECT 91 AS int, X'5b' AS blob UNION SELECT 92 AS int, X'5c' AS blob UNION SELECT 93 AS int, X'5d' AS blob UNION SELECT 94 AS int, X'5e' AS blob UNION SELECT 95 AS int, X'5f' AS blob UNION SELECT 96 AS int, X'60' AS blob UNION SELECT 97 AS int, X'61' AS blob UNION SELECT 98 AS int, X'62' AS blob UNION SELECT 99 AS int, X'63' AS blob UNION SELECT 100 AS int, X'64' AS blob UNION SELECT 101 AS int, X'65' AS blob UNION SELECT 102 AS int, X'66' AS blob UNION SELECT 103 AS int, X'67' AS blob UNION SELECT 104 AS int, X'68' AS blob UNION SELECT 105 AS int, X'69' AS blob UNION SELECT 106 AS int, X'6a' AS blob UNION SELECT 107 AS int, X'6b' AS blob UNION SELECT 108 AS int, X'6c' AS blob UNION SELECT 109 AS int, X'6d' AS blob UNION SELECT 110 AS int, X'6e' AS blob UNION SELECT 111 AS int, X'6f' AS blob UNION SELECT 112 AS int, X'70' AS blob UNION SELECT 113 AS int, X'71' AS blob UNION SELECT 114 AS int, X'72' AS blob UNION SELECT 115 AS int, X'73' AS blob UNION SELECT 116 AS int, X'74' AS blob UNION SELECT 117 AS int, X'75' AS blob UNION SELECT 118 AS int, X'76' AS blob UNION SELECT 119 AS int, X'77' AS blob UNION SELECT 120 AS int, X'78' AS blob UNION SELECT 121 AS int, X'79' AS blob UNION SELECT 122 AS int, X'7a' AS blob UNION SELECT 123 AS int, X'7b' AS blob UNION SELECT 124 AS int, X'7c' AS blob UNION SELECT 125 AS int, X'7d' AS blob UNION SELECT 126 AS int, X'7e' AS blob UNION SELECT 127 AS int, X'7f' AS blob UNION SELECT 128 AS int, X'80' AS blob UNION SELECT 129 AS int, X'81' AS blob UNION SELECT 130 AS int, X'82' AS blob UNION SELECT 131 AS int, X'83' AS blob UNION SELECT 132 AS int, X'84' AS blob UNION SELECT 133 AS int, X'85' AS blob UNION SELECT 134 AS int, X'86' AS blob UNION SELECT 135 AS int, X'87' AS blob UNION SELECT 136 AS int, X'88' AS blob UNION SELECT 137 AS int, X'89' AS blob UNION SELECT 138 AS int, X'8a' AS blob UNION SELECT 139 AS int, X'8b' AS blob UNION SELECT 140 AS int, X'8c' AS blob UNION SELECT 141 AS int, X'8d' AS blob UNION SELECT 142 AS int, X'8e' AS blob UNION SELECT 143 AS int, X'8f' AS blob UNION SELECT 144 AS int, X'90' AS blob UNION SELECT 145 AS int, X'91' AS blob UNION SELECT 146 AS int, X'92' AS blob UNION SELECT 147 AS int, X'93' AS blob UNION SELECT 148 AS int, X'94' AS blob UNION SELECT 149 AS int, X'95' AS blob UNION SELECT 150 AS int, X'96' AS blob UNION SELECT 151 AS int, X'97' AS blob UNION SELECT 152 AS int, X'98' AS blob UNION SELECT 153 AS int, X'99' AS blob UNION SELECT 154 AS int, X'9a' AS blob UNION SELECT 155 AS int, X'9b' AS blob UNION SELECT 156 AS int, X'9c' AS blob UNION SELECT 157 AS int, X'9d' AS blob UNION SELECT 158 AS int, X'9e' AS blob UNION SELECT 159 AS int, X'9f' AS blob UNION SELECT 160 AS int, X'a0' AS blob UNION SELECT 161 AS int, X'a1' AS blob UNION SELECT 162 AS int, X'a2' AS blob UNION SELECT 163 AS int, X'a3' AS blob UNION SELECT 164 AS int, X'a4' AS blob UNION SELECT 165 AS int, X'a5' AS blob UNION SELECT 166 AS int, X'a6' AS blob UNION SELECT 167 AS int, X'a7' AS blob UNION SELECT 168 AS int, X'a8' AS blob UNION SELECT 169 AS int, X'a9' AS blob UNION SELECT 170 AS int, X'aa' AS blob UNION SELECT 171 AS int, X'ab' AS blob UNION SELECT 172 AS int, X'ac' AS blob UNION SELECT 173 AS int, X'ad' AS blob UNION SELECT 174 AS int, X'ae' AS blob UNION SELECT 175 AS int, X'af' AS blob UNION SELECT 176 AS int, X'b0' AS blob UNION SELECT 177 AS int, X'b1' AS blob UNION SELECT 178 AS int, X'b2' AS blob UNION SELECT 179 AS int, X'b3' AS blob UNION SELECT 180 AS int, X'b4' AS blob UNION SELECT 181 AS int, X'b5' AS blob UNION SELECT 182 AS int, X'b6' AS blob UNION SELECT 183 AS int, X'b7' AS blob UNION SELECT 184 AS int, X'b8' AS blob UNION SELECT 185 AS int, X'b9' AS blob UNION SELECT 186 AS int, X'ba' AS blob UNION SELECT 187 AS int, X'bb' AS blob UNION SELECT 188 AS int, X'bc' AS blob UNION SELECT 189 AS int, X'bd' AS blob UNION SELECT 190 AS int, X'be' AS blob UNION SELECT 191 AS int, X'bf' AS blob UNION SELECT 192 AS int, X'c0' AS blob UNION SELECT 193 AS int, X'c1' AS blob UNION SELECT 194 AS int, X'c2' AS blob UNION SELECT 195 AS int, X'c3' AS blob UNION SELECT 196 AS int, X'c4' AS blob UNION SELECT 197 AS int, X'c5' AS blob UNION SELECT 198 AS int, X'c6' AS blob UNION SELECT 199 AS int, X'c7' AS blob UNION SELECT 200 AS int, X'c8' AS blob UNION SELECT 201 AS int, X'c9' AS blob UNION SELECT 202 AS int, X'ca' AS blob UNION SELECT 203 AS int, X'cb' AS blob UNION SELECT 204 AS int, X'cc' AS blob UNION SELECT 205 AS int, X'cd' AS blob UNION SELECT 206 AS int, X'ce' AS blob UNION SELECT 207 AS int, X'cf' AS blob UNION SELECT 208 AS int, X'd0' AS blob UNION SELECT 209 AS int, X'd1' AS blob UNION SELECT 210 AS int, X'd2' AS blob UNION SELECT 211 AS int, X'd3' AS blob UNION SELECT 212 AS int, X'd4' AS blob UNION SELECT 213 AS int, X'd5' AS blob UNION SELECT 214 AS int, X'd6' AS blob UNION SELECT 215 AS int, X'd7' AS blob UNION SELECT 216 AS int, X'd8' AS blob UNION SELECT 217 AS int, X'd9' AS blob UNION SELECT 218 AS int, X'da' AS blob UNION SELECT 219 AS int, X'db' AS blob UNION SELECT 220 AS int, X'dc' AS blob UNION SELECT 221 AS int, X'dd' AS blob UNION SELECT 222 AS int, X'de' AS blob UNION SELECT 223 AS int, X'df' AS blob UNION SELECT 224 AS int, X'e0' AS blob UNION SELECT 225 AS int, X'e1' AS blob UNION SELECT 226 AS int, X'e2' AS blob UNION SELECT 227 AS int, X'e3' AS blob UNION SELECT 228 AS int, X'e4' AS blob UNION SELECT 229 AS int, X'e5' AS blob UNION SELECT 230 AS int, X'e6' AS blob UNION SELECT 231 AS int, X'e7' AS blob UNION SELECT 232 AS int, X'e8' AS blob UNION SELECT 233 AS int, X'e9' AS blob UNION SELECT 234 AS int, X'ea' AS blob UNION SELECT 235 AS int, X'eb' AS blob UNION SELECT 236 AS int, X'ec' AS blob UNION SELECT 237 AS int, X'ed' AS blob UNION SELECT 238 AS int, X'ee' AS blob UNION SELECT 239 AS int, X'ef' AS blob UNION SELECT 240 AS int, X'f0' AS blob UNION SELECT 241 AS int, X'f1' AS blob UNION SELECT 242 AS int, X'f2' AS blob UNION SELECT 243 AS int, X'f3' AS blob UNION SELECT 244 AS int, X'f4' AS blob UNION SELECT 245 AS int, X'f5' AS blob UNION SELECT 246 AS int, X'f6' AS blob UNION SELECT 247 AS int, X'f7' AS blob UNION SELECT 248 AS int, X'f8' AS blob UNION SELECT 249 AS int, X'f9' AS blob UNION SELECT 250 AS int, X'fa' AS blob UNION SELECT 251 AS int, X'fb' AS blob UNION SELECT 252 AS int, X'fc' AS blob UNION SELECT 253 AS int, X'fd' AS blob UNION SELECT 254 AS int, X'fe' AS blob UNION SELECT 255 AS int, X'ff' AS blob
), vm (
    cycle,
    instruction,
    arg,
    pc,
    stack,
    memory
) AS (
    SELECT
        0,
        0,
        0,
        0,
        ZEROBLOB(0),
        ZEROBLOB(256)
    UNION ALL
    SELECT
        cycle + 1,
        (SELECT int FROM hex WHERE blob=(SELECT array_slice(program, pc + 1, pc + 1) FROM program)),
        (SELECT int FROM hex WHERE blob=(SELECT array_slice(program, pc + 2, pc + 2) FROM program)),
        IIF(cycle & 1,
            CASE instruction
            -- JUMP[x]
            WHEN 15 THEN arg
            -- JUMPP
            WHEN 16 THEN (SELECT int FROM hex WHERE blob=UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 1), 2)))
            -- JUMPZ[x]
            WHEN 17 THEN IIF(UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 1), 2))=X'00', arg, pc + 2)
            -- JUMPPZ
            WHEN 18 THEN IIF(UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 2), 2))=X'00', (SELECT int FROM hex WHERE blob=UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 1), 2))), pc + 1)
            -- CALL[x]
            WHEN 19 THEN arg
            -- CALLP
            WHEN 20 THEN (SELECT int FROM hex WHERE blob=UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 1), 2)))
            ELSE pc + 1 + (instruction & 1)
            END,
            pc),
        IIF(cycle & 1,
            CASE instruction
            -- PUSH[n]
            WHEN 1 THEN UNHEX(HEX(stack) || HEX((SELECT blob FROM hex WHERE int=arg)))
            -- POP
            WHEN 2 THEN UNHEX(SUBSTR(HEX(stack), 1, 2 * (octet_length(stack) - 1)))
            -- DUP
            WHEN 4 THEN UNHEX(HEX(stack) || SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 1), 2))
            -- PICK[n]
            WHEN 5 THEN UNHEX(SUBSTR(HEX(stack), 1, 2 * (octet_length(stack) - arg - 1))
                              || SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - arg), 2 * arg)
                              || SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - arg - 1), 2))
            -- ADD
            WHEN 6 THEN UNHEX(SUBSTR(HEX(stack), 1, 2 * (octet_length(stack) - 2))
                              || HEX((SELECT blob FROM hex WHERE int=(
                                (SELECT int FROM hex WHERE blob=UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 2), 2))) +
                                (SELECT int FROM hex WHERE blob=UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 1), 2)))
                            ) % 256)))
            -- SUB
            WHEN 8 THEN UNHEX(SUBSTR(HEX(stack), 1, 2 * (octet_length(stack) - 2))
                              || HEX((SELECT blob FROM hex WHERE int=((
                                (SELECT int FROM hex WHERE blob=UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 2), 2))) -
                                (SELECT int FROM hex WHERE blob=UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 1), 2)))
                            ) + 256) % 256)))
            -- AND
            WHEN 10 THEN UNHEX(SUBSTR(HEX(stack), 1, 2 * (octet_length(stack) - 2))
                              || HEX((SELECT blob FROM hex WHERE int=(
                                (SELECT int FROM hex WHERE blob=UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 2), 2))) &
                                (SELECT int FROM hex WHERE blob=UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 1), 2)))
                            ))))
            -- OR
            WHEN 12 THEN UNHEX(SUBSTR(HEX(stack), 1, 2 * (octet_length(stack) - 2))
                              || HEX((SELECT blob FROM hex WHERE int=(
                                (SELECT int FROM hex WHERE blob=UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 2), 2))) |
                                (SELECT int FROM hex WHERE blob=UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 1), 2)))
                            ))))
            -- NOT
            WHEN 14 THEN UNHEX(SUBSTR(HEX(stack), 1, 2 * (octet_length(stack) - 1))
                              || HEX((SELECT blob FROM hex WHERE int=(
                                (SELECT 255&(~int) FROM hex WHERE blob=UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 1), 2)))
                            ))))
            -- JUMPP
            WHEN 16 THEN UNHEX(SUBSTR(HEX(stack), 1, 2 * (octet_length(stack) - 1)))
            -- JUMPZ[x]
            WHEN 17 THEN UNHEX(SUBSTR(HEX(stack), 1, 2 * (octet_length(stack) - 1)))
            -- JUMPPZ
            WHEN 18 THEN UNHEX(SUBSTR(HEX(stack), 1, 2 * (octet_length(stack) - 2)))
            -- CALL
            WHEN 19 THEN UNHEX(HEX(stack) || HEX((SELECT blob FROM hex WHERE int=(pc+2)%256)))
            -- CALLP
            WHEN 20 THEN UNHEX(SUBSTR(HEX(stack), 1, 2 * (octet_length(stack) - 1)) || HEX((SELECT blob FROM hex WHERE int=(pc+1)%256)))
            -- READ
            WHEN 22 THEN UNHEX(SUBSTR(HEX(stack), 1, 2 * (octet_length(stack) - 1)) || SUBSTR(HEX(memory), 1 + 2 * (SELECT int FROM hex WHERE blob=UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 1), 2))), 2))
            -- WRITE
            WHEN 24 THEN UNHEX(SUBSTR(HEX(stack), 1, 2 * (octet_length(stack) - 2)))
            ELSE stack
            END,
            stack),
        IIF(cycle & 1,
            CASE instruction
            -- WRITE
            WHEN 24 THEN UNHEX(SUBSTR(HEX(memory), 1, 2 * (SELECT int FROM hex WHERE blob=UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 1), 2))))
                               || SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 2), 2)
                               || SUBSTR(HEX(memory), 1 + 2 * (SELECT 1 + int FROM hex WHERE blob=UNHEX(SUBSTR(HEX(stack), 1 + 2 * (octet_length(stack) - 1), 2)))))
            ELSE memory
            END,
            memory)
    FROM vm
    WHERE instruction<>254
)
SELECT
    pc,
    HEX(stack) as stack,
    HEX(memory) as memory
FROM vm
WHERE cycle%2=0;