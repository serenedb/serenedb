#
# Copy this to some place in your homedirectory, for example
#   ~/.gdb/GdbSlicePrettyPrint.py
# and add the line
#   source ~/.gdb/GdbSlicePrettyPrint.py
# to your ~/.gdbinit file. Also make sure the serenevpack (built with
# SereneDB is in a directory contained in your path.
# Then you can use
#   p <Slice>
# in gdb sessions.
#

import subprocess

class SlicePrinter (object):
  "Print a VPack Slice prettily."

  def __init__ (self, val):
    self.val = val

  def readUInt(self, raw, size):
    x = 0
    shift = 0
    i = 0
    if size > 0:
      while i < size:
        x += int(raw[i]) << shift
        shift += 8
        i += 1
      return x
    else:
      size = -size
      while i < size:
        x += int(raw[size-i]) << shift
        shift += 8
        i += 1
      return x

  def readVarUInt(self, raw, forwards):
    r = 0
    s = 0
    while True:
      i = int(raw[0])
      r |= ((i & 0x7f) << s)
      s += 7
      if forwards:
        raw += 1
      else:
        raw -= 1
      if i & 0x80 == 0:
        return r

  def findByteLength(self, raw):
    # Raw must be a Value of type uint8_t* pointing to a vpack value
    # This finds the length of the vpack value
    typeByte = int(raw[0])
    if typeByte == 0x00:     # None
      return 0
    if typeByte == 0x01:     # empty array
      return 1
    if typeByte == 0x0a:     # empty object
      return 1
    if 0x02 <= typeByte <= 0x05:  # array equal length subobjs
      return self.readUInt(raw+1, typeByte - 0x01)
    if 0x06 <= typeByte <= 0x09:  # array
      return self.readUInt(raw+1, typeByte - 0x05)
    if 0x0b <= typeByte <= 0x0e:  # object
      return self.readUInt(raw+1, typeByte - 0x0a)
    if typeByte == 0x13 or typeByte == 0x14:  # compact array or object
      return self.readVarUInt(raw+1, True)
    if 0x18 <= typeByte <= 0x1a:  # bool
      return 1
    if typeByte == 0x1f:  # double
      return 9
    if 0x20 <= typeByte <= 0x27:  # int
      return 1 + typeByte - 0x1f
    if 0x28 <= typeByte <= 0x2f:  # uint
      return 1 + typeByte - 0x27
    if 0x30 <= typeByte <= 0x3f:  # small int
      return 1
    if 0x80 <= typeByte <= 0xfe:  # short string
      return 1 + typeByte - 0x80
    if typeByte == 0xff:  # long string
      return 9 + self.readUInt(raw+1, 8)
    return 1  # reserved

  def to_string(self):
    vpack = self.val["_start"]    # uint8_t*
    length = self.findByteLength(vpack)
    if length == 0:
      return "External" if int(vpack[0]) == 0x1d else "NoneSlice"
    b = bytearray(length)
    for i in range(0, length):
      b[i] = int(vpack[i])
    p = subprocess.Popen(["serenevpack", "--print-non-json"],
                                         stdin=subprocess.PIPE,
                                         stdout=subprocess.PIPE)
    s, e = p.communicate(b)
    return s.decode("utf-8")

def str_lookup_function(val):
  lookup_type = val.type
  if lookup_type.tag == "vpack::Slice" or \
     lookup_type == "vpack::Slice":
    return SlicePrinter(val)
  if str(lookup_type).find("vpack::Slice") >= 0 or \
     str(lookup_type).find("vpack::Slice") >= 0:
    return SlicePrinter(val)
  return None

gdb.pretty_printers.append(str_lookup_function)
