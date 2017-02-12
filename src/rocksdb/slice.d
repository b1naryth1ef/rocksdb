module rocksdb.slice;

import std.conv : to;

struct Slice {
  size_t l;
  immutable char* p;

  static Slice fromString(string source) {
    return Slice(source.length, &source.to!(immutable(char)[])[0]);
  }

  ubyte[] value() {
    return cast(ubyte[])(this.p[0..this.l]);
  }

  string toString() {
    return (this.p[0..this.l]).to!string;
  }
}
