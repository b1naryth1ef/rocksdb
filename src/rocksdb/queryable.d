module rocksdb.queryable;

import std.conv;

import rocksdb.options;

mixin template Getable() {
  public import std.conv : to;
  public import rocksdb.options : ReadOptions, WriteOptions;

  /// Get a key (as a string)
  string getString(string key, ReadOptions opts = null) {
    return cast(string)this.get(cast(ubyte[])key, opts);
  }

  /// Get a key
  ubyte[] get(ubyte[] key, ReadOptions opts = null) {
    return this.getImpl(key, null, opts);
  }
}

mixin template Putable() {
  void putString(string key, string value, WriteOptions opts = null) {
    this.put(cast(ubyte[])key, cast(ubyte[])value, opts);
  }

  void put(ubyte[] key, ubyte[] value, WriteOptions opts = null) {
    return this.putImpl(key, value, null, opts);
  }
}

mixin template Removeable() {
  void removeString(string key, WriteOptions opts = null) {
    this.remove(cast(ubyte[])key, opts);
  }

  void remove(ubyte[] key, WriteOptions opts = null) {
    return this.removeImpl(key, null, opts);
  }
}
